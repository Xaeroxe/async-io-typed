// Copyright 2022 Jacob Kiesel
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![doc = include_str!("../README.md")]

use std::{
    collections::VecDeque,
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bincode::Options;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{stream::Stream, Sink, SinkExt};
use serde::{de::DeserializeOwned, Serialize};

#[cfg(test)]
mod tests;

const U16_MARKER: u8 = 252;
const U32_MARKER: u8 = 253;
const U64_MARKER: u8 = 254;
const ZST_MARKER: u8 = 255;

/// A duplex async connection for sending and receiving messages of a particular type.
#[derive(Debug)]
pub struct DuplexStreamTyped<RW, T: Serialize + DeserializeOwned + Unpin> {
    rw: RW,
    read_state: AsyncReadState,
    read_buffer: Vec<u8>,
    write_state: AsyncWriteState,
    write_buffer: Vec<u8>,
    primed_values: VecDeque<T>,
    size_limit: u64,
}

impl<RW: AsyncRead + AsyncWrite, T: Serialize + DeserializeOwned + Unpin> DuplexStreamTyped<RW, T> {
    /// Creates a duplex typed reader and writer, initializing it with the given size limit specified in bytes.
    ///
    /// Be careful, large limits might create a vulnerability to a Denial of Service attack.
    pub fn new_with_limit(rw: RW, size_limit: u64) -> Self {
        Self {
            rw,
            read_state: AsyncReadState::Idle,
            read_buffer: Vec::new(),
            write_state: AsyncWriteState::Idle,
            write_buffer: Vec::new(),
            primed_values: VecDeque::new(),
            size_limit,
        }
    }

    /// Creates a duplex typed reader and writer, initializing it with a default size limit of 1 MB per message.
    pub fn new(rw: RW) -> Self {
        Self::new_with_limit(rw, 1024_u64.pow(2))
    }

    pub fn inner(&self) -> &RW {
        &self.rw
    }

    pub fn into_inner(self) -> RW {
        self.rw
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin> Stream
    for DuplexStreamTyped<RW, T>
{
    type Item = Result<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Self {
            ref mut rw,
            ref mut read_state,
            ref mut read_buffer,
            ref size_limit,
            ..
        } = *self.as_mut();
        AsyncReadTyped::poll_next_impl(read_state, rw, read_buffer, *size_limit, cx)
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin> Sink<T>
    for DuplexStreamTyped<RW, T>
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.primed_values.push_front(item);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Self {
            ref mut rw,
            ref size_limit,
            ref mut write_state,
            ref mut write_buffer,
            ref mut primed_values,
            ..
        } = *self.as_mut();
        match futures_core::ready!(AsyncWriteTyped::maybe_send(
            rw,
            *size_limit,
            write_state,
            write_buffer,
            primed_values,
            cx,
            false,
        ))? {
            Some(()) => {
                // Send successful, poll_flush now
                Pin::new(rw).poll_flush(cx).map(|r| r.map_err(Error::Io))
            }
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Self {
            ref mut rw,
            ref size_limit,
            ref mut write_state,
            ref mut write_buffer,
            ref mut primed_values,
            ..
        } = *self.as_mut();
        match futures_core::ready!(AsyncWriteTyped::maybe_send(
            rw,
            *size_limit,
            write_state,
            write_buffer,
            primed_values,
            cx,
            true,
        ))? {
            Some(()) => {
                // Send successful, poll_close now
                Pin::new(rw).poll_close(cx).map(|r| r.map_err(Error::Io))
            }
            None => Poll::Ready(Ok(())),
        }
    }
}

/// Errors that might arise while using a typed stream.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error from `std::io`
    #[error("io error {0}")]
    Io(#[from] io::Error),
    /// Error from the `bincode` crate
    #[error("bincode serialization/deserialization error {0}")]
    Bincode(#[from] bincode::Error),
    /// A message was sent that exceeded the configured length limit
    #[error("message sent exceeded configured length limit")]
    SentMessageTooLarge,
    /// A message was received that exceeded the configured length limit
    #[error("message received exceeded configured length limit, terminating connection")]
    ReceivedMessageTooLarge,
}

fn bincode_options(size_limit: u64) -> impl Options {
    // Two of these are defaults, so you might say this is over specified. I say it's future proof, as
    // bincode default changes won't introduce accidental breaking changes.
    bincode::DefaultOptions::new()
        .with_limit(size_limit)
        .with_little_endian()
        .with_varint_encoding()
        .reject_trailing_bytes()
}

/// Provides the ability to read `serde` compatible types from any type that implements `futures::io::AsyncRead`.
#[derive(Debug)]
pub struct AsyncReadTyped<R, T: Serialize + DeserializeOwned + Unpin> {
    raw: R,
    size_limit: u64,
    state: AsyncReadState,
    item_buffer: Vec<u8>,
    _phantom: PhantomData<T>,
}

#[derive(Debug)]
enum AsyncReadState {
    Idle,
    ReadingLen {
        len_read_mode: LenReadMode,
        len_in_progress: [u8; 8],
        len_in_progress_assigned: u8,
    },
    ReadingItem {
        len_read: usize,
    },
    Finished,
}

impl<R: AsyncRead + Unpin, T: Serialize + DeserializeOwned + Unpin> Stream
    for AsyncReadTyped<R, T>
{
    type Item = Result<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Self {
            ref mut raw,
            ref size_limit,
            ref mut item_buffer,
            ref mut state,
            _phantom,
        } = &mut *self;
        Self::poll_next_impl(state, raw, item_buffer, *size_limit, cx)
    }
}

impl<R: AsyncRead + Unpin, T: Serialize + DeserializeOwned + Unpin> AsyncReadTyped<R, T> {
    /// Creates a typed reader, initializing it with the given size limit specified in bytes.
    ///
    /// Be careful, large limits might create a vulnerability to a Denial of Service attack.
    pub fn new_with_limit(raw: R, size_limit: u64) -> Self {
        Self {
            raw,
            size_limit,
            state: AsyncReadState::Idle,
            item_buffer: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Creates a duplex typed reader and writer, initializing it with a default size limit of 1 MB.
    pub fn new(raw: R) -> Self {
        Self::new_with_limit(raw, 1024u64.pow(2))
    }

    pub fn inner(&self) -> &R {
        &self.raw
    }

    pub fn into_inner(self) -> R {
        self.raw
    }

    fn poll_next_impl(
        state: &mut AsyncReadState,
        mut raw: &mut R,
        item_buffer: &mut Vec<u8>,
        size_limit: u64,
        cx: &mut Context,
    ) -> Poll<Option<Result<T, Error>>> {
        loop {
            return match state {
                AsyncReadState::Idle => {
                    let mut buf = [0];
                    futures_core::ready!(Pin::new(&mut raw).poll_read(cx, &mut buf))?;
                    match buf[0] {
                        U16_MARKER => {
                            *state = AsyncReadState::ReadingLen {
                                len_read_mode: LenReadMode::U16,
                                len_in_progress: Default::default(),
                                len_in_progress_assigned: 0,
                            };
                        }
                        U32_MARKER => {
                            *state = AsyncReadState::ReadingLen {
                                len_read_mode: LenReadMode::U32,
                                len_in_progress: Default::default(),
                                len_in_progress_assigned: 0,
                            };
                        }
                        U64_MARKER => {
                            *state = AsyncReadState::ReadingLen {
                                len_read_mode: LenReadMode::U64,
                                len_in_progress: Default::default(),
                                len_in_progress_assigned: 0,
                            };
                        }
                        ZST_MARKER => {
                            return Poll::Ready(Some(
                                bincode_options(size_limit)
                                    .deserialize(&[])
                                    .map_err(Error::Bincode),
                            ));
                        }
                        0 => {
                            *state = AsyncReadState::Finished;
                            return Poll::Ready(None);
                        }
                        other => {
                            item_buffer.resize(other as usize, 0);
                            *state = AsyncReadState::ReadingItem { len_read: 0 };
                        }
                    }
                    continue;
                }
                AsyncReadState::ReadingLen {
                    ref mut len_read_mode,
                    ref mut len_in_progress,
                    ref mut len_in_progress_assigned,
                } => {
                    let mut buf = [0; 8];
                    let accumulated = *len_in_progress_assigned as usize;
                    let slice = match len_read_mode {
                        LenReadMode::U16 => &mut buf[accumulated..2],
                        LenReadMode::U32 => &mut buf[accumulated..4],
                        LenReadMode::U64 => &mut buf[accumulated..8],
                    };
                    let len = futures_core::ready!(Pin::new(&mut raw).poll_read(cx, slice))?;
                    len_in_progress[accumulated..(accumulated + len)]
                        .copy_from_slice(&slice[..len]);
                    *len_in_progress_assigned += len as u8;
                    if len == slice.len() {
                        let new_len = match len_read_mode {
                            LenReadMode::U16 => u16::from_le_bytes(
                                (&len_in_progress[0..2]).try_into().expect("infallible"),
                            ) as u64,
                            LenReadMode::U32 => u32::from_le_bytes(
                                (&len_in_progress[0..4]).try_into().expect("infallible"),
                            ) as u64,
                            LenReadMode::U64 => u64::from_le_bytes(*len_in_progress),
                        };
                        if new_len > size_limit {
                            *state = AsyncReadState::Finished;
                            return Poll::Ready(Some(Err(Error::ReceivedMessageTooLarge)));
                        }
                        item_buffer.resize(new_len as usize, 0);
                        *state = AsyncReadState::ReadingItem { len_read: 0 };
                    }
                    continue;
                }
                AsyncReadState::ReadingItem { ref mut len_read } => {
                    while *len_read < item_buffer.len() {
                        let len = futures_core::ready!(
                            Pin::new(&mut raw).poll_read(cx, &mut item_buffer[*len_read..])
                        )?;
                        *len_read += len;
                        if *len_read == item_buffer.len() {
                            break;
                        }
                    }
                    let ret = Poll::Ready(Some(
                        bincode_options(size_limit)
                            .deserialize(item_buffer)
                            .map_err(Error::Bincode),
                    ));
                    *state = AsyncReadState::Idle;
                    ret
                }
                AsyncReadState::Finished => Poll::Ready(None),
            };
        }
    }
}

#[derive(Debug)]
enum LenReadMode {
    U16,
    U32,
    U64,
}

/// Provides the ability to write `serde` compatible types to any type that implements `futures::io::AsyncWrite`.
#[derive(Debug)]
pub struct AsyncWriteTyped<W: AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin> {
    raw: Option<W>,
    size_limit: u64,
    write_buffer: Vec<u8>,
    state: AsyncWriteState,
    primed_values: VecDeque<T>,
}

#[derive(Debug)]
enum AsyncWriteState {
    Idle,
    WritingLen {
        current_len: [u8; 9],
        len_to_be_sent: usize,
        len_sent: usize,
    },
    WritingValue {
        bytes_sent: usize,
    },
    Closing,
    Closed,
}

impl<W: AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin> Sink<T>
    for AsyncWriteTyped<W, T>
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.primed_values.push_front(item);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Self {
            ref mut raw,
            ref size_limit,
            ref mut write_buffer,
            ref mut state,
            ref mut primed_values,
        } = *self.as_mut();
        match futures_core::ready!(Self::maybe_send(
            raw.as_mut().expect("infallible"),
            *size_limit,
            state,
            write_buffer,
            primed_values,
            cx,
            false,
        ))? {
            Some(()) => {
                // Send successful, poll_flush now
                Pin::new(raw.as_mut().expect("infallible"))
                    .poll_flush(cx)
                    .map(|r| r.map_err(Error::Io))
            }
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Self {
            ref mut raw,
            ref size_limit,
            ref mut state,
            ref mut write_buffer,
            ref mut primed_values,
        } = *self.as_mut();
        match futures_core::ready!(Self::maybe_send(
            raw.as_mut().expect("infallible"),
            *size_limit,
            state,
            write_buffer,
            primed_values,
            cx,
            true,
        ))? {
            Some(()) => {
                // Send successful, poll_close now
                Pin::new(raw.as_mut().expect("infallible"))
                    .poll_close(cx)
                    .map(|r| r.map_err(Error::Io))
            }
            None => Poll::Ready(Ok(())),
        }
    }
}

impl<W: AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin> AsyncWriteTyped<W, T> {
    fn maybe_send(
        raw: &mut W,
        size_limit: u64,
        state: &mut AsyncWriteState,
        write_buffer: &mut Vec<u8>,
        primed_values: &mut VecDeque<T>,
        cx: &mut Context<'_>,
        closing: bool,
    ) -> Poll<Result<Option<()>, Error>> {
        loop {
            return match state {
                AsyncWriteState::Idle => {
                    if let Some(item) = primed_values.pop_back() {
                        write_buffer.clear();
                        bincode_options(size_limit)
                            .serialize_into(&mut *write_buffer, &item)
                            .map_err(Error::Bincode)?;
                        if write_buffer.len() as u64 > size_limit {
                            return Poll::Ready(Err(Error::SentMessageTooLarge));
                        }
                        let (new_current_len, to_be_sent) = if write_buffer.is_empty() {
                            ([ZST_MARKER, 0, 0, 0, 0, 0, 0, 0, 0], 1)
                        } else if write_buffer.len() < U16_MARKER as usize {
                            let bytes = (write_buffer.len() as u8).to_le_bytes();
                            ([bytes[0], 0, 0, 0, 0, 0, 0, 0, 0], 1)
                        } else if (write_buffer.len() as u64) < 2_u64.pow(16) {
                            let bytes = (write_buffer.len() as u16).to_le_bytes();
                            ([U16_MARKER, bytes[0], bytes[1], 0, 0, 0, 0, 0, 0], 3)
                        } else if (write_buffer.len() as u64) < 2_u64.pow(32) {
                            let bytes = (write_buffer.len() as u32).to_le_bytes();
                            (
                                [
                                    U32_MARKER, bytes[0], bytes[1], bytes[2], bytes[3], 0, 0, 0, 0,
                                ],
                                5,
                            )
                        } else {
                            let bytes = (write_buffer.len() as u64).to_le_bytes();
                            (
                                [
                                    U64_MARKER, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
                                    bytes[5], bytes[6], bytes[7],
                                ],
                                9,
                            )
                        };
                        *state = AsyncWriteState::WritingLen {
                            current_len: new_current_len,
                            len_to_be_sent: to_be_sent,
                            len_sent: 0,
                        };
                        let len = futures_core::ready!(
                            Pin::new(&mut *raw).poll_write(cx, &new_current_len[0..to_be_sent])
                        )?;
                        *state = if len == to_be_sent {
                            AsyncWriteState::WritingValue { bytes_sent: 0 }
                        } else {
                            AsyncWriteState::WritingLen {
                                current_len: new_current_len,
                                len_to_be_sent: to_be_sent,
                                len_sent: len,
                            }
                        };
                        continue;
                    } else if closing {
                        *state = AsyncWriteState::Closing;
                        continue;
                    } else {
                        Poll::Ready(Ok(Some(())))
                    }
                }
                AsyncWriteState::WritingLen {
                    ref current_len,
                    ref len_to_be_sent,
                    ref mut len_sent,
                } => {
                    let len = futures_core::ready!(Pin::new(&mut *raw)
                        .poll_write(cx, &current_len[(*len_sent)..(*len_to_be_sent)]))?;
                    *len_sent += len;
                    if *len_sent == *len_to_be_sent {
                        *state = AsyncWriteState::WritingValue { bytes_sent: 0 };
                    }
                    continue;
                }
                AsyncWriteState::WritingValue { bytes_sent } => {
                    let len = futures_core::ready!(
                        Pin::new(&mut *raw).poll_write(cx, &write_buffer[*bytes_sent..])
                    )?;
                    *bytes_sent += len;
                    if *bytes_sent == write_buffer.len() {
                        *state = AsyncWriteState::Idle;
                        if primed_values.is_empty() {
                            return Poll::Ready(Ok(Some(())));
                        }
                    }
                    continue;
                }
                AsyncWriteState::Closing => {
                    let len = futures_core::ready!(Pin::new(&mut *raw).poll_write(cx, &[0]))?;
                    if len == 1 {
                        *state = AsyncWriteState::Closed;
                        Poll::Ready(Ok(Some(())))
                    } else {
                        continue;
                    }
                }
                AsyncWriteState::Closed => Poll::Ready(Ok(None)),
            };
        }
    }
}

impl<W: AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin> AsyncWriteTyped<W, T> {
    /// Creates a typed writer, initializing it with the given size limit specified in bytes.
    ///
    /// Be careful, large limits might create a vulnerability to a Denial of Service attack.
    pub fn new_with_limit(raw: W, size_limit: u64) -> Self {
        Self {
            raw: Some(raw),
            size_limit,
            write_buffer: Vec::new(),
            state: AsyncWriteState::Idle,
            primed_values: VecDeque::new(),
        }
    }

    /// Creates a typed writer, initializing it with a default size limit of 1 MB per message.
    pub fn new(raw: W) -> Self {
        Self::new_with_limit(raw, 1024u64.pow(2))
    }

    pub fn inner(&self) -> &W {
        self.raw.as_ref().expect("infallible")
    }

    pub fn into_inner(mut self) -> W {
        self.raw.take().expect("infallible")
    }
}

impl<W: AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin> Drop
    for AsyncWriteTyped<W, T>
{
    fn drop(&mut self) {
        // This will panic if raw was already taken.
        if self.raw.is_some() {
            let _ = futures_executor::block_on(SinkExt::close(self));
        }
    }
}
