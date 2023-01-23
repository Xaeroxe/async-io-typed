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
    hash::Hasher,
    io,
    marker::PhantomData,
    mem::size_of,
    pin::Pin,
    task::{Context, Poll},
};

use bincode::Options;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{stream::Stream, Sink, SinkExt};
use serde::{de::DeserializeOwned, Serialize};
use siphasher::sip::SipHasher;

#[cfg(test)]
mod tests;

const U16_MARKER: u8 = 252;
const U32_MARKER: u8 = 253;
const U64_MARKER: u8 = 254;
const ZST_MARKER: u8 = 255;

// These values chosen such that zeroed data and data that is all ones doesn't resemble either of them.
const CHECKSUM_ENABLED: u8 = 2;
const CHECKSUM_DISABLED: u8 = 3;

// Current protocol version. The original implementation had no version number, version 2 is the first version with a version number.
const PROTOCOL_VERSION: u64 = 2;

/// A duplex async connection for sending and receiving messages of a particular type.
#[derive(Debug)]
pub struct DuplexStreamTyped<
    RW: AsyncRead + AsyncWrite + Unpin,
    T: Serialize + DeserializeOwned + Unpin,
> {
    rw: Option<RW>,
    read_state: AsyncReadState,
    read_buffer: Vec<u8>,
    write_state: AsyncWriteState,
    write_buffer: Vec<u8>,
    primed_values: VecDeque<T>,
    checksum_read_state: ChecksumReadState,
    message_features: MessageFeatures,
}

impl<RW: AsyncRead + AsyncWrite + Unpin, T: Serialize + DeserializeOwned + Unpin>
    DuplexStreamTyped<RW, T>
{
    /// Creates a duplex typed reader and writer, initializing it with the given size limit specified in bytes.
    /// Checksums are used to validate that messages arrived without corruption. **The checksum will only be used
    /// if both the reader and the writer enable it. If either one disables it, then no checking is performed.**
    ///
    /// Be careful, large size limits might create a vulnerability to a Denial of Service attack.
    pub fn new_with_limit(rw: RW, size_limit: u64, checksum_enabled: bool) -> Self {
        Self {
            rw: Some(rw),
            read_state: AsyncReadState::ReadingVersion {
                version_in_progress: [0; 8],
                version_in_progress_assigned: 0,
            },
            read_buffer: Vec::new(),
            write_state: AsyncWriteState::WritingVersion {
                version: PROTOCOL_VERSION.to_le_bytes(),
                len_sent: 0,
            },
            write_buffer: Vec::new(),
            primed_values: VecDeque::new(),
            checksum_read_state: if checksum_enabled {
                ChecksumReadState::Yes
            } else {
                ChecksumReadState::No
            },
            message_features: MessageFeatures {
                size_limit,
                checksum_enabled,
            },
        }
    }

    /// Creates a duplex typed reader and writer, initializing it with a default size limit of 1 MB per message.
    /// Checksums are used to validate that messages arrived without corruption. **The checksum will only be used
    /// if both the reader and the writer enable it. If either one disables it, then no checking is performed.**
    pub fn new(rw: RW, checksum_enabled: bool) -> Self {
        Self::new_with_limit(rw, 1024_u64.pow(2), checksum_enabled)
    }

    /// Returns a reference to the raw I/O primitive that this type is using.
    pub fn inner(&self) -> &RW {
        self.rw.as_ref().expect("infallible")
    }

    /// Consumes this `DuplexStreamTyped` and returns the raw I/O primitive that was being used.
    pub fn into_inner(mut self) -> RW {
        self.rw.take().expect("infallible")
    }

    /// `DuplexStreamTyped` keeps memory buffers for sending and receiving values which are the same size as the largest
    /// message that's been sent or received. If the message size varies a lot, you might find yourself wasting
    /// memory space. This function will reduce the memory usage as much as is possible without impeding
    /// functioning. Overuse of this function may cause excessive memory allocations when the buffer
    /// needs to grow.
    pub fn optimize_memory_usage(&mut self) {
        match self.read_state {
            AsyncReadState::ReadingItem { .. } => self.read_buffer.shrink_to_fit(),
            _ => {
                self.read_buffer = Vec::new();
            }
        }
        match self.write_state {
            AsyncWriteState::WritingLen { .. } | AsyncWriteState::WritingValue { .. } => {
                self.write_buffer.shrink_to_fit()
            }
            _ => {
                self.write_buffer = Vec::new();
            }
        }
    }

    /// Reports the size of the memory buffers used for sending and receiving values. You can shrink these buffers as much as
    /// possible with [`Self::optimize_memory_usage`].
    pub fn current_memory_usage(&self) -> usize {
        self.write_buffer.capacity() + self.read_buffer.capacity()
    }

    /// Returns true if checksums are enabled for this channel. This does not guarantee that the reader is
    /// actually using those checksum values, it only reflects whether checksums are being sent.
    pub fn checksum_send_enabled(&self) -> bool {
        self.message_features.checksum_enabled
    }

    /// Returns true if checksums are enabled for this channel. This may become false after receiving the first value.
    /// If that happens, the writer may have disabled checksums, so there is no checksum for the reader to check.
    pub fn checksum_receive_enabled(&self) -> bool {
        self.checksum_read_state == ChecksumReadState::Yes
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
            ref message_features,
            ref mut checksum_read_state,
            ..
        } = *self.as_mut();
        AsyncReadTyped::poll_next_impl(
            read_state,
            rw.as_mut().expect("infallible"),
            read_buffer,
            message_features.size_limit,
            checksum_read_state,
            cx,
        )
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
            ref mut write_state,
            ref mut write_buffer,
            ref mut primed_values,
            ref message_features,
            ..
        } = *self.as_mut();
        let rw = rw.as_mut().expect("infallible");
        match futures_core::ready!(AsyncWriteTyped::maybe_send(
            rw,
            write_state,
            write_buffer,
            primed_values,
            *message_features,
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
            ref mut write_state,
            ref mut write_buffer,
            ref mut primed_values,
            ref message_features,
            ..
        } = *self.as_mut();
        let rw = rw.as_mut().expect("infallible");
        match futures_core::ready!(AsyncWriteTyped::maybe_send(
            rw,
            write_state,
            write_buffer,
            primed_values,
            *message_features,
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

impl<RW: AsyncRead + AsyncWrite + Unpin, T: Serialize + Unpin + DeserializeOwned> Drop
    for DuplexStreamTyped<RW, T>
{
    fn drop(&mut self) {
        if self.rw.is_some() {
            let _ = futures_executor::block_on(SinkExt::close(self));
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
    /// A checksum mismatch occurred, indicating that the data was corrupted. This error will never occur if
    /// either side of the channel has checksums disabled.
    #[error("checksum mismatch, data corrupted or there was a protocol mismatch")]
    ChecksumMismatch {
        sent_checksum: u64,
        computed_checksum: u64,
    },
    /// The peer is using an incompatible protocol version.
    #[error("the peer is using an incompatible protocol version. Our version {our_version}, Their version {their_version}")]
    ProtocolVersionMismatch {
        our_version: u64,
        their_version: u64,
    },
    /// When exchanging information about whether to use a checksum or not, the peer sent us something unexpected.
    #[error("checksum handshake failed, expected {CHECKSUM_ENABLED} or {CHECKSUM_DISABLED}, got {checksum_value}")]
    ChecksumHandshakeFailed { checksum_value: u8 },
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
    checksum_read_state: ChecksumReadState,
    _phantom: PhantomData<T>,
}

#[derive(Debug)]
enum AsyncReadState {
    ReadingVersion {
        version_in_progress: [u8; 8],
        version_in_progress_assigned: usize,
    },
    ReadingChecksumEnabled,
    Idle,
    ReadingLen {
        len_read_mode: LenReadMode,
        len_in_progress: [u8; 8],
        len_in_progress_assigned: usize,
    },
    ReadingItem {
        len_read: usize,
    },
    ReadingChecksum {
        checksum_in_progress: [u8; 8],
        checksum_assigned: usize,
    },
    Finished,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ChecksumReadState {
    /// The writer will not send checksums, the reader can't use them.
    No,
    /// The writer will send checksums, and we want to validate against them.
    /// Checksums are enabled for both sides.
    Yes,
    /// The writer will send checksums, and we want to ignore them. Checksums
    /// are enabled for the writer, and disabled for the reader.
    Ignore,
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
            ref mut checksum_read_state,
            _phantom,
        } = &mut *self;
        Self::poll_next_impl(
            state,
            raw,
            item_buffer,
            *size_limit,
            checksum_read_state,
            cx,
        )
    }
}

impl<R: AsyncRead + Unpin, T: Serialize + DeserializeOwned + Unpin> AsyncReadTyped<R, T> {
    /// Creates a typed reader, initializing it with the given size limit specified in bytes.
    ///
    /// Be careful, large limits might create a vulnerability to a Denial of Service attack.
    pub fn new_with_limit(raw: R, size_limit: u64, checksum_enabled: bool) -> Self {
        Self {
            raw,
            size_limit,
            state: AsyncReadState::ReadingVersion {
                version_in_progress: [0; 8],
                version_in_progress_assigned: 0,
            },
            item_buffer: Vec::new(),
            checksum_read_state: if checksum_enabled {
                ChecksumReadState::Yes
            } else {
                ChecksumReadState::No
            },
            _phantom: PhantomData,
        }
    }

    /// Creates a duplex typed reader and writer, initializing it with a default size limit of 1 MB.
    pub fn new(raw: R, checksum_enabled: bool) -> Self {
        Self::new_with_limit(raw, 1024u64.pow(2), checksum_enabled)
    }

    /// Returns a reference to the raw I/O primitive that this type is using.
    pub fn inner(&self) -> &R {
        &self.raw
    }

    /// Consumes this `AsyncReadTyped` and returns the raw I/O primitive that was being used.
    pub fn into_inner(self) -> R {
        self.raw
    }

    /// `AsyncReadTyped` keeps a memory buffer for receiving values which is the same size as the largest
    /// message that's been received. If the message size varies a lot, you might find yourself wasting
    /// memory space. This function will reduce the memory usage as much as is possible without impeding
    /// functioning. Overuse of this function may cause excessive memory allocations when the buffer
    /// needs to grow.
    pub fn optimize_memory_usage(&mut self) {
        match self.state {
            AsyncReadState::ReadingItem { .. } => self.item_buffer.shrink_to_fit(),
            _ => {
                self.item_buffer = Vec::new();
            }
        }
    }

    /// Reports the size of the memory buffer used for receiving values. You can shrink this buffer as much as
    /// possible with [`Self::optimize_memory_usage`].
    pub fn current_memory_usage(&self) -> usize {
        self.item_buffer.capacity()
    }

    /// Returns true if checksums are enabled for this channel. This may become false after receiving the first value.
    /// If that happens, the writer may have disabled checksums, so there is no checksum for the reader to check.
    pub fn checksum_enabled(&self) -> bool {
        self.checksum_read_state == ChecksumReadState::Yes
    }

    fn poll_next_impl(
        state: &mut AsyncReadState,
        mut raw: &mut R,
        item_buffer: &mut Vec<u8>,
        size_limit: u64,
        checksum_read_state: &mut ChecksumReadState,
        cx: &mut Context,
    ) -> Poll<Option<Result<T, Error>>> {
        loop {
            return match state {
                AsyncReadState::ReadingVersion {
                    version_in_progress,
                    version_in_progress_assigned,
                } => {
                    while *version_in_progress_assigned < size_of::<u64>() {
                        let len = futures_core::ready!(Pin::new(&mut raw).poll_read(
                            cx,
                            &mut version_in_progress[(*version_in_progress_assigned)..]
                        ))?;
                        *version_in_progress_assigned += len;
                    }
                    let version = u64::from_le_bytes(*version_in_progress);
                    if version != PROTOCOL_VERSION {
                        *state = AsyncReadState::Finished;
                        return Poll::Ready(Some(Err(Error::ProtocolVersionMismatch {
                            our_version: PROTOCOL_VERSION,
                            their_version: version,
                        })));
                    }
                    *state = AsyncReadState::ReadingChecksumEnabled;
                    continue;
                }
                AsyncReadState::ReadingChecksumEnabled => {
                    let mut checksum_enabled = [0];
                    if futures_core::ready!(Pin::new(&mut raw).poll_read(cx, &mut checksum_enabled))?
                        == 1
                    {
                        match checksum_enabled[0] {
                            CHECKSUM_ENABLED => {
                                match *checksum_read_state {
                                    ChecksumReadState::Yes => {
                                        // Do nothing, we are in agreement that a checksum should be used.
                                    }
                                    ChecksumReadState::No => {
                                        // The peer is going to send checksums and we can't tell them to stop.
                                        // Ignore the checksums.
                                        *checksum_read_state = ChecksumReadState::Ignore;
                                    }
                                    ChecksumReadState::Ignore => {
                                        // This should never happen, but if it does we can continue ignoring them I suppose.
                                    }
                                }
                            }
                            CHECKSUM_DISABLED => {
                                // We can't use checksums if the peer won't send them, so disable them.
                                *checksum_read_state = ChecksumReadState::No;
                            }
                            _ => {
                                *state = AsyncReadState::Finished;
                                return Poll::Ready(Some(Err(Error::ChecksumHandshakeFailed {
                                    checksum_value: checksum_enabled[0],
                                })));
                            }
                        }
                        *state = AsyncReadState::Idle;
                    }
                    continue;
                }
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
                            item_buffer.truncate(0);
                            *state = AsyncReadState::ReadingItem { len_read: 0 };
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
                    let accumulated = *len_in_progress_assigned;
                    let slice = match len_read_mode {
                        LenReadMode::U16 => &mut buf[accumulated..2],
                        LenReadMode::U32 => &mut buf[accumulated..4],
                        LenReadMode::U64 => &mut buf[accumulated..8],
                    };
                    let len = futures_core::ready!(Pin::new(&mut raw).poll_read(cx, slice))?;
                    len_in_progress[accumulated..(accumulated + len)]
                        .copy_from_slice(&slice[..len]);
                    *len_in_progress_assigned += len;
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
                    }
                    if [ChecksumReadState::Yes, ChecksumReadState::Ignore]
                        .contains(checksum_read_state)
                    {
                        *state = AsyncReadState::ReadingChecksum {
                            checksum_in_progress: [0; 8],
                            checksum_assigned: 0,
                        };
                        continue;
                    } else {
                        let ret = Poll::Ready(Some(
                            bincode_options(size_limit)
                                .deserialize(item_buffer)
                                .map_err(Error::Bincode),
                        ));
                        *state = AsyncReadState::Idle;
                        ret
                    }
                }
                AsyncReadState::ReadingChecksum {
                    checksum_in_progress,
                    checksum_assigned,
                } => {
                    while *checksum_assigned < size_of::<u64>() {
                        let len = futures_core::ready!(Pin::new(&mut raw)
                            .poll_read(cx, &mut checksum_in_progress[(*checksum_assigned)..]))?;
                        *checksum_assigned += len;
                    }
                    let ret = (*checksum_read_state == ChecksumReadState::Yes)
                        .then(|| {
                            let sent_checksum = u64::from_le_bytes(*checksum_in_progress);
                            let mut hasher = SipHasher::new();
                            hasher.write(item_buffer);
                            let computed_checksum = hasher.finish();
                            (sent_checksum != computed_checksum).then_some(Err(
                                Error::ChecksumMismatch {
                                    sent_checksum,
                                    computed_checksum,
                                },
                            ))
                        })
                        .flatten()
                        .unwrap_or_else(|| {
                            bincode_options(size_limit)
                                .deserialize(item_buffer)
                                .map_err(Error::Bincode)
                        });
                    *state = AsyncReadState::Idle;
                    Poll::Ready(Some(ret))
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
    write_buffer: Vec<u8>,
    state: AsyncWriteState,
    primed_values: VecDeque<T>,
    message_features: MessageFeatures,
}

#[derive(Debug)]
enum AsyncWriteState {
    WritingVersion {
        version: [u8; 8],
        len_sent: usize,
    },
    WritingChecksumEnabled,
    Idle,
    WritingLen {
        current_len: [u8; 9],
        len_to_be_sent: usize,
        len_sent: usize,
    },
    WritingValue {
        bytes_sent: usize,
    },
    WritingChecksum {
        checksum: [u8; 8],
        len_sent: usize,
    },
    Closing,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageFeatures {
    size_limit: u64,
    checksum_enabled: bool,
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
            ref mut write_buffer,
            ref mut state,
            ref mut primed_values,
            ref message_features,
        } = *self.as_mut();
        match futures_core::ready!(Self::maybe_send(
            raw.as_mut().expect("infallible"),
            state,
            write_buffer,
            primed_values,
            *message_features,
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
            ref mut state,
            ref mut write_buffer,
            ref mut primed_values,
            ref message_features,
        } = *self.as_mut();
        match futures_core::ready!(Self::maybe_send(
            raw.as_mut().expect("infallible"),
            state,
            write_buffer,
            primed_values,
            *message_features,
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
        state: &mut AsyncWriteState,
        write_buffer: &mut Vec<u8>,
        primed_values: &mut VecDeque<T>,
        message_features: MessageFeatures,
        cx: &mut Context<'_>,
        closing: bool,
    ) -> Poll<Result<Option<()>, Error>> {
        let MessageFeatures {
            checksum_enabled,
            size_limit,
        } = message_features;
        loop {
            return match state {
                AsyncWriteState::WritingVersion { version, len_sent } => {
                    while *len_sent < size_of::<u64>() {
                        let len = futures_core::ready!(
                            Pin::new(&mut *raw).poll_write(cx, &version[(*len_sent)..])
                        )?;
                        *len_sent += len;
                    }
                    *state = AsyncWriteState::WritingChecksumEnabled;
                    continue;
                }
                AsyncWriteState::WritingChecksumEnabled => {
                    let to_send = if checksum_enabled {
                        CHECKSUM_ENABLED
                    } else {
                        CHECKSUM_DISABLED
                    };
                    if futures_core::ready!(Pin::new(&mut *raw).poll_write(cx, &[to_send]))? == 1 {
                        *state = AsyncWriteState::Idle;
                    }
                    continue;
                }
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
                    while *len_sent < *len_to_be_sent {
                        let len = futures_core::ready!(Pin::new(&mut *raw)
                            .poll_write(cx, &current_len[(*len_sent)..(*len_to_be_sent)]))?;
                        *len_sent += len;
                    }
                    *state = AsyncWriteState::WritingValue { bytes_sent: 0 };
                    continue;
                }
                AsyncWriteState::WritingValue { bytes_sent } => {
                    while *bytes_sent < write_buffer.len() {
                        let len = futures_core::ready!(
                            Pin::new(&mut *raw).poll_write(cx, &write_buffer[*bytes_sent..])
                        )?;
                        *bytes_sent += len;
                    }
                    if checksum_enabled {
                        let mut hasher = SipHasher::new();
                        hasher.write(write_buffer);
                        let checksum = hasher.finish();
                        *state = AsyncWriteState::WritingChecksum {
                            checksum: checksum.to_le_bytes(),
                            len_sent: 0,
                        };
                    } else {
                        *state = AsyncWriteState::Idle;
                        if primed_values.is_empty() {
                            return Poll::Ready(Ok(Some(())));
                        }
                    }
                    continue;
                }
                AsyncWriteState::WritingChecksum { checksum, len_sent } => {
                    while *len_sent < size_of::<u64>() {
                        let len = futures_core::ready!(
                            Pin::new(&mut *raw).poll_write(cx, &checksum[*len_sent..])
                        )?;
                        *len_sent += len;
                    }
                    *state = AsyncWriteState::Idle;
                    if primed_values.is_empty() {
                        return Poll::Ready(Ok(Some(())));
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
    /// Checksums are used to validate that messages arrived without corruption. **The checksum will only be used
    /// if both the reader and the writer enable it. If either one disables it, then no checking is performed.**
    ///
    /// Be careful, large size limits might create a vulnerability to a Denial of Service attack.
    pub fn new_with_limit(raw: W, size_limit: u64, checksum_enabled: bool) -> Self {
        Self {
            raw: Some(raw),
            write_buffer: Vec::new(),
            state: AsyncWriteState::WritingVersion {
                version: PROTOCOL_VERSION.to_le_bytes(),
                len_sent: 0,
            },
            message_features: MessageFeatures {
                size_limit,
                checksum_enabled,
            },
            primed_values: VecDeque::new(),
        }
    }

    /// Creates a typed writer, initializing it with a default size limit of 1 MB per message.
    /// Checksums are used to validate that messages arrived without corruption. **The checksum will only be used
    /// if both the reader and the writer enable it. If either one disables it, then no checking is performed.**
    pub fn new(raw: W, checksum_enabled: bool) -> Self {
        Self::new_with_limit(raw, 1024u64.pow(2), checksum_enabled)
    }

    /// Returns a reference to the raw I/O primitive that this type is using.
    pub fn inner(&self) -> &W {
        self.raw.as_ref().expect("infallible")
    }

    /// Consumes this `AsyncWriteTyped` and returns the raw I/O primitive that was being used.
    pub fn into_inner(mut self) -> W {
        self.raw.take().expect("infallible")
    }

    /// `AsyncWriteTyped` keeps a memory buffer for sending values which is the same size as the largest
    /// message that's been sent. If the message size varies a lot, you might find yourself wasting
    /// memory space. This function will reduce the memory usage as much as is possible without impeding
    /// functioning. Overuse of this function may cause excessive memory allocations when the buffer
    /// needs to grow.
    pub fn optimize_memory_usage(&mut self) {
        match self.state {
            AsyncWriteState::WritingLen { .. } | AsyncWriteState::WritingValue { .. } => {
                self.write_buffer.shrink_to_fit()
            }
            _ => {
                self.write_buffer = Vec::new();
            }
        }
    }

    /// Reports the size of the memory buffer used for sending values. You can shrink this buffer as much as
    /// possible with [`Self::optimize_memory_usage`].
    pub fn current_memory_usage(&self) -> usize {
        self.write_buffer.capacity()
    }

    /// Returns true if checksums are enabled for this channel. This does not guarantee that the reader is
    /// actually using those checksum values, it only reflects whether checksums are being sent.
    pub fn checksum_enabled(&self) -> bool {
        self.message_features.checksum_enabled
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
