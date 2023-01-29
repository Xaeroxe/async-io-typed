use crate::{
    ChecksumEnabled, Error, CHECKSUM_DISABLED, CHECKSUM_ENABLED, PROTOCOL_VERSION, U16_MARKER,
    U32_MARKER, U64_MARKER, ZST_MARKER,
};
use bincode::Options;
use futures_io::AsyncWrite;
use futures_util::{Sink, SinkExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use siphasher::sip::SipHasher;
use std::collections::VecDeque;
use std::hash::Hasher;
use std::mem::size_of;
use std::pin::Pin;
use std::task::{Context, Poll};

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
pub(crate) enum AsyncWriteState {
    WritingVersion { version: [u8; 8], len_sent: usize },
    WritingChecksumEnabled,
    Idle,
    WritingValue { bytes_sent: usize },
    Closing,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MessageFeatures {
    pub size_limit: u64,
    pub checksum_enabled: bool,
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
    pub(crate) fn maybe_send(
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
                        let length = crate::bincode_options(size_limit)
                            .serialized_size(&item)
                            .map_err(Error::Bincode)?;
                        if length > size_limit {
                            return Poll::Ready(Err(Error::SentMessageTooLarge));
                        }
                        if length == 0 {
                            write_buffer.push(ZST_MARKER);
                        } else if length < U16_MARKER as u64 {
                            write_buffer.extend((length as u8).to_le_bytes());
                        } else if length < 2_u64.pow(16) {
                            write_buffer.push(U16_MARKER);
                            write_buffer.extend((length as u16).to_le_bytes());
                        } else if length < 2_u64.pow(32) {
                            write_buffer.push(U32_MARKER);
                            write_buffer.extend((length as u32).to_le_bytes());
                        } else {
                            write_buffer.push(U64_MARKER);
                            write_buffer.extend(length.to_le_bytes());
                        }
                        // Save the length... of the length value.
                        let length_length = write_buffer.len();
                        crate::bincode_options(size_limit)
                            .serialize_into(&mut *write_buffer, &item)
                            .map_err(Error::Bincode)?;
                        if checksum_enabled {
                            let mut hasher = SipHasher::new();
                            hasher.write(&write_buffer[length_length..]);
                            let checksum = hasher.finish();
                            write_buffer.extend(checksum.to_le_bytes());
                        }
                        *state = AsyncWriteState::WritingValue { bytes_sent: 0 };
                        continue;
                    } else if closing {
                        *state = AsyncWriteState::Closing;
                        continue;
                    } else {
                        Poll::Ready(Ok(Some(())))
                    }
                }
                AsyncWriteState::WritingValue { bytes_sent } => {
                    while *bytes_sent < write_buffer.len() {
                        let len = futures_core::ready!(
                            Pin::new(&mut *raw).poll_write(cx, &write_buffer[*bytes_sent..])
                        )?;
                        *bytes_sent += len;
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
    pub fn new_with_limit(raw: W, size_limit: u64, checksum_enabled: ChecksumEnabled) -> Self {
        Self {
            raw: Some(raw),
            write_buffer: Vec::new(),
            state: AsyncWriteState::WritingVersion {
                version: PROTOCOL_VERSION.to_le_bytes(),
                len_sent: 0,
            },
            message_features: MessageFeatures {
                size_limit,
                checksum_enabled: checksum_enabled.into(),
            },
            primed_values: VecDeque::new(),
        }
    }

    /// Creates a typed writer, initializing it with a default size limit of 1 MB per message.
    /// Checksums are used to validate that messages arrived without corruption. **The checksum will only be used
    /// if both the reader and the writer enable it. If either one disables it, then no checking is performed.**
    pub fn new(raw: W, checksum_enabled: ChecksumEnabled) -> Self {
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
            AsyncWriteState::WritingValue { .. } => self.write_buffer.shrink_to_fit(),
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
