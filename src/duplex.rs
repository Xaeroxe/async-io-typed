use crate::read::{AsyncReadState, AsyncReadTyped, ChecksumReadState};
use crate::write::{AsyncWriteState, AsyncWriteTyped, MessageFeatures};
use crate::{Error, PROTOCOL_VERSION};
use futures_core::Stream;
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{Sink, SinkExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

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
