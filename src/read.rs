use crate::{
    ChecksumEnabled, Error, CHECKSUM_DISABLED, CHECKSUM_ENABLED, PROTOCOL_VERSION, U16_MARKER,
    U32_MARKER, U64_MARKER, ZST_MARKER,
};
use bincode::Options;
use futures_core::Stream;
use futures_io::AsyncRead;
use serde::de::DeserializeOwned;
use serde::Serialize;
use siphasher::sip::SipHasher;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::mem::size_of;
use std::pin::Pin;
use std::task::{Context, Poll};

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
pub(crate) enum AsyncReadState {
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
pub(crate) enum ChecksumReadState {
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
    pub fn new_with_limit(raw: R, size_limit: u64, checksum_enabled: ChecksumEnabled) -> Self {
        Self {
            raw,
            size_limit,
            state: AsyncReadState::ReadingVersion {
                version_in_progress: [0; 8],
                version_in_progress_assigned: 0,
            },
            item_buffer: Vec::new(),
            checksum_read_state: checksum_enabled.into(),
            _phantom: PhantomData,
        }
    }

    /// Creates a typed reader, initializing it with a default size limit of 1 MB.
    pub fn new(raw: R, checksum_enabled: ChecksumEnabled) -> Self {
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

    pub(crate) fn poll_next_impl(
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
                            crate::bincode_options(size_limit)
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
                            crate::bincode_options(size_limit)
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
pub(crate) enum LenReadMode {
    U16,
    U32,
    U64,
}
