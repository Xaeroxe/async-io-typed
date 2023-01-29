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

use std::io;

use bincode::Options;

#[cfg(test)]
mod tests;

mod duplex;
mod read;
mod write;

pub use duplex::*;
pub use read::*;
pub use write::*;

pub(crate) const U16_MARKER: u8 = 252;
pub(crate) const U32_MARKER: u8 = 253;
pub(crate) const U64_MARKER: u8 = 254;
pub(crate) const ZST_MARKER: u8 = 255;

// These values chosen such that zeroed data and data that is all ones doesn't resemble either of them.
pub(crate) const CHECKSUM_ENABLED: u8 = 2;
pub(crate) const CHECKSUM_DISABLED: u8 = 3;

// Current protocol version. The original implementation had no version number, version 2 is the first version with a version number.
pub(crate) const PROTOCOL_VERSION: u64 = 2;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChecksumEnabled {
    Yes,
    No,
}

impl From<bool> for ChecksumEnabled {
    fn from(value: bool) -> Self {
        if value {
            ChecksumEnabled::Yes
        } else {
            ChecksumEnabled::No
        }
    }
}

impl From<ChecksumEnabled> for bool {
    fn from(value: ChecksumEnabled) -> Self {
        value == ChecksumEnabled::Yes
    }
}

impl From<ChecksumEnabled> for ChecksumReadState {
    fn from(value: ChecksumEnabled) -> Self {
        match value {
            ChecksumEnabled::Yes => ChecksumReadState::Yes,
            ChecksumEnabled::No => ChecksumReadState::No,
        }
    }
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
