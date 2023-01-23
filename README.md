# async-io-typed [![Build Status]][actions] [![Latest Version]][crates.io]

[Build Status]: https://img.shields.io/github/actions/workflow/status/Xaeroxe/async-io-typed/rust.yml?branch=main
[actions]: https://github.com/Xaeroxe/async-io-typed/actions?query=branch%3Amain
[Latest Version]: https://img.shields.io/crates/v/async-io-typed.svg
[crates.io]: https://crates.io/crates/async-io-typed

[Documentation](https://docs.rs/async-io-typed)

Combines [`bincode`](https://github.com/bincode-org/bincode) and [`futures`](https://github.com/rust-lang/futures-rs) to
adapt any `AsyncRead` or `AsyncWrite` type into a channel for transmission of [`serde`](https://github.com/serde-rs/serde)
compatible Rust types.

## Who needs this?

If you have an endpoint you need to communicate with and

- You can establish some kind of I/O connection to it (i.e. TCP, named pipes, or a unix socket)
- You need clear message boundaries
- You're not trying to conform to an existing wire format such as HTTP or protobufs. This crate uses a custom format.
- The data you wish to send can be easily represented in a Rust type, and that type implements serde's `Deserialize` and `Serialize` traits.

Then this crate might be useful to you!

## Who doesn't need this?

If the endpoint is in the same process then you should not use this crate. You're better served by existing async mpsc channels.
Many crates provide async mpsc channels, including `futures` and `tokio`. Pick your favorite implementation. Additionally, if you're
trying to interface with a process that doesn't have Rust code, and can't adopt a Rust portion, this crate will hurt much more than
it will help. Consider using protobufs or JSON if Rust adoption is a blocker.

## Binary format

Details on the binary format used by this crate can be found in [the binary format specification](https://github.com/Xaeroxe/async-io-typed/blob/main/MESSAGE_BINARY_FORMAT.md).