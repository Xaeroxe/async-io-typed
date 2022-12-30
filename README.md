# async-io-typed [![Build Status]][actions] [![Latest Version]][crates.io]

[Build Status]: https://img.shields.io/github/actions/workflow/status/Xaeroxe/async-io-typed/rust.yml?branch=main
[actions]: https://github.com/Xaeroxe/async-io-typed/actions?query=branch%3Amaster
[Latest Version]: https://img.shields.io/crates/v/async-io-typed.svg
[crates.io]: https://crates.io/crates/async-io-typed

[Documentation](https://docs.rs/async-io-typed)

Combines [`bincode`](https://github.com/bincode-org/bincode) and [`tokio`](https://github.com/tokio-rs/tokio) to
adapt any `AsyncRead` or `AsyncWrite` type into a channel for transmission of [`serde`](https://github.com/serde-rs/serde)
compatible Rust types.

## Binary format

### Introduction

The main offering of this crate is a consistent and known representation of Rust types. As such, the format is 
considered to be part of our stable API, and changing the format requires a major version number bump. To aid you 
in debugging, that format is documented here.

### High-level overview

The byte stream is split up into messages. Every message begins with a `length` value. After `length` bytes have 
been read, a new message can begin immediately afterward. This `length` value is the entirety of the header of a 
message. Messages have no footer. The bytes read out of a message are then deserialized into a Rust type via
[`bincode`](https://github.com/bincode-org/bincode), using the following configuration

```rust,ignore
bincode::DefaultOptions::new()
    .with_limit(size_limit)
    .with_little_endian()
    .with_varint_encoding()
    .reject_trailing_bytes()
```

### Length encoding

The length is encoded using a variably sized integer encoding scheme. To understand this scheme, first we need a few constant values.

```ignore
u16_marker; decimal: 252, hex: FC
u32_marker; decimal: 253, hex: FD
u64_marker; decimal: 254, hex: FE
zst_marker; decimal: 255, hex: FF
stream_end; decimal: 0,   hex: 00
```

Any length less than `u16_marker` and greater than 0 is encoded as a single byte whose value is the length.
A length of zero is encoded with the `zst_marker`. The stream is ended with the `stream_end` value. When this is
read the peer is expected to close the connection.

`async-io-typed` always uses little-endian. The user data being sent may contain values that are not 
little-endian, but `async-io-typed` itself always uses little-endian.

If the first byte is `u16_marker`, then the length is 16 bits wide, and encoded in the following 2 bytes. Once
those 2 bytes are read, the message begins. `u32_marker` and `u64_marker` are used in a similar way, each of 
those being 4 bytes, and 8 bytes respectively.

### Examples


Length 12
```ignore
0C
```

Length 0
```ignore
FF
```

Length 252 (First byte is u16_marker)
```ignore
FC, FC, 00
```

Length 253 (First byte is u16_marker)
```ignore
FC, FD, 00
```

Length 65,536 (aka 2^16) (First byte is u32_marker)
```ignore
FD, 00, 00, 01, 00
```

Length 4,294,967,296 (aka 2^32) (First byte is u64_marker)
```ignore
FE, 00, 00, 00, 00, 01, 00, 00, 00
```
