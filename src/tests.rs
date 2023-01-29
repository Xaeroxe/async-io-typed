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

use std::{
    net::{Ipv4Addr, SocketAddrV4},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{
    io::{AsyncReadExt, AsyncWriteExt},
    Sink,
};
use serde::{de::DeserializeOwned, Serialize, Deserialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver},
};
use tokio_util::{
    compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt},
    sync::PollSender,
};

// What follows is an intentionally obnoxious `AsyncRead` and `AsyncWrite` implementation. Please don't use this outside of tests.
struct BasicChannelSender {
    max_size_per_write: usize,
    sender: PollSender<Vec<u8>>,
}

impl AsyncWrite for BasicChannelSender {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<futures_io::Result<usize>> {
        if futures_core::ready!(self.sender.poll_reserve(cx)).is_err() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "remote hung up",
            )));
        }
        let write_len = self.max_size_per_write.min(buf.len());
        self.sender
            .send_item(buf[..write_len].to_vec())
            .expect("receiver hung up!");
        Poll::Ready(Ok(write_len))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<futures_io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<futures_io::Result<()>> {
        self.sender.close();
        Poll::Ready(Ok(()))
    }
}

struct BasicChannelReceiver {
    receiver: Receiver<Vec<u8>>,
    last_received: Vec<u8>,
}

impl AsyncRead for BasicChannelReceiver {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<futures_io::Result<usize>> {
        let mut len_written = 0;
        loop {
            if !self.last_received.is_empty() {
                let copy_len = self.last_received.len().min(buf.len() - len_written);
                buf[len_written..(len_written + copy_len)]
                    .copy_from_slice(&self.last_received[..copy_len]);
                self.last_received = self.last_received.split_off(copy_len);
                len_written += copy_len;
                if len_written == buf.len() {
                    return Poll::Ready(Ok(buf.len()));
                }
            } else {
                self.last_received = match self.receiver.poll_recv(cx) {
                    Poll::Ready(Some(v)) => v,
                    Poll::Ready(None) => {
                        return if len_written > 0 {
                            Poll::Ready(Ok(len_written))
                        } else {
                            Poll::Pending
                        }
                    }
                    Poll::Pending => {
                        return if len_written > 0 {
                            Poll::Ready(Ok(len_written))
                        } else {
                            Poll::Pending
                        }
                    }
                }
            }
        }
    }
}

fn basic_channel(max_size_per_write: usize) -> (BasicChannelSender, BasicChannelReceiver) {
    let (sender, receiver) = mpsc::channel(32);
    (
        BasicChannelSender {
            sender: PollSender::new(sender),
            max_size_per_write,
        },
        BasicChannelReceiver {
            receiver,
            last_received: Vec::new(),
        },
    )
}

#[tokio::test]
async fn bad_protocol_version() {
    let (mut sender, receiver) = basic_channel(1024);
    let mut typed_receiver = AsyncReadTyped::<_, u8>::new(receiver, ChecksumEnabled::No);
    // Intentionally send a message with a bad checksum
    let sent_value = 5;
    let mut message = Vec::from(0u64.to_le_bytes());
    message.push(CHECKSUM_ENABLED);
    message.push(1);
    message.push(sent_value);
    message.extend(0u64.to_le_bytes());
    sender.write_all(&message).await.unwrap();
    let message = typed_receiver.next().await.unwrap();
    assert!(matches!(
        message,
        Err(Error::ProtocolVersionMismatch {
            our_version: PROTOCOL_VERSION,
            their_version: 0
        })
    ));
    assert!(typed_receiver.next().await.is_none());
}

#[tokio::test]
async fn bad_checksum_enabled_value() {
    let (mut sender, receiver) = basic_channel(1024);
    let mut typed_receiver = AsyncReadTyped::<_, u8>::new(receiver, ChecksumEnabled::No);
    // Intentionally send a message with a bad checksum
    let sent_value = 5;
    const BAD_CHECKSUM_ENABLED_VALUE: u8 = 42;
    let mut message = Vec::from(PROTOCOL_VERSION.to_le_bytes());
    message.push(BAD_CHECKSUM_ENABLED_VALUE);
    message.push(1);
    message.push(sent_value);
    message.extend(0u64.to_le_bytes());
    sender.write_all(&message).await.unwrap();
    let message = typed_receiver.next().await.unwrap();
    assert!(matches!(
        message,
        Err(Error::ChecksumHandshakeFailed {
            checksum_value: BAD_CHECKSUM_ENABLED_VALUE
        })
    ));
    assert!(typed_receiver.next().await.is_none());
}

#[tokio::test]
async fn checksum_ignored() {
    let (mut sender, receiver) = basic_channel(1024);
    let mut typed_receiver = AsyncReadTyped::new(receiver, ChecksumEnabled::No);
    // Intentionally send a message with a bad checksum
    let sent_value = 5;
    let mut message = Vec::from(PROTOCOL_VERSION.to_le_bytes());
    message.push(CHECKSUM_ENABLED);
    message.push(1);
    message.push(sent_value);
    message.extend(0u64.to_le_bytes());
    sender.write_all(&message).await.unwrap();
    let message: u8 = typed_receiver.next().await.unwrap().unwrap();
    assert_eq!(message, sent_value);
}

#[tokio::test]
async fn checksum_unavailable() {
    let (mut sender, receiver) = basic_channel(1024);
    let mut typed_receiver = AsyncReadTyped::<_, u8>::new(receiver, ChecksumEnabled::Yes);
    assert!(typed_receiver.checksum_enabled());
    // Send two message without checksums.
    const SENT_VALUE: u8 = 5;
    let mut message = Vec::from(PROTOCOL_VERSION.to_le_bytes());
    message.push(CHECKSUM_DISABLED);
    message.push(1);
    message.push(SENT_VALUE);
    sender.write_all(&message).await.unwrap();
    let result = typed_receiver.next().await.unwrap();
    assert!(!typed_receiver.checksum_enabled());
    assert_eq!(result.unwrap(), SENT_VALUE);
    const SENT_VALUE_2: u8 = 5;
    message.clear();
    message.push(1);
    message.push(SENT_VALUE_2);
    sender.write_all(&message).await.unwrap();
    let result = typed_receiver.next().await.unwrap();
    assert!(!typed_receiver.checksum_enabled());
    assert_eq!(result.unwrap(), SENT_VALUE_2);
}

#[tokio::test]
async fn checksum_used() {
    let (mut sender, receiver) = basic_channel(1024);
    let mut typed_receiver = AsyncReadTyped::<_, u8>::new(receiver, ChecksumEnabled::Yes);
    // Intentionally send a message with a bad checksum
    const SENT_VALUE: u8 = 5;
    const SENT_VALUE_CHECKSUM: u64 = 10536747468361244917;
    let mut message = Vec::from(PROTOCOL_VERSION.to_le_bytes());
    message.push(CHECKSUM_ENABLED);
    message.push(1);
    message.push(SENT_VALUE);
    message.extend(0u64.to_le_bytes());
    sender.write_all(&message).await.unwrap();
    let result = typed_receiver.next().await.unwrap();
    assert!(typed_receiver.checksum_enabled());
    assert!(matches!(
        result,
        Err(Error::ChecksumMismatch {
            sent_checksum: 0,
            computed_checksum: SENT_VALUE_CHECKSUM
        })
    ));
    // Now send one with a good checksum.
    message.clear();
    message.push(1);
    message.push(SENT_VALUE);
    message.extend(SENT_VALUE_CHECKSUM.to_le_bytes());
    sender.write_all(&message).await.unwrap();
    let result = typed_receiver.next().await.unwrap();
    assert!(typed_receiver.checksum_enabled());
    assert_eq!(result.unwrap(), SENT_VALUE);
}

#[tokio::test]
async fn checksum_unused() {
    let (mut sender, receiver) = basic_channel(1024);
    let mut typed_receiver = AsyncReadTyped::<_, u8>::new(receiver, ChecksumEnabled::Yes);
    // Send two messages with no checksum
    const SENT_VALUE: u8 = 5;
    const SENT_VALUE_2: u8 = 20;
    let mut message = Vec::from(PROTOCOL_VERSION.to_le_bytes());
    message.push(CHECKSUM_DISABLED);
    message.push(1);
    message.push(SENT_VALUE);
    message.push(1);
    message.push(SENT_VALUE_2);
    sender.write_all(&message).await.unwrap();
    let result = typed_receiver.next().await.unwrap();
    assert!(!typed_receiver.checksum_enabled());
    assert_eq!(result.unwrap(), SENT_VALUE);
    let result = typed_receiver.next().await.unwrap();
    assert!(!typed_receiver.checksum_enabled());
    assert_eq!(result.unwrap(), SENT_VALUE_2);
}

#[tokio::test]
async fn checksum_sent() {
    let (sender, mut receiver) = basic_channel(1024);
    let mut typed_sender = AsyncWriteTyped::new(sender, ChecksumEnabled::Yes);
    const SENT_VALUE: u8 = 5;
    const SENT_VALUE_CHECKSUM: u64 = 10536747468361244917;
    typed_sender.send(SENT_VALUE).await.unwrap();
    const SENT_VALUE_2: u8 = 20;
    const SENT_VALUE_2_CHECKSUM: u64 = 16424472559682478309;
    typed_sender.send(SENT_VALUE_2).await.unwrap();
    const READ_LENGTH: usize = 29;
    let mut receive_buffer = [0; READ_LENGTH];
    receiver.read_exact(&mut receive_buffer).await.unwrap();
    let mut expected = Vec::from(PROTOCOL_VERSION.to_le_bytes());
    expected.push(CHECKSUM_ENABLED);
    expected.push(1);
    expected.push(SENT_VALUE);
    expected.extend(SENT_VALUE_CHECKSUM.to_le_bytes());
    expected.push(1);
    expected.push(SENT_VALUE_2);
    expected.extend(SENT_VALUE_2_CHECKSUM.to_le_bytes());
    assert_eq!(receive_buffer.as_slice(), expected.as_slice());
}

#[tokio::test]
async fn checksum_not_sent() {
    let (sender, mut receiver) = basic_channel(1024);
    let mut typed_sender = AsyncWriteTyped::new(sender, ChecksumEnabled::No);
    const SENT_VALUE: u8 = 5;
    typed_sender.send(SENT_VALUE).await.unwrap();
    const SENT_VALUE_2: u8 = 20;
    typed_sender.send(SENT_VALUE_2).await.unwrap();
    const READ_LENGTH: usize = 13;
    let mut receive_buffer = [0; READ_LENGTH];
    receiver.read_exact(&mut receive_buffer).await.unwrap();
    let mut expected = Vec::from(PROTOCOL_VERSION.to_le_bytes());
    expected.push(CHECKSUM_DISABLED);
    expected.push(1);
    expected.push(SENT_VALUE);
    expected.push(1);
    expected.push(SENT_VALUE_2);
    assert_eq!(receive_buffer.as_slice(), expected.as_slice());
}

// This tests our testing equipment, just makes sure the above implementations are correct.
#[tokio::test(flavor = "multi_thread")]
async fn basic_channel_test() {
    for i in (1..10).chain(Some(usize::MAX)) {
        {
            let (mut sender, mut receiver) = basic_channel(i);
            let message = "Hello World!".as_bytes();
            let mut read_buf = vec![0; message.len()];
            let write = tokio::spawn(async move { sender.write_all(message).await });
            tokio::time::timeout(Duration::from_secs(2), receiver.read_exact(&mut read_buf))
                .await
                .unwrap()
                .unwrap();
            write.await.unwrap().unwrap();
            assert_eq!(message, read_buf);
        }
        {
            let (sender, mut receiver) = basic_channel(i);
            let mut sender = Some(sender);
            for _ in 0..10 {
                let message = (0..255).collect::<Vec<u8>>();
                let mut read_buf = vec![0; message.len()];
                let message_clone = message.clone();
                let mut sender_inner = sender.take().unwrap();
                let write = tokio::spawn(async move {
                    sender_inner.write_all(&message_clone).await.unwrap();
                    sender_inner
                });
                tokio::time::timeout(Duration::from_secs(2), receiver.read_exact(&mut read_buf))
                    .await
                    .unwrap()
                    .unwrap();
                sender = Some(write.await.unwrap());
                assert_eq!(message, read_buf);
            }
        }
    }
}

use std::mem;

use bincode::Options;
use futures_util::{SinkExt, StreamExt};
use tokio::task::JoinHandle;

use super::*;

fn start_send_helper<
    T: Serialize + DeserializeOwned + Unpin + Send + 'static,
    S: Sink<T> + Unpin + Send + 'static,
>(
    mut s: S,
    value: T,
) -> JoinHandle<(S, Result<(), S::Error>)>
where
    S::Error: Send,
{
    tokio::spawn(async move {
        let ret = s.send(value).await;
        (s, ret)
    })
}

fn make_channel<T: DeserializeOwned + Serialize + Unpin>(
    max_size_per_write: usize,
    sender_checksum_enabled: bool,
    receiver_checksum_enabled: bool,
) -> (
    Option<AsyncWriteTyped<BasicChannelSender, T>>,
    AsyncReadTyped<BasicChannelReceiver, T>,
) {
    let (sender, receiver) = basic_channel(max_size_per_write);
    (
        Some(AsyncWriteTyped::new(sender, sender_checksum_enabled.into())),
        AsyncReadTyped::new(receiver, receiver_checksum_enabled.into()),
    )
}

// Copy paste of the options from async-io-typed.
fn bincode_options(size_limit: u64) -> impl Options {
    // Two of these are defaults, so you might say this is over specified. I say it's future proof, as
    // bincode default changes won't introduce accidental breaking changes.
    bincode::DefaultOptions::new()
        .with_limit(size_limit)
        .with_little_endian()
        .with_varint_encoding()
        .reject_trailing_bytes()
}

fn standard_test_parameter_set() -> impl Iterator<Item = TestParameters> {
    (1..=3)
        .chain(8..=10)
        .chain(Some(1024usize.pow(2)))
        .flat_map(|size| {
            (0..=1).map(move |sender_checksum_enabled| {
                (0..=1).map(move |receiver_checksum_enabled| {
                    (
                        size,
                        sender_checksum_enabled == 1,
                        receiver_checksum_enabled == 1,
                    )
                })
            })
        })
        .flatten()
        .map(
            |(max_size_per_write, sender_checksum_enabled, receiver_checksum_enabled)| {
                TestParameters {
                    max_size_per_write,
                    sender_checksum_enabled,
                    receiver_checksum_enabled,
                }
            },
        )
}

struct TestParameters {
    max_size_per_write: usize,
    sender_checksum_enabled: bool,
    receiver_checksum_enabled: bool,
}

#[tokio::test]
async fn zero_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let fut = start_send_helper(server_stream.take().unwrap(), ());
        client_stream.next().await.unwrap().unwrap();
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn zero_len_messages() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        for _ in 0..100 {
            let fut = start_send_helper(server_stream.take().unwrap(), ());
            client_stream.next().await.unwrap().unwrap();
            let (stream, result) = fut.await.unwrap();
            server_stream = Some(stream);
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn hello_world() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let message = "Hello, world!".as_bytes().to_vec();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

// Mostly just here to have at least one test using sort of real world conditions.
#[tokio::test]
async fn hello_world_tokio_tcp() {
    let tcp_listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let port = tcp_listener.local_addr().unwrap().port();
    let accept_fut = tcp_listener.accept();
    let mut client_stream = DuplexStreamTyped::<_, Vec<u8>>::new(
        TcpStream::connect(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap()
            .compat(),
        ChecksumEnabled::Yes,
    );
    let (server_stream, _address) = accept_fut.await.unwrap();
    let mut server_stream = Some(DuplexStreamTyped::new(
        server_stream.compat_write(),
        ChecksumEnabled::Yes,
    ));
    let message = "Hello, world!".as_bytes().to_vec();
    let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
    assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
    fut.await.unwrap().1.unwrap();
}

#[tokio::test]
async fn blog_example() {
    #[derive(Deserialize, Serialize)]
    pub struct MyMessage {
        pub field_1: bool,
        pub field_2: String,
    }

    let tcp_listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    // Get the port number that the operating system assigned to us.
    let port = tcp_listener.local_addr().unwrap().port();
    // Start accepting a new connection. We intentionally don't await this future yet,
    // because that would prevent us from creating the client connecting to it.
    let accept_fut = tcp_listener.accept();
    // Create a new `DuplexStreamTyped` which exchanges `MyMessage` type, by connecting
    // to the newly opened TCP port. We then wrap the connection in a compatibility layer
    // so that `tokio::io` and `futures_io` can work together. The second boolean parameter
    // here disables checksums. TCP already does error checking for us, so we can disable the
    // checksum.
    let mut client_stream = DuplexStreamTyped::<_, MyMessage>::new(
        TcpStream::connect(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap()
            .compat(),
        ChecksumEnabled::No,
    );
    // Now that a connection is established, let's resolve our accept future from earlier.
    let (server_stream, _address) = accept_fut.await.unwrap();
    // Wrap the connection in a `DuplexStreamTyped`, with checksums off.
    let mut server_stream = DuplexStreamTyped::new(server_stream.compat_write(), ChecksumEnabled::No);
    // Let's compose the message to send
    let message = MyMessage {
        field_1: true,
        field_2: String::from("Hello World!"),
    };
    // Send the message
    server_stream.send(message).await.unwrap();
    // Receive the message
    let _ = client_stream.next().await.unwrap().unwrap();
    // Do whatever you want to do with the received message...
}

#[tokio::test]
async fn shutdown_after_hello_world() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let message = "Hello, world!".as_bytes().to_vec();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        let (server_stream, result) = fut.await.unwrap();
        result.unwrap();
        mem::drop(server_stream);
        let next = client_stream.next().await;
        assert!(next.is_none(), "{next:?} was not none");
    }
}

#[tokio::test]
async fn hello_worlds() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        for i in 0..100 {
            let message = format!("Hello, world {}!", i).into_bytes();
            let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
            assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
            let (stream, result) = fut.await.unwrap();
            server_stream = Some(stream);
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn u16_marker_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let bincode_config = bincode_options(1024);
        let message = (0..248).map(|_| 1).chain(Some(300)).collect::<Vec<_>>();
        assert_eq!(bincode_config.serialize(&message).unwrap().len(), 252);
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u32_marker_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let bincode_config = bincode_options(1024);
        let message = (0..249).map(|_| 1).chain(Some(300)).collect::<Vec<_>>();
        assert_eq!(bincode_config.serialize(&message).unwrap().len(), 253);
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u64_marker_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let bincode_config = bincode_options(1024);
        let message = (0..251).map(|_| 240u8).collect::<Vec<_>>();
        assert_eq!(bincode_config.serialize(&message).unwrap().len(), 254);
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn zst_marker_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let bincode_config = bincode_options(1024);
        let message = (0u8..252).collect::<Vec<_>>();
        assert_eq!(bincode_config.serialize(&message).unwrap().len(), 255);
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn random_len_test() {
    use rand::Rng;
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        for _ in 0..800 {
            let message = (0..(rand::thread_rng().gen_range(0..(u8::MAX as u32 + 1) / 4)))
                .collect::<Vec<_>>();
            let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
            assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
            let (stream, result) = fut.await.unwrap();
            server_stream = Some(stream);
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn u16_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let message = (0..(u8::MAX as u16 + 1) / 2).collect::<Vec<_>>();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u16_len_messages() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        for _ in 0..10 {
            let message = (0..(u8::MAX as u16 + 1) / 2)
                .map(|_| 258u16)
                .collect::<Vec<_>>();
            let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
            assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
            let (stream, result) = fut.await.unwrap();
            server_stream = Some(stream);
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn u32_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let message = (0..(u16::MAX as u32 + 1) / 4).collect::<Vec<_>>();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u32_len_messages() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        for _ in 0..10 {
            let message = (0..(u16::MAX as u32 + 1) / 4).collect::<Vec<_>>();
            let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
            assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
            let (stream, result) = fut.await.unwrap();
            server_stream = Some(stream);
            result.unwrap();
        }
    }
}

// It takes a ridiculous amount of time to run the u64 tests
#[ignore]
#[tokio::test]
async fn u64_len_message() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        let message = (0..(u32::MAX as u64 + 1) / 8).collect::<Vec<_>>();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[ignore]
#[tokio::test]
async fn u64_len_messages() {
    for TestParameters {
        max_size_per_write,
        sender_checksum_enabled,
        receiver_checksum_enabled,
    } in standard_test_parameter_set()
    {
        let (mut server_stream, mut client_stream) = make_channel(
            max_size_per_write,
            sender_checksum_enabled,
            receiver_checksum_enabled,
        );
        for _ in 0..10 {
            let message = (0..(u32::MAX as u64 + 1) / 8).collect::<Vec<_>>();
            let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
            assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
            let (stream, result) = fut.await.unwrap();
            server_stream = Some(stream);
            result.unwrap();
        }
    }
}
