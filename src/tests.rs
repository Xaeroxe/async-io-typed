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
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver};
use tokio_util::sync::PollSender;

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
            .send_item((&buf[..write_len]).to_vec())
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
            if self.last_received.len() > 0 {
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

fn start_send_helper<T: Serialize + DeserializeOwned + Unpin + Send + 'static>(
    mut s: AsyncWriteTyped<BasicChannelSender, T>,
    value: T,
) -> JoinHandle<(AsyncWriteTyped<BasicChannelSender, T>, Result<(), Error>)> {
    tokio::spawn(async move {
        let ret = s.send(value).await;
        (s, ret)
    })
}

fn make_channel<T: DeserializeOwned + Serialize + Unpin>(
    max_size_per_write: usize,
) -> (
    Option<AsyncWriteTyped<BasicChannelSender, T>>,
    AsyncReadTyped<BasicChannelReceiver, T>,
) {
    let (sender, receiver) = basic_channel(max_size_per_write);
    (
        Some(AsyncWriteTyped::new(sender)),
        AsyncReadTyped::new(receiver),
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

fn interesting_sizes() -> impl Iterator<Item = usize> {
    (1..=3).chain(8..=10).chain(Some(1024usize.pow(2)))
}

#[tokio::test]
async fn zero_len_message() {
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
        let fut = start_send_helper(server_stream.take().unwrap(), ());
        client_stream.next().await.unwrap().unwrap();
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn zero_len_messages() {
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
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
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
        let message = "Hello, world!".as_bytes().to_vec();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn shutdown_after_hello_world() {
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
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
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
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
    for size in interesting_sizes() {
        let bincode_config = bincode_options(1024);

        let (mut server_stream, mut client_stream) = make_channel(size);
        let message = (0..248).map(|_| 1).chain(Some(300)).collect::<Vec<_>>();
        assert_eq!(bincode_config.serialize(&message).unwrap().len(), 252);
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u32_marker_len_message() {
    for size in interesting_sizes() {
        let bincode_config = bincode_options(1024);
        let (mut server_stream, mut client_stream) = make_channel(size);
        let message = (0..249).map(|_| 1).chain(Some(300)).collect::<Vec<_>>();
        assert_eq!(bincode_config.serialize(&message).unwrap().len(), 253);
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u64_marker_len_message() {
    for size in interesting_sizes() {
        let bincode_config = bincode_options(1024);
        let (mut server_stream, mut client_stream) = make_channel(size);
        let message = (0..251).map(|_| 240u8).collect::<Vec<_>>();
        assert_eq!(bincode_config.serialize(&message).unwrap().len(), 254);
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn zst_marker_len_message() {
    for size in interesting_sizes() {
        let bincode_config = bincode_options(1024);
        let (mut server_stream, mut client_stream) = make_channel(size);
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
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
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
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
        let message = (0..(u8::MAX as u16 + 1) / 2).collect::<Vec<_>>();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u16_len_messages() {
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
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
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
        let message = (0..(u16::MAX as u32 + 1) / 4).collect::<Vec<_>>();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[tokio::test]
async fn u32_len_messages() {
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
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
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
        let message = (0..(u32::MAX as u64 + 1) / 8).collect::<Vec<_>>();
        let fut = start_send_helper(server_stream.take().unwrap(), message.clone());
        assert_eq!(client_stream.next().await.unwrap().unwrap(), message);
        fut.await.unwrap().1.unwrap();
    }
}

#[ignore]
#[tokio::test]
async fn u64_len_messages() {
    for size in interesting_sizes() {
        let (mut server_stream, mut client_stream) = make_channel(size);
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
