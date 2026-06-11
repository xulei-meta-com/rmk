//! Transport trait and shared transport errors.
//!
//! This module is the narrow byte-link seam a transport crate implements: just
//! [`Transport`], [`TransportError`], and [`MaybeSend`]. Request-layer types
//! (`RequestError`, `TopicFrame`) live in [`crate::client`] with the protocol
//! logic that owns them.

use thiserror::Error;

/// A byte link to a Rynk device.
///
/// Implementor contract — [`Client`](crate::client::Client) correctness rests
/// on it:
///
/// - [`send`](Self::send) must deliver the **whole frame, in order, with no
///   silent loss** — chunk internally to the medium, and use acknowledged
///   writes where the medium can drop (e.g. BLE). Cancelling a send mid-frame
///   may desync the link; that risk stays with the caller.
/// - [`recv`](Self::recv) must be **cancel-safe**: dropping its future must
///   not lose a delivered chunk. The client relies on this for caller-owned
///   timeouts plus [`Client::resync`](crate::client::Client::resync).
/// - [`recv`](Self::recv) may return arbitrary chunk boundaries; the client
///   reassembles frames.
///
/// The shipped impls conform: serial reads into an owned buffer, BLE forwards
/// a tokio mpsc channel — both cancel-safe by construction.
pub trait Transport: MaybeSend {
    /// Send one complete protocol frame.
    fn send(&mut self, frame: &[u8]) -> impl core::future::Future<Output = Result<(), TransportError>> + MaybeSend;

    /// Receive the next byte chunk.
    fn recv(&mut self) -> impl core::future::Future<Output = Result<Vec<u8>, TransportError>> + MaybeSend;
}

/// `&mut T` is itself a transport, so `Client::connect(&mut t)` borrows the
/// link — after a same-stream handshake failure like a major-version
/// mismatch, the caller still owns `t` and can retry with a client built for
/// the firmware's major.
impl<T: Transport + ?Sized> Transport for &mut T {
    fn send(&mut self, frame: &[u8]) -> impl core::future::Future<Output = Result<(), TransportError>> + MaybeSend {
        (**self).send(frame)
    }

    fn recv(&mut self) -> impl core::future::Future<Output = Result<Vec<u8>, TransportError>> + MaybeSend {
        (**self).recv()
    }
}

/// Transport setup and I/O errors.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport disconnected")]
    Disconnected,
    #[error("io error: {0}")]
    Io(String),
    #[error("device not found: {0}")]
    DeviceNotFound(String),
}

/// `Send` on native targets, no-op on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}
