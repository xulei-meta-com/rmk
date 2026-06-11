//! Runtime-free Rynk host protocol client.
//!
//! [`Client`] drives the Rynk protocol over any [`Transport`] — a byte link to
//! a device. This crate does not open devices and does not depend on an async
//! runtime. Native, BLE, web, and third-party transports live in separate crates
//! that implement [`Transport`] and hand the link to [`Client::connect`].
//!
//! ```no_run
//! # use rynk::{Client, Transport};
//! # async fn run<T: Transport>(transport: T) -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = Client::connect(transport).await?;
//! let layer = client.get_current_layer().await?;
//! println!("active layer: {layer}");
//! # Ok(()) }
//! ```
//!
//! Each method returns the response value directly; a device rejection is
//! [`RequestError::Rejected`], so `?` carries both transport and firmware
//! failures.
//!
//! ## Multi-version dispatch
//!
//! [`Client::connect`] rejects only a protocol **major** mismatch. To support
//! several majors at once, link one `rynk` build per major (cargo
//! `package` renames) and probe with the newest first: `&mut T` is itself a
//! [`Transport`], so `Client::connect(&mut transport)` borrows the link, and
//! on [`ConnectError::VersionMismatch`] the handshake round trip has already
//! completed — the same transport retries cleanly with the next client. The
//! probe itself (`GetVersion`, the 5-byte header, and the version reply) is
//! frozen across all majors by the protocol ICD.

pub mod client;
pub mod transport;

pub use client::{Client, ConnectError, Event, RequestError, TopicFrame};
/// The protocol/wire types appearing in [`Client`] method signatures,
/// re-exported so downstream crates import them from the matching version.
pub use rmk_types;
pub use transport::{MaybeSend, Transport, TransportError};
