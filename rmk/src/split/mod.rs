use postcard::experimental::max_size::MaxSize;
use rmk_types::connection::ConnectionStatus;
use serde::{Deserialize, Serialize};

#[cfg(feature = "_ble")]
use crate::event::BatteryStatusEvent;
use crate::event::{KeyboardEvent, PointingEvent};

#[cfg(feature = "_ble")]
pub mod ble;
pub mod central;
/// Common abstraction layer of split driver
pub(crate) mod driver;
pub mod peripheral;
#[cfg(feature = "rp2040")]
pub mod rp;
#[cfg(not(feature = "_ble"))]
pub mod serial;

/// Maximum size of a split message
pub const SPLIT_MESSAGE_MAX_SIZE: usize = SplitMessage::POSTCARD_MAX_SIZE + 4;

/// Message used from central & peripheral communication
#[repr(u8)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, MaxSize)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub(crate) enum SplitMessage {
    /// Keyboard event, from peripheral to central
    Key(KeyboardEvent),
    /// Pointing device event, from peripheral to central
    Pointing(PointingEvent),
    /// Led state, on/off, from central to peripheral
    LedState(bool),
    /// `ConnectionStatus` snapshot of the central.
    /// Synced central → peripheral on every change.
    ConnectionStatus(ConnectionStatus),
    /// BLE Address, used in syncing address between central and peripheral
    Address([u8; 6]),
    /// Clear the saved peer info
    ClearPeer,
    /// Lock state led indicator from central to peripheral
    KeyboardIndicator(u8),
    /// Layer number from central to peripheral
    Layer(u8),
    /// Cross-component RGB effect state. Sent central → peripheral on effect change
    /// and on (re)connect. All ticks are in **central** milliseconds: `start_tick` is
    /// the epoch when the current effect started (phase origin); `central_tick` is the
    /// central time at send (clock anchor a peripheral uses to derive its offset).
    RgbEffect {
        effect: u8,
        hue: u8,
        sat: u8,
        val: u8,
        speed: u8,
        start_tick: u32,
        central_tick: u32,
    },
    /// A reactive RGB hit at the pressed key's matrix position `(row, col)`, stamped in
    /// **central** time (`at_tick`). Sent peripheral → central, then relayed central → other peripherals.
    RgbHit { row: u8, col: u8, at_tick: u32 },
    /// WPM from central to peripheral
    #[cfg(feature = "display")]
    Wpm(u16),
    /// Modifier state from central to peripheral
    #[cfg(feature = "display")]
    Modifier(u8),
    /// Sleep state from central to peripheral
    #[cfg(feature = "display")]
    SleepState(bool),
    /// Battery status, from peripheral to central
    #[cfg(feature = "_ble")]
    BatteryStatus(BatteryStatusEvent),
}
