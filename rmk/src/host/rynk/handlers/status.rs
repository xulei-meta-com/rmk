//! Status handlers — current layer, matrix bitmap, battery, peripheral status,
//! plus the live getters for WPM / sleep / LED. Each value is read from its
//! producer-owned current-value accessor, not a host-side cache.

use rmk_types::protocol::rynk::{MATRIX_BITMAP_SIZE, MatrixState, RynkError, RynkMessage};

use super::super::RynkService;

impl<'a> RynkService<'a> {
    pub(crate) async fn handle_get_current_layer(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let layer = self.ctx.active_layer();
        msg.write_response(&layer)
    }

    pub(crate) async fn handle_get_matrix_state(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        // Sized for the maximum supported geometry — host slices it down
        // using num_rows / num_cols from `DeviceCapabilities`.
        let mut bitmap: heapless::Vec<u8, MATRIX_BITMAP_SIZE> = heapless::Vec::new();
        bitmap.resize_default(MATRIX_BITMAP_SIZE).expect("bitmap size matches");

        // Matrix tracking is gated on `host_security`. Without it, return
        // the zero bitmap so the wire shape stays consistent and tools
        // degrade cleanly to "no key pressed".
        #[cfg(feature = "host_security")]
        self.ctx.read_matrix_state(&mut bitmap);

        let state = MatrixState { pressed_bitmap: bitmap };
        msg.write_response(&state)
    }

    #[cfg(feature = "_ble")]
    pub(crate) async fn handle_get_battery_status(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let status = self.ctx.battery_status();
        msg.write_response(&status)
    }

    /// `Cmd::GetPeripheralStatus` — payload is a peripheral slot id. The
    /// snapshot is owned by the split central
    /// ([`current_peripheral_status`](crate::split::ble::central::current_peripheral_status)),
    /// fed at the `PeripheralConnectedEvent` / `PeripheralBatteryEvent` publish sites.
    #[cfg(all(feature = "_ble", feature = "split"))]
    pub(crate) async fn handle_get_peripheral_status(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let id = msg.request::<u8>()?;
        let status = crate::split::ble::central::current_peripheral_status(id as usize).ok_or(RynkError::Invalid)?;
        msg.write_response(&status)
    }

    pub(crate) async fn handle_get_wpm(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let wpm = crate::processor::builtin::wpm::current_wpm();
        msg.write_response(&wpm)
    }

    pub(crate) async fn handle_get_sleep_state(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let sleep = crate::state::current_sleep_state();
        msg.write_response(&sleep)
    }

    pub(crate) async fn handle_get_led_indicator(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let led = self.ctx.led_indicator();
        msg.write_response(&led)
    }
}
