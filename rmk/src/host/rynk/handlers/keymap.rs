//! Keymap and encoder handlers (encoder is part of keymap's `0x01xx` Cmd group).

use rmk_types::protocol::rynk::{
    GetEncoderRequest, KeyPosition, RynkError, RynkMessage, SetEncoderRequest, SetKeyRequest,
};

use super::super::RynkService;

impl<'a> RynkService<'a> {
    pub(crate) async fn handle_get_key_action(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let pos = msg.request::<KeyPosition>()?;
        let (rows, cols, num_layers) = self.ctx.keymap_dimensions();
        // An out-of-range position is a semantic error — reads and writes use
        // the same bounds (see `handle_set_key_action`).
        if (pos.layer as usize) >= num_layers || (pos.row as usize) >= rows || (pos.col as usize) >= cols {
            return Err(RynkError::Invalid);
        }
        let action = self.ctx.get_action(pos.layer, pos.row, pos.col);
        msg.write_response(&action)
    }

    pub(crate) async fn handle_set_key_action(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let set = msg.request::<SetKeyRequest>()?;
        let (rows, cols, num_layers) = self.ctx.keymap_dimensions();
        if (set.position.layer as usize) >= num_layers
            || (set.position.row as usize) >= rows
            || (set.position.col as usize) >= cols
        {
            return Err(RynkError::Invalid);
        }
        self.ctx
            .set_action(set.position.layer, set.position.row, set.position.col, set.action)
            .await;
        msg.write_response(&())
    }

    pub(crate) async fn handle_get_default_layer(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let layer = self.ctx.default_layer();
        msg.write_response(&layer)
    }

    pub(crate) async fn handle_set_default_layer(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let layer = msg.request::<u8>()?;
        let (_, _, num_layers) = self.ctx.keymap_dimensions();
        if (layer as usize) >= num_layers {
            return Err(RynkError::Invalid);
        }
        self.ctx.set_default_layer(layer).await;
        msg.write_response(&())
    }

    pub(crate) async fn handle_get_encoder_action(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let r = msg.request::<GetEncoderRequest>()?;
        self.check_encoder_bounds(r.layer, r.encoder_id)?;
        let action = self.ctx.get_encoder(r.layer, r.encoder_id).ok_or(RynkError::Invalid)?;
        msg.write_response(&action)
    }

    pub(crate) async fn handle_set_encoder_action(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let r = msg.request::<SetEncoderRequest>()?;
        self.check_encoder_bounds(r.layer, r.encoder_id)?;
        // No clean "set whole encoder" accessor exists yet — split into two writes.
        self.ctx
            .set_encoder_clockwise(r.layer, r.encoder_id, r.action.clockwise)
            .await;
        self.ctx
            .set_encoder_counter_clockwise(r.layer, r.encoder_id, r.action.counter_clockwise)
            .await;
        msg.write_response(&())
    }

    /// `Invalid` for an out-of-range encoder. Checks `layer` and `encoder_id`
    /// explicitly rather than relying on `get_encoder` returning `None`: the
    /// keymap flat-indexes encoders (`layer * num_encoder + id`), so an
    /// over-range `id` would otherwise alias into another layer's slot.
    fn check_encoder_bounds(&self, layer: u8, encoder_id: u8) -> Result<(), RynkError> {
        let (_, _, num_layers) = self.ctx.keymap_dimensions();
        if (layer as usize) >= num_layers || (encoder_id as usize) >= self.ctx.num_encoders() {
            return Err(RynkError::Invalid);
        }
        Ok(())
    }

    #[cfg(feature = "bulk")]
    pub(crate) async fn handle_get_keymap_bulk(&self, _msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        Err(RynkError::Unimplemented)
    }

    #[cfg(feature = "bulk")]
    pub(crate) async fn handle_set_keymap_bulk(&self, _msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        Err(RynkError::Unimplemented)
    }
}
