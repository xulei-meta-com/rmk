//! Morse handlers.

use rmk_types::protocol::rynk::{RynkError, RynkMessage, SetMorseRequest};

use super::super::RynkService;

impl<'a> RynkService<'a> {
    pub(crate) async fn handle_get_morse(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let idx = msg.request::<u8>()?;
        let morse = self.ctx.get_morse(idx).ok_or(RynkError::Invalid)?;
        msg.write_response(&morse)
    }

    pub(crate) async fn handle_set_morse(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let r = msg.request::<SetMorseRequest>()?;
        if (r.index as usize) >= self.ctx.morses_len() {
            return Err(RynkError::Invalid);
        }
        self.ctx
            .update_morse(r.index, |m| {
                *m = r.config;
            })
            .await;
        msg.write_response(&())
    }

    #[cfg(feature = "bulk")]
    pub(crate) async fn handle_get_morse_bulk(&self, _msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        Err(RynkError::Unimplemented)
    }

    #[cfg(feature = "bulk")]
    pub(crate) async fn handle_set_morse_bulk(&self, _msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        Err(RynkError::Unimplemented)
    }
}
