//! Behavior-config handlers (combo timeout, one-shot timeout, tap intervals).

use rmk_types::protocol::rynk::{BehaviorConfig, RynkError, RynkMessage};

use super::super::RynkService;

impl<'a> RynkService<'a> {
    pub(crate) async fn handle_get_behavior_config(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let cfg = BehaviorConfig {
            combo_timeout_ms: self.ctx.combo_timeout().as_millis() as u16,
            oneshot_timeout_ms: self.ctx.one_shot_timeout().as_millis() as u16,
            tap_interval_ms: self.ctx.tap_interval(),
            tap_capslock_interval_ms: self.ctx.tap_capslock_interval(),
        };
        msg.write_response(&cfg)
    }

    pub(crate) async fn handle_set_behavior_config(&self, msg: &mut RynkMessage<'_>) -> Result<(), RynkError> {
        let cfg = msg.request::<BehaviorConfig>()?;
        self.ctx.set_combo_timeout(cfg.combo_timeout_ms).await;
        self.ctx.set_one_shot_timeout(cfg.oneshot_timeout_ms).await;
        self.ctx.set_tap_interval(cfg.tap_interval_ms).await;
        self.ctx.set_tap_capslock_interval(cfg.tap_capslock_interval_ms).await;
        msg.write_response(&())
    }
}
