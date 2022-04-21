use epkv_utils::stepper::Stepper;

pub struct CommandNotify(Stepper);

impl CommandNotify {
    const COMMITTED: u8 = 1;
    const EXECUTED: u8 = 2;

    #[must_use]
    pub fn new() -> Self {
        Self(Stepper::new())
    }

    pub fn notify_committed(&self) {
        self.0.set_state(Self::COMMITTED);
    }
    pub fn notify_executed(&self) {
        self.0.set_state(Self::EXECUTED);
    }

    pub async fn wait_committed(&self) {
        self.0.wait_state(Self::COMMITTED).await;
    }
    pub async fn wait_executed(&self) {
        self.0.wait_state(Self::EXECUTED).await;
    }
}

impl Default for CommandNotify {
    fn default() -> Self {
        Self::new()
    }
}
