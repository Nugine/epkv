use crate::status::Status;

use epkv_utils::stepper::Stepper;

pub struct ExecNotify(Stepper);

impl ExecNotify {
    const ISSUED: u8 = 1;
    const EXECUTED: u8 = 2;

    #[must_use]
    pub fn new() -> Self {
        Self(Stepper::new())
    }

    pub fn notify_issued(&self) {
        self.0.set_state(Self::ISSUED);
    }
    pub fn notify_executed(&self) {
        self.0.set_state(Self::EXECUTED);
    }

    pub async fn wait_issued(&self) {
        self.0.wait_state(Self::ISSUED).await;
    }
    pub async fn wait_executed(&self) {
        self.0.wait_state(Self::EXECUTED).await;
    }

    pub fn status(&self) -> Status {
        let state = self.0.state();
        if state >= Self::EXECUTED {
            Status::Executed
        } else if state >= Self::ISSUED {
            Status::Issued
        } else {
            Status::Committed
        }
    }
}

impl Default for ExecNotify {
    fn default() -> Self {
        Self::new()
    }
}
