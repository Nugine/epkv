use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Status {
    PreAccepted = 1,
    Accepted = 2,
    Committed = 3,
    Issued = 5,
    Executed = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ExecStatus {
    Committed = 3,
    Issuing = 4,
    Issued = 5,
    Executed = 6,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ord() {
        let ss = [
            Status::PreAccepted,
            Status::Accepted,
            Status::Committed,
            Status::Issued,
            Status::Executed,
        ];

        for i in 0..ss.len() - 1 {
            for j in (i + 1)..ss.len() {
                assert!(ss[i] < ss[j]);
            }
        }

        let ss = [
            ExecStatus::Committed,
            ExecStatus::Issuing,
            ExecStatus::Issued,
            ExecStatus::Executed,
        ];

        for i in 0..ss.len() - 1 {
            for j in (i + 1)..ss.len() {
                assert!(ss[i] < ss[j]);
            }
        }
    }
}
