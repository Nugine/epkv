use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum InstanceStatus {
    PreAccepted = 1,
    Accepted = 2,
    Committed = 3,
    Issued = 4,
    Executed = 5,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ord() {
        let ss = [
            InstanceStatus::PreAccepted,
            InstanceStatus::Accepted,
            InstanceStatus::Committed,
            InstanceStatus::Issued,
            InstanceStatus::Executed,
        ];

        for i in 0..ss.len() - 1 {
            for j in (i + 1)..ss.len() {
                assert!(ss[i] < ss[j]);
            }
        }
    }
}