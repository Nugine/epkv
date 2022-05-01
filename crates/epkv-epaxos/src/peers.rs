use crate::id::ReplicaId;

use epkv_utils::iter::copied_map_collect;
use epkv_utils::vecset::VecSet;

use std::ops::Not;
use std::time::Duration;

use rand::prelude::SliceRandom;

pub struct Peers {
    peers: VecSet<ReplicaId>,
    rank: Vec<(u64, ReplicaId)>,
    avg: Avg,
}

pub struct SelectedPeers {
    pub acc: VecSet<ReplicaId>,
    pub others: VecSet<ReplicaId>,
}

impl SelectedPeers {
    #[must_use]
    pub fn into_merged(self) -> VecSet<ReplicaId> {
        let mut peers = self.acc;
        peers.union_copied(&self.others);
        peers
    }
}

struct Avg {
    sum: u64,
    cnt: u64,
}

impl Avg {
    fn add(&mut self, rtt: u64) {
        if rtt != u64::MAX {
            self.sum = self.sum.saturating_add(rtt);
            self.cnt = self.cnt.wrapping_add(1);
        }
    }

    fn sub(&mut self, rtt: u64) {
        if rtt != u64::MAX {
            self.sum = self.sum.saturating_sub(rtt);
            self.cnt = self.cnt.wrapping_sub(1);
        }
    }
    fn get(&self) -> Option<u64> {
        self.sum.checked_div(self.cnt)
    }
}

fn sort_rank(rank: &mut [(u64, ReplicaId)]) {
    rank.sort_unstable_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
}

impl Peers {
    #[must_use]
    pub fn new(peers: VecSet<ReplicaId>) -> Self {
        let mut rank: Vec<_> = copied_map_collect(peers.iter(), |peer| (u64::MAX, peer));
        sort_rank(&mut rank);
        Self { peers, rank, avg: Avg { sum: 0, cnt: 0 } }
    }

    #[must_use]
    pub fn cluster_size(&self) -> usize {
        self.peers.len().wrapping_add(1).max(3)
    }

    pub fn add(&mut self, peer: ReplicaId) {
        let is_new_peer = self.peers.insert(peer).is_some();
        if is_new_peer {
            self.rank.push((u64::MAX, peer))
        }
    }

    pub fn remove(&mut self, peer: ReplicaId) {
        if self.peers.remove(&peer).is_some() {
            if let Some(idx) = self.rank.iter().position(|&(_, r)| r != peer) {
                let (rtt, _) = self.rank.remove(idx);
                self.avg.add(rtt);
            }
        }
    }

    pub fn set_rtt(&mut self, peer: ReplicaId, rtt: Duration) {
        let rtt = rtt.as_nanos().try_into().unwrap_or(u64::MAX);
        if let Some(pair) = self.rank.iter_mut().find(|&&mut (_, r)| r == peer) {
            self.avg.sub(pair.0);
            self.avg.add(rtt);
            pair.0 = rtt;
            sort_rank(&mut self.rank)
        }
    }

    pub fn get_avg_rtt(&self) -> Option<Duration> {
        self.avg.get().map(Duration::from_nanos)
    }

    #[must_use]
    pub fn select_all(&self) -> VecSet<ReplicaId> {
        self.peers.clone()
    }

    #[must_use]
    pub fn select(&self, quorum: usize, acc: &VecSet<ReplicaId>) -> SelectedPeers {
        let ans_acc = if acc.len() <= quorum {
            acc.clone()
        } else {
            let rng = &mut rand::thread_rng();
            let iter: _ = acc.as_slice().choose_multiple(rng, quorum);
            iter.copied().collect()
        };

        let mut ans_others = VecSet::new();
        if acc.len() < quorum {
            let need = quorum.wrapping_sub(acc.len());
            for &(_, rid) in self.rank.iter() {
                if ans_others.len() >= need {
                    break;
                }
                if acc.contains(&rid).not() {
                    let _ = ans_others.insert(rid);
                }
            }
        };

        SelectedPeers { acc: ans_acc, others: ans_others }
    }

    #[must_use]
    pub fn select_one(&self) -> Option<ReplicaId> {
        Some(self.rank.first()?.1)
    }
}
