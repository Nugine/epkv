use crate::id::ReplicaId;

use epkv_utils::iter::copied_map_collect;

use std::ops::Not;
use std::time::Duration;

use ordered_vecmap::VecSet;
use rand::prelude::SliceRandom;
use tracing::debug;

pub struct Peers {
    peers: VecSet<ReplicaId>,
    rank: Vec<(u64, ReplicaId)>,
    avg: Avg,
}

#[derive(Debug)]
pub struct SelectedPeers {
    pub acc: VecSet<ReplicaId>,
    pub others: VecSet<ReplicaId>,
}

impl SelectedPeers {
    #[must_use]
    pub fn into_merged(self) -> VecSet<ReplicaId> {
        let mut peers = self.acc;
        peers.union_copied_inplace(&self.others);
        peers
    }

    #[must_use]
    pub fn to_merged(&self) -> VecSet<ReplicaId> {
        self.acc.union_copied(&self.others)
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
        let is_new_peer = self.peers.insert(peer).is_none();
        if is_new_peer {
            self.rank.push((u64::MAX, peer))
        }
    }

    pub fn remove(&mut self, peer: ReplicaId) {
        if self.peers.remove(&peer).is_some() {
            if let Some(idx) = self.rank.iter().position(|&(_, r)| r != peer) {
                let (rtt, _) = self.rank.remove(idx);
                self.avg.sub(rtt);
            }
        }
    }

    pub fn set_rtt(&mut self, peer: ReplicaId, rtt: Duration) {
        let rtt = rtt.as_nanos().try_into().unwrap_or(u64::MAX);
        if let Some((rk, _)) = self.rank.iter_mut().find(|&&mut (_, r)| r == peer) {
            self.avg.sub(*rk);
            self.avg.add(rtt);
            *rk = rtt;
            sort_rank(&mut self.rank)
        }
    }

    pub fn set_inf_rtt(&mut self, peers: &VecSet<ReplicaId>) {
        let mut is_changed = false;
        for &mut (ref mut rk, rid) in &mut self.rank {
            if peers.contains(&rid) {
                self.avg.sub(*rk);
                *rk = u64::MAX;
                is_changed = true;
            }
        }
        if is_changed {
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
        debug!(?quorum, rank=?self.rank, "select peers");

        let acc = acc.intersection_copied(&self.peers);

        let ans_acc = if acc.len() <= quorum {
            acc
        } else {
            let rng = &mut rand::thread_rng();
            let iter = acc.as_slice().choose_multiple(rng, quorum);
            iter.copied().collect()
        };

        let mut ans_others = VecSet::new();
        if ans_acc.len() < quorum {
            let need = quorum.wrapping_sub(ans_acc.len());
            for &(_, rid) in self.rank.iter() {
                if ans_others.len() >= need {
                    break;
                }
                if ans_acc.contains(&rid).not() {
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
