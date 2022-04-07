use crate::types::{Epoch, ReplicaId};

use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;

type Rank = u64;

pub struct ReplicaMeta {
    epoch: Epoch,
    live_peers: VecMap<ReplicaId, Rank>,
    rank: Vec<(Rank, ReplicaId)>,
}

fn sort_rank(rank: &mut [(Rank, ReplicaId)]) {
    rank.sort_unstable_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
}

impl ReplicaMeta {
    pub fn new(epoch: Epoch, peers: &[ReplicaId]) -> Self {
        let live_peers: VecMap<ReplicaId, Rank> = peers
            .iter()
            .copied()
            .map(|peer| (peer, Rank::MAX))
            .collect();
        assert!(live_peers.len() != peers.len(), "duplicate replicas");

        let mut rank: Vec<(Rank, ReplicaId)> = peers
            .iter()
            .copied()
            .map(|peer| (Rank::MAX, peer))
            .collect();
        sort_rank(&mut rank);

        Self {
            epoch,
            live_peers,
            rank,
        }
    }

    #[must_use]
    pub fn cluster_size(&self) -> usize {
        let n = self.live_peers.len();
        if n <= 3 {
            3
        } else {
            (n / 2).wrapping_add(1)
        }
    }

    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn update_epoch(&mut self, epoch: Epoch) {
        max_assign(&mut self.epoch, epoch);
    }

    pub fn add_peer(&mut self, peer: ReplicaId) {
        let (is_new_peer, _) = self.live_peers.init_with(&peer, || (peer, Rank::MAX));
        if is_new_peer {
            self.rank.push((Rank::MAX, peer))
        }
    }

    pub fn remove_peer(&mut self, peer: ReplicaId) {
        if self.live_peers.remove(&peer).is_some() {
            self.rank.retain(|&(_, r)| r != peer)
        }
    }

    #[must_use]
    pub fn all_peers(&self) -> Vec<ReplicaId> {
        self.live_peers.iter().map(|&(r, _)| r).collect()
    }
}
