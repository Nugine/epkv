use crate::types::{Epoch, ReplicaId};

use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::ops::Not;

use rand::prelude::SliceRandom;

type Rank = u64;

pub struct Meta {
    epoch: Epoch,
    peers: VecMap<ReplicaId, Rank>,
    rank: Vec<(Rank, ReplicaId)>,
}

pub struct SelectedPeers {
    pub acc: VecSet<ReplicaId>,
    pub others: VecSet<ReplicaId>,
}

fn sort_rank(rank: &mut [(Rank, ReplicaId)]) {
    rank.sort_unstable_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
}

fn copied_map_collect<'a, C, T, U>(iter: impl Iterator<Item = &'a T>, f: impl FnMut(T) -> U) -> C
where
    T: Copy + 'a,
    C: FromIterator<U>,
{
    iter.copied().map(f).collect()
}

impl Meta {
    #[must_use]
    pub fn new(epoch: Epoch, peers: &VecSet<ReplicaId>) -> Self {
        let map: VecMap<_, _> = copied_map_collect(peers.iter(), |peer| (peer, Rank::MAX));
        let mut rank: Vec<_> = copied_map_collect(peers.iter(), |peer| (Rank::MAX, peer));
        sort_rank(&mut rank);

        Self {
            epoch,
            peers: map,
            rank,
        }
    }

    #[must_use]
    pub fn cluster_size(&self) -> usize {
        self.peers.len().wrapping_add(1).max(3)
    }

    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn update_epoch(&mut self, epoch: Epoch) {
        max_assign(&mut self.epoch, epoch);
    }

    pub fn add_peer(&mut self, peer: ReplicaId) {
        let (is_new_peer, _) = self.peers.init_with(&peer, || (peer, Rank::MAX));
        if is_new_peer {
            self.rank.push((Rank::MAX, peer))
        }
    }

    pub fn remove_peer(&mut self, peer: ReplicaId) {
        if self.peers.remove(&peer).is_some() {
            self.rank.retain(|&(_, r)| r != peer)
        }
    }

    #[must_use]
    pub fn all_peers(&self) -> VecSet<ReplicaId> {
        self.peers.iter().map(|&(r, _)| r).collect()
    }

    #[must_use]
    pub fn select_peers(&self, quorum: usize, acc: &VecSet<ReplicaId>) -> SelectedPeers {
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

        SelectedPeers {
            acc: ans_acc,
            others: ans_others,
        }
    }
}
