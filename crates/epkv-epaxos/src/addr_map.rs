use crate::id::ReplicaId;

use std::net::SocketAddr;

use fnv::FnvHashMap;
use ordered_vecmap::VecMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct AddrMap {
    map: VecMap<ReplicaId, SocketAddr>,
    rev: FnvHashMap<SocketAddr, ReplicaId>,
}

impl AddrMap {
    #[must_use]
    pub fn new() -> Self {
        Self { map: VecMap::new(), rev: FnvHashMap::default() }
    }

    pub fn update(&mut self, rid: ReplicaId, addr: SocketAddr) -> Option<ReplicaId> {
        if let Some(prev_addr) = self.map.insert(rid, addr) {
            self.rev.remove(&prev_addr);
        }
        let prev_rid = self.rev.insert(addr, rid);
        if let Some(prev_rid) = prev_rid {
            let _ = self.map.remove(&prev_rid);
        }
        prev_rid
    }

    pub fn remove(&mut self, rid: ReplicaId) {
        if let Some(addr) = self.map.remove(&rid) {
            self.rev.remove(&addr);
        }
    }

    #[must_use]
    pub fn map(&self) -> &VecMap<ReplicaId, SocketAddr> {
        &self.map
    }
}

impl Default for AddrMap {
    fn default() -> Self {
        Self::new()
    }
}
