use crate::bounds::PeerStatusBounds;
use crate::cmd::CommandLike;
use crate::id::{Head, LocalInstanceId, ReplicaId, SyncId};
use crate::log::Log;
use crate::peers::Peers;
use crate::store::LogStore;

use epkv_utils::vecset::VecSet;

use anyhow::Result;

pub struct State<C, L>
where
    C: CommandLike,
    L: LogStore<C>,
{
    pub peers: Peers,

    pub log: Log<C, L>,

    pub peer_status_bounds: PeerStatusBounds,

    pub lid_head: Head<LocalInstanceId>,

    pub sync_id_head: Head<SyncId>,
}

impl<C, L> State<C, L>
where
    C: CommandLike,
    L: LogStore<C>,
{
    pub async fn new(rid: ReplicaId, mut log_store: L, peers: VecSet<ReplicaId>) -> Result<Self> {
        let peers = Peers::new(peers);

        let attr_bounds = log_store.load_attr_bounds().await?;
        let status_bounds = log_store.load_status_bounds().await?;

        let lid_head =
            Head::new(attr_bounds.max_lids.get(&rid).copied().unwrap_or(LocalInstanceId::ZERO));

        let sync_id_head = Head::new(SyncId::ZERO);

        let log = Log::new(log_store, attr_bounds, status_bounds);

        let peer_status_bounds = PeerStatusBounds::new();

        Ok(Self { peers, lid_head, sync_id_head, log, peer_status_bounds })
    }
}
