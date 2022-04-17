use crate::bounds::PeerStatusBounds;
use crate::cmd::CommandLike;
use crate::id::{Head, LocalInstanceId, ReplicaId, SyncId};
use crate::log::Log;
use crate::peers::Peers;
use crate::store::LogStore;

use epkv_utils::vecset::VecSet;

use anyhow::Result;

pub struct State<C, S>
where
    C: CommandLike,
    S: LogStore<C>,
{
    pub peers: Peers,

    pub log: Log<C, S>,

    pub peer_status_bounds: PeerStatusBounds,

    pub lid_head: Head<LocalInstanceId>,

    pub sync_id_head: Head<SyncId>,
}

impl<C, S> State<C, S>
where
    C: CommandLike,
    S: LogStore<C>,
{
    pub async fn new(rid: ReplicaId, mut store: S, peers: VecSet<ReplicaId>) -> Result<Self> {
        let peers = Peers::new(peers);

        let attr_bounds = store.load_attr_bounds().await?;
        let status_bounds = store.load_status_bounds().await?;

        let lid_head =
            Head::new(attr_bounds.max_lids.get(&rid).copied().unwrap_or(LocalInstanceId::ZERO));

        let sync_id_head = Head::new(SyncId::ZERO);

        let log = Log::new(store, attr_bounds, status_bounds);

        let peer_status_bounds = PeerStatusBounds::new();

        Ok(Self { peers, lid_head, sync_id_head, log, peer_status_bounds })
    }
}
