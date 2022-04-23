//! <https://github.com/tikv/rust-rocksdb>

use crate::cmd::BatchedCommand;
use crate::db_utils::{get_value, put_small_value, put_value};
use crate::log_key::{GlobalFieldKey, InstanceFieldKey};

use async_trait::async_trait;
use epkv_epaxos::bounds::{AttrBounds, SavedStatusBounds, StatusBounds, StatusMap};
use epkv_epaxos::deps::Deps;
use epkv_epaxos::id::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use epkv_epaxos::ins::Instance;
use epkv_epaxos::status::Status;
use epkv_epaxos::store::{LogStore, UpdateMode};

use epkv_utils::cmp::max_assign;
use epkv_utils::codec;
use epkv_utils::onemap::OneMap;
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::ops::Not;
use std::sync::Arc;

use anyhow::{ensure, Result};
use bytemuck::bytes_of;
use bytemuck::checked::{from_bytes, try_from_bytes};
use camino::Utf8Path;
use rocksdb::{DBRawIterator, WriteBatch, DB};
use tracing::debug;

pub struct LogDb {
    db: DB,
}

impl LogDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path)?;
        Ok(Arc::new(Self { db }))
    }

    pub fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<BatchedCommand>,
        mode: UpdateMode,
    ) -> Result<()> {
        let needs_save_cmd = match mode {
            UpdateMode::Full => true,
            UpdateMode::Partial => false,
        };

        let mut wb = WriteBatch::default();

        let mut log_key = InstanceFieldKey::new(id, 0);
        let mut buf = Vec::new();

        // status
        {
            log_key.set_field(InstanceFieldKey::FIELD_STATUS);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.status)?;
        }

        // seq
        {
            log_key.set_field(InstanceFieldKey::FIELD_SEQ);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.seq)?;
        }

        // pbal
        {
            log_key.set_field(InstanceFieldKey::FIELD_PBAL);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.pbal)?;
        }

        if needs_save_cmd {
            log_key.set_field(InstanceFieldKey::FIELD_CMD);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.cmd)?;
        }

        // (deps, abal, acc)
        {
            log_key.set_field(InstanceFieldKey::FIELD_OTHERS);
            let value: _ = (&ins.deps, ins.abal, &ins.acc);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &value)?;
        }

        self.db.write(wb)?;

        Ok(())
    }

    pub fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<BatchedCommand>>> {
        // <https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration>
        // <https://github.com/facebook/rocksdb/wiki/Iterator>

        let mut iter = self.db.raw_iterator();

        let status: Status = {
            let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_STATUS);

            iter.seek(bytes_of(&log_key));

            if iter.valid().not() {
                iter.status()?;
                debug!(?id, "not found");
                return Ok(None);
            }

            let log_key: &InstanceFieldKey = match try_from_bytes(iter.key().unwrap()) {
                Ok(k) => k,
                Err(_) => {
                    debug!(?id, iter_key = ?iter.key());
                    return Ok(None);
                }
            };

            if log_key.id() != id {
                return Ok(None);
            }
            if log_key.field() != InstanceFieldKey::FIELD_STATUS {
                return Ok(None);
            }

            codec::deserialize_owned(iter.value().unwrap())?
        };

        macro_rules! next_field {
            ($field:tt) => {{
                iter.next();
                iter.status()?;
                ensure!(iter.valid());
                let log_key: &InstanceFieldKey = from_bytes(iter.key().unwrap());
                assert_eq!(log_key.id(), id);
                assert_eq!(log_key.field(), InstanceFieldKey::$field);
                codec::deserialize_owned(iter.value().unwrap())?
            }};
        }

        let seq: Seq = next_field!(FIELD_SEQ);
        let pbal: Ballot = next_field!(FIELD_PBAL);
        let cmd: BatchedCommand = next_field!(FIELD_CMD);
        let others: (Deps, Ballot, VecSet<ReplicaId>) = next_field!(FIELD_OTHERS);
        let (deps, abal, acc) = others;

        let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };

        Ok(Some(ins))
    }

    pub fn save_pbal(self: &Arc<Self>, id: InstanceId, pbal: Ballot) -> Result<()> {
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_PBAL);
        put_small_value(&mut &self.db, bytes_of(&log_key), &pbal)
    }

    pub fn load_pbal(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>> {
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_PBAL);
        get_value(&self.db, bytes_of(&log_key))
    }

    pub fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()> {
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_STATUS);
        put_small_value(&mut &self.db, bytes_of(&log_key), &status)
    }

    pub fn save_bounds(self: &Arc<Self>, attr: AttrBounds, status: SavedStatusBounds) -> Result<()> {
        let mut buf = Vec::new();
        let mut wb = WriteBatch::default();
        {
            let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_ATTR_BOUNDS);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &attr)?;
        }
        {
            let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_STATUS_BOUNDS);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &status)?;
        }
        self.db.write(wb)?;
        Ok(())
    }

    fn load_bounds_optional(
        &self,
        iter: &mut DBRawIterator<'_>,
    ) -> Result<Option<(AttrBounds, SavedStatusBounds)>> {
        let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_ATTR_BOUNDS);
        iter.seek(bytes_of(&log_key));

        if iter.valid().not() {
            iter.status()?;
            return Ok(None);
        }

        if bytes_of(&log_key) != iter.key().unwrap() {
            return Ok(None);
        }

        let attr_bounds: AttrBounds = codec::deserialize_owned(iter.value().unwrap())?;

        let log_key = GlobalFieldKey::new(GlobalFieldKey::FIELD_STATUS_BOUNDS);
        iter.seek(bytes_of(&log_key));
        iter.status()?;
        ensure!(iter.valid());
        assert_eq!(bytes_of(&log_key), iter.key().unwrap());
        let saved_status_bounds: SavedStatusBounds = codec::deserialize_owned(iter.value().unwrap())?;

        Ok(Some((attr_bounds, saved_status_bounds)))
    }

    pub fn load_bounds(self: &Arc<Self>) -> Result<(AttrBounds, StatusBounds)> {
        let mut iter = self.db.raw_iterator();

        let (mut attr_bounds, saved_status_bounds) =
            self.load_bounds_optional(&mut iter)?.unwrap_or_else(|| {
                let attr_bounds = AttrBounds { max_seq: Seq::ZERO, max_lids: VecMap::new() };
                let saved_status_bounds = SavedStatusBounds::default();
                (attr_bounds, saved_status_bounds)
            });

        let mut status_bounds = {
            let mut maps: VecMap<ReplicaId, StatusMap> = VecMap::new();

            let create_default = || StatusMap {
                known: OneMap::new(0),
                committed: OneMap::new(0),
                executed: OneMap::new(0),
            };

            let mut merge: _ = |map: &VecMap<ReplicaId, LocalInstanceId>,
                                project: fn(&mut StatusMap) -> &mut OneMap| {
                for &(rid, lid) in map {
                    let (_, m) = maps.init_with(rid, create_default);
                    ((project)(m)).set_bound(lid.raw_value());
                }
            };

            merge(&saved_status_bounds.known_up_to, |m| &mut m.known);
            merge(&saved_status_bounds.committed_up_to, |m| &mut m.committed);
            merge(&saved_status_bounds.executed_up_to, |m| &mut m.executed);

            StatusBounds::from_maps(maps)
        };

        {
            let field_status = InstanceFieldKey::FIELD_STATUS;

            let mut rid = ReplicaId::ONE;
            let mut lid = match saved_status_bounds.executed_up_to.get(&rid) {
                Some(jump_to) => jump_to.add_one(),
                None => LocalInstanceId::ONE,
            };

            loop {
                let log_key = InstanceFieldKey::new(InstanceId(rid, lid), field_status);

                iter.seek(bytes_of(&log_key));
                if iter.valid().not() {
                    break;
                }

                let log_key: &InstanceFieldKey = match try_from_bytes(iter.key().unwrap()) {
                    Ok(k) => k,
                    Err(_) => break,
                };

                let id = log_key.id();
                let is_rid_changed = rid != id.0;
                InstanceId(rid, lid) = id;

                if is_rid_changed {
                    if let Some(&jump_to) = saved_status_bounds.executed_up_to.get(&id.0) {
                        if lid < jump_to {
                            lid = jump_to.add_one();
                            continue;
                        }
                    }
                }

                if log_key.field() != field_status {
                    lid = lid.add_one();
                    continue;
                }

                let status: Status = codec::deserialize_owned(iter.value().unwrap())?;

                iter.next();
                if iter.valid().not() {
                    break;
                }

                let seq: Seq = codec::deserialize_owned(iter.value().unwrap())?;

                max_assign(&mut attr_bounds.max_seq, seq);
                attr_bounds.max_lids.update(rid, |l| max_assign(l, lid), || lid);
                status_bounds.set(InstanceId(rid, lid), status);

                lid = lid.add_one();
            }

            iter.status()?;
        }

        status_bounds.update_bounds();

        Ok((attr_bounds, status_bounds))
    }
}

#[async_trait]
impl LogStore<BatchedCommand> for Arc<LogDb> {
    async fn save(&mut self, id: InstanceId, ins: Instance<BatchedCommand>, mode: UpdateMode) -> Result<()> {
        let this = Arc::clone(self);
        let task = move || LogDb::save(&this, id, ins, mode);
        tokio::task::spawn_blocking(task).await.unwrap()
    }

    async fn load(&mut self, id: InstanceId) -> Result<Option<Instance<BatchedCommand>>> {
        let this = Arc::clone(self);
        let task = move || LogDb::load(&this, id);
        tokio::task::spawn_blocking(task).await.unwrap()
    }

    async fn load_pbal(&mut self, id: InstanceId) -> Result<Option<Ballot>> {
        let this = Arc::clone(self);
        let task = move || LogDb::load_pbal(&this, id);
        tokio::task::spawn_blocking(task).await.unwrap()
    }

    async fn save_pbal(&mut self, id: InstanceId, pbal: Ballot) -> Result<()> {
        let this = Arc::clone(self);
        let task = move || LogDb::save_pbal(&this, id, pbal);
        tokio::task::spawn_blocking(task).await.unwrap()
    }

    async fn load_bounds(&mut self) -> Result<(AttrBounds, StatusBounds)> {
        let this = Arc::clone(self);
        let task = move || LogDb::load_bounds(&this);
        tokio::task::spawn_blocking(task).await.unwrap()
    }

    async fn save_bounds(&mut self, attr_bounds: AttrBounds, status_bounds: SavedStatusBounds) -> Result<()> {
        let this = Arc::clone(self);
        let task = move || LogDb::save_bounds(&this, attr_bounds, status_bounds);
        tokio::task::spawn_blocking(task).await.unwrap()
    }

    async fn update_status(&mut self, id: InstanceId, status: Status) -> Result<()> {
        let this = Arc::clone(self);
        let task = move || LogDb::update_status(&this, id, status);
        tokio::task::spawn_blocking(task).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use epkv_epaxos::deps::{Deps, MutableDeps};
    use epkv_epaxos::id::{Ballot, InstanceId, ReplicaId, Round};
    use epkv_epaxos::status::Status;

    use epkv_utils::codec;
    use epkv_utils::vecset::VecSet;

    use std::io;

    #[test]
    fn tuple_ref_serde() {
        let deps = {
            let mut deps = MutableDeps::with_capacity(1);
            deps.insert(InstanceId(2022.into(), 422.into()));
            Deps::from_mutable(deps)
        };

        let status = Status::Committed;
        let acc = VecSet::from_single(ReplicaId::from(2022));

        let input_tuple = &(&deps, status, &acc);

        let bytes = codec::serialize(input_tuple).unwrap();

        let output_tuple: (Deps, Status, VecSet<ReplicaId>) = codec::deserialize_owned(&*bytes).unwrap();

        assert_eq!(input_tuple.0, &output_tuple.0);
        assert_eq!(input_tuple.1, output_tuple.1);
        assert_eq!(input_tuple.2, &output_tuple.2);
    }

    #[test]
    fn cursor_serde() {
        let input_pbal = Ballot(Round::ONE, ReplicaId::ONE);

        let mut buf = [0u8; 64];
        let pos: usize = {
            let mut value_buf = io::Cursor::new(buf.as_mut_slice());
            codec::serialize_into(&mut value_buf, &input_pbal).unwrap();
            value_buf.position().try_into().unwrap()
        };
        let value = &buf[..pos];

        let output_pbal: Ballot = codec::deserialize_owned(value).unwrap();

        assert_eq!(input_pbal, output_pbal);
    }
}
