//! <https://github.com/tikv/rust-rocksdb>

use crate::cmd::BatchedCommand;
use crate::db_utils::{get_value, put_small_value, put_value};
use crate::log_key::{GlobalFieldKey, InstanceFieldKey};

use epkv_epaxos::bounds::{AttrBounds, StatusBounds, StatusMap};
use epkv_epaxos::deps::Deps;
use epkv_epaxos::id::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use epkv_epaxos::ins::Instance;
use epkv_epaxos::status::Status;
use epkv_epaxos::store::UpdateMode;

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
use serde::{Deserialize, Serialize};
use tracing::debug;

pub struct LogDb {
    db: DB,
}

#[derive(Default, Deserialize, Serialize)]
pub struct SavedStatusBounds {
    pub known_up_to: VecMap<ReplicaId, LocalInstanceId>,
    pub committed_up_to: VecMap<ReplicaId, LocalInstanceId>,
    pub executed_up_to: VecMap<ReplicaId, LocalInstanceId>,
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

        if needs_save_cmd {
            log_key.set_field(InstanceFieldKey::FIELD_CMD);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.cmd)?;
        }

        // pbal
        {
            log_key.set_field(InstanceFieldKey::FIELD_PBAL);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.pbal)?;
        }

        // status
        {
            log_key.set_field(InstanceFieldKey::FIELD_STATUS);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &ins.status)?;
        }

        // (seq, deps, abal, acc)
        {
            log_key.set_field(InstanceFieldKey::FIELD_OTHERS);
            let value: _ = (ins.seq, &ins.deps, ins.abal, &ins.acc);
            put_value(&mut wb, bytes_of(&log_key), &mut buf, &value)?;
        }

        self.db.write(wb)?;

        Ok(())
    }

    pub fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<BatchedCommand>>> {
        // <https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration>
        // <https://github.com/facebook/rocksdb/wiki/Iterator>

        let mut iter = self.db.raw_iterator();

        let cmd: BatchedCommand = {
            let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_CMD);

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
            if log_key.field() != InstanceFieldKey::FIELD_CMD {
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

        let pbal: Ballot = next_field!(FIELD_PBAL);
        let status: Status = next_field!(FIELD_STATUS);
        let others: (Seq, Deps, Ballot, VecSet<ReplicaId>) = next_field!(FIELD_OTHERS);
        let (seq, deps, abal, acc) = others;

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

        let (attr_bounds, saved_status_bounds) = self.load_bounds_optional(&mut iter)?.unwrap_or_else(|| {
            let attr_bounds = AttrBounds { max_seq: Seq::ZERO, max_lids: VecMap::new() };
            let saved_status_bounds = SavedStatusBounds::default();
            (attr_bounds, saved_status_bounds)
        });

        let status_bounds = {
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
            let zero_id = InstanceId(ReplicaId::ONE, LocalInstanceId::ZERO);
            let mut log_key = InstanceFieldKey::new(zero_id, field_status);

            // loop {
            //     iter.seek(bytes_of(&log_key));
            //     if iter.valid().not() {
            //         break;
            //     }

            //     log_key = match try_from_bytes(iter.key().unwrap()) {
            //         Ok(k) => *k,
            //         Err(_) => break,
            //     };

            //     let id = log_key.id();
            //     assert!(id.0 > ReplicaId::ZERO);
            //     assert!(id.1 > LocalInstanceId::ZERO);

            //     assert_eq!(log_key.field(), InstanceFieldKey::FIELD_CMD);

            //     log_key.set_field(field_status);

            //     iter.seek(bytes_of(&log_key));
            //     if iter.valid().not() {
            //         break;
            //     }

            //     log_key = *from_bytes(iter.key().unwrap());
            //     assert_eq!(log_key.id(), id);
            //     assert_eq!(log_key.field(), field_status);

            //     // TODO
            // }

            // iter.status()?;
        }

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use epkv_epaxos::deps::{Deps, MutableDeps};
    use epkv_epaxos::id::{Ballot, InstanceId, ReplicaId, Round, Seq};
    use epkv_epaxos::status::Status;

    use epkv_utils::codec;
    use epkv_utils::vecset::VecSet;

    use std::io;

    #[test]
    fn tuple_ref_serde() {
        let seq = Seq::from(1);

        let deps = {
            let mut deps = MutableDeps::with_capacity(1);
            deps.insert(InstanceId(2022.into(), 422.into()));
            Deps::from_mutable(deps)
        };

        let status = Status::Committed;
        let acc = VecSet::from_single(ReplicaId::from(2022));

        let input_tuple = &(seq, &deps, status, &acc);

        let bytes = codec::serialize(input_tuple).unwrap();

        let output_tuple: (Seq, Deps, Status, VecSet<ReplicaId>) = codec::deserialize_owned(&*bytes).unwrap();

        assert_eq!(input_tuple.0, output_tuple.0);
        assert_eq!(*input_tuple.1, output_tuple.1);
        assert_eq!(input_tuple.2, output_tuple.2);
        assert_eq!(*input_tuple.3, output_tuple.3);
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
