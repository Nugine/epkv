//! <https://github.com/tikv/rust-rocksdb>

use crate::cmd::BatchedCommand;
use crate::error::RocksDbErrorExt;
use crate::log_key::InstanceFieldKey;

use epkv_epaxos::deps::Deps;
use epkv_epaxos::id::{Ballot, InstanceId, ReplicaId, Seq};
use epkv_epaxos::ins::Instance;
use epkv_epaxos::status::Status;
use epkv_epaxos::store::UpdateMode;

use epkv_utils::codec;
use epkv_utils::vecset::VecSet;

use std::io;
use std::ops::Not;
use std::sync::Arc;

use anyhow::{ensure, Result};
use bytemuck::{bytes_of, try_from_bytes};
use camino::Utf8Path;
use rocksdb::{SeekKey, Writable, WriteBatch, DB};
use serde::Serialize;
use tracing::debug;

pub struct LogDb {
    db: DB,
}

fn put_small_value<T: Serialize>(db: &impl Writable, key: &[u8], value: &T) -> Result<()> {
    let mut buf = [0u8; 64];
    let pos: usize = {
        let mut value_buf = io::Cursor::new(buf.as_mut_slice());
        codec::serialize_into(&mut value_buf, &value)?;
        value_buf.position().try_into().unwrap()
    };
    let value = &buf[..pos];
    db.put(key, value).cvt()?;
    Ok(())
}

impl LogDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path.as_ref()).cvt()?;
        Ok(Arc::new(Self { db }))
    }

    pub fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: &Instance<BatchedCommand>,
        mode: UpdateMode,
    ) -> Result<()> {
        let needs_save_cmd = match mode {
            UpdateMode::Full => true,
            UpdateMode::Partial => false,
        };

        let wb = WriteBatch::new();

        let mut log_key = InstanceFieldKey::new(id, 0);
        let mut value_buf = Vec::new();

        macro_rules! put_field {
            ($field: tt, $value: expr) => {{
                log_key.set_field(InstanceFieldKey::$field);
                value_buf.clear();
                codec::serialize_into(&mut value_buf, $value)?;
                wb.put(bytes_of(&log_key), &value_buf).cvt()?;
            }};
        }

        if needs_save_cmd {
            // cmd
            put_field!(FIELD_CMD, &ins.cmd);
        }

        // pbal
        put_field!(FIELD_PBAL, &ins.pbal);

        // status
        put_field!(FIELD_STATUS, &ins.status);

        // (seq, deps, abal, acc)
        put_field!(FIELD_OTHERS, &(ins.seq, &ins.deps, ins.abal, &ins.acc));

        self.db.write(&wb).cvt()?;

        Ok(())
    }

    pub fn load(self: &Arc<Self>, id: InstanceId) -> Result<Option<Instance<BatchedCommand>>> {
        // <https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration>
        // <https://github.com/facebook/rocksdb/wiki/Iterator>

        let mut iter = self.db.iter();

        let cmd: BatchedCommand = {
            let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_CMD);
            let is_valid = iter.seek(SeekKey::Key(bytes_of(&log_key))).cvt()?;
            if is_valid.not() {
                debug!(?id, "not found");
                return Ok(None);
            }

            let log_key: &InstanceFieldKey = match try_from_bytes(iter.key()) {
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
            codec::deserialize_owned(iter.value())?
        };

        macro_rules! next_field {
            ($field:tt) => {{
                ensure!(iter.next().cvt()?);
                let log_key: &InstanceFieldKey =
                    try_from_bytes(iter.key()).expect("invalid log key");
                assert_eq!(log_key.id(), id);
                assert_eq!(log_key.field(), InstanceFieldKey::$field);
                codec::deserialize_owned(iter.value())?
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
        put_small_value(&self.db, bytes_of(&log_key), &pbal)
    }

    pub fn load_pbal(self: &Arc<Self>, id: InstanceId) -> Result<Option<Ballot>> {
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_PBAL);
        let ans = match self.db.get(bytes_of(&log_key)).cvt()? {
            None => return Ok(None),
            Some(v) => v,
        };
        let pbal: Ballot = codec::deserialize_owned(&*ans)?;
        Ok(Some(pbal))
    }

    pub fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> Result<()> {
        let log_key = InstanceFieldKey::new(id, InstanceFieldKey::FIELD_STATUS);
        put_small_value(&self.db, bytes_of(&log_key), &status)
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

        let output_tuple: (Seq, Deps, Status, VecSet<ReplicaId>) =
            codec::deserialize_owned(&*bytes).unwrap();

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
