//! <https://github.com/tikv/rust-rocksdb>

use crate::cmd::BatchedCommand;
use crate::error::RocksDbErrorExt;
use crate::log_key::InstanceFieldKey;

use epkv_epaxos::id::InstanceId;
use epkv_epaxos::ins::Instance;
use epkv_epaxos::store::UpdateMode;
use epkv_utils::codec;

use std::sync::Arc;

use anyhow::Result;
use bytemuck::bytes_of;
use camino::Utf8Path;
use rocksdb::{Writable, WriteBatch, DB};

pub struct LogDb {
    db: DB,
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
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use epkv_epaxos::deps::{Deps, MutableDeps};
    use epkv_epaxos::id::{InstanceId, ReplicaId, Seq};
    use epkv_epaxos::status::Status;
    use epkv_utils::codec;
    use epkv_utils::vecset::VecSet;

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
}
