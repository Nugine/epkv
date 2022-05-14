use crate::cmd::{BatchedCommand, CommandKind, Del, Get, Set};

use asc::Asc;
use epkv_epaxos::exec::ExecNotify;
use epkv_epaxos::id::InstanceId;
use epkv_epaxos::store::DataStore;
use epkv_utils::cast::NumericCast;
use epkv_utils::lock::with_mutex;

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use camino::Utf8Path;
use parking_lot::Mutex as SyncMutex;
use rocksdb::DB;
use tracing::debug;

pub struct DataDb {
    db: DB,
    metrics: SyncMutex<Metrics>,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    pub executed_single_cmd_count: u64,
    pub executed_batched_cmd_count: u64,
}

impl DataDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path)?;
        let metrics = SyncMutex::new(Metrics { executed_batched_cmd_count: 0, executed_single_cmd_count: 0 });
        Ok(Arc::new(Self { db, metrics }))
    }

    pub fn execute(self: &Arc<Self>, cmd: CommandKind) -> Result<()> {
        match cmd {
            CommandKind::Get(cmd) => self.execute_get(cmd),
            CommandKind::Set(cmd) => self.execute_set(cmd),
            CommandKind::Del(cmd) => self.execute_del(cmd),
            CommandKind::Nop(_) => Ok(()),
            CommandKind::Fence(_) => Ok(()),
        }
    }

    pub fn execute_get(self: &Arc<Self>, cmd: Get) -> Result<()> {
        let ans = self.db.get(cmd.key.as_ref())?.map(Bytes::from);
        if let Some(tx) = cmd.tx {
            let _ = tx.blocking_send(ans);
        }
        Ok(())
    }

    pub fn execute_set(self: &Arc<Self>, cmd: Set) -> Result<()> {
        self.db.put(cmd.key.as_ref(), cmd.value.as_ref())?;
        Ok(())
    }

    pub fn execute_del(self: &Arc<Self>, cmd: Del) -> Result<()> {
        self.db.delete(cmd.key.as_ref())?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(id = ?id))]
    pub fn batched_execute(self: &Arc<Self>, id: InstanceId, cmd: BatchedCommand) -> Result<()> {
        let single_cmd_count: u64 = cmd.as_slice().len().numeric_cast();
        for cmd in cmd.as_slice() {
            let kind = cmd.kind.clone();
            self.execute(kind)?;
            if let Some(ref n) = cmd.notify {
                n.notify_executed();
            }
        }
        with_mutex(&self.metrics, |m| {
            m.executed_single_cmd_count = m.executed_single_cmd_count.wrapping_add(single_cmd_count);
            m.executed_batched_cmd_count = m.executed_batched_cmd_count.wrapping_add(1)
        });
        debug!(?id, "cmd executed");
        Ok(())
    }

    pub fn metrics(&self) -> Metrics {
        with_mutex(&self.metrics, |m| m.clone())
    }
}

type IssueFuture = impl Future<Output = Result<()>> + Send + 'static;

impl DataStore<BatchedCommand> for DataDb {
    type Future<'a> = IssueFuture;
    fn issue<'a>(
        self: &'a Arc<Self>,
        id: InstanceId,
        cmd: BatchedCommand,
        notify: Asc<ExecNotify>,
    ) -> Self::Future<'a> {
        debug!(?id, "issuing cmd");
        let this = Arc::clone(self);
        let task = move || {
            let result = this.batched_execute(id, cmd);
            notify.notify_executed();
            result
        };
        async move { tokio::task::spawn_blocking(task).await.unwrap() }
    }
}
