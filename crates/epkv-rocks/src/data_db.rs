use crate::cmd::{BatchedCommand, CommandKind, Del, Get, Set};

use epkv_epaxos::exec::ExecNotify;
use epkv_epaxos::id::InstanceId;
use epkv_epaxos::store::DataStore;
use epkv_utils::asc::Asc;
use tracing::debug;

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use camino::Utf8Path;
use rocksdb::DB;

pub struct DataDb {
    db: DB,
}

impl DataDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path)?;
        Ok(Arc::new(Self { db }))
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

    pub fn batched_execute(self: &Arc<Self>, cmd: BatchedCommand) -> Result<()> {
        for cmd in cmd.as_slice() {
            let cmd = cmd.as_ref();
            let kind = cmd.kind.clone();
            self.execute(kind)?;
            if let Some(ref n) = cmd.notify {
                n.notify_executed();
            }
        }
        Ok(())
    }
}

type IssueFuture = impl Future<Output = Result<()>> + Send + 'static;

impl DataStore<BatchedCommand> for Arc<DataDb> {
    type Future<'a> = IssueFuture;
    fn issue(&self, id: InstanceId, cmd: BatchedCommand, notify: Asc<ExecNotify>) -> Self::Future<'_> {
        let this = Arc::clone(self);
        let task = move || {
            let result = this.batched_execute(cmd);
            notify.notify_executed();
            debug!(?id, "cmd executed");
            result
        };
        async move { tokio::task::spawn_blocking(task).await.unwrap() }
    }
}
