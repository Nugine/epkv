use epkv_epaxos::acc::Acc;
use epkv_epaxos::deps::Deps;
use epkv_epaxos::id::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Round, Seq};
use epkv_epaxos::ins::Instance;
use epkv_epaxos::status::Status;
use epkv_epaxos::store::UpdateMode;
use epkv_rocks::cmd::{BatchedCommand, CommandKind, Get, MutableCommand};
use epkv_rocks::log_db::LogDb;
use epkv_utils::clone;
use epkv_utils::tracing::setup_tracing;

use std::{env, fs};

use anyhow::Result;
use camino::Utf8Path;

#[test]
fn log_db() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "epkv_rocks=debug")
    }
    setup_tracing();

    let path = Utf8Path::new("/tmp/epkv-rocks/tests/log_db");
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }

    let log_db = LogDb::new(path)?;

    {
        let id = InstanceId(1.into(), 1.into());
        let ins = log_db.load(id)?;
        assert!(ins.is_none());
    }

    let id = InstanceId(1.into(), 1.into());
    let ins = {
        let pbal = Ballot(Round::ZERO, 1.into());
        let cmd = BatchedCommand::from_vec(vec![MutableCommand {
            kind: CommandKind::Get(Get { key: "hello".into(), tx: None }),
            notify: None,
        }]);
        let seq = Seq::from(2);
        let deps = Deps::from_iter([(ReplicaId::from(2), LocalInstanceId::from(1))]);
        let abal = pbal;
        let status = Status::PreAccepted;
        let acc = Acc::from_iter([ReplicaId::from(1)]);

        Instance { pbal, cmd, seq, deps, abal, status, acc }
    };

    {
        clone!(ins);
        log_db.save(id, ins, UpdateMode::Full)?;
    }

    {
        let ans = log_db.load(id)?.unwrap();
        assert_eq!(ans.pbal, ins.pbal);
        assert_eq!(ans.seq, ins.seq);
        assert_eq!(ans.deps, ins.deps);
        assert_eq!(ans.abal, ins.abal);
        assert_eq!(ans.status, ins.status);
        assert_eq!(ans.acc, ins.acc);

        for (i, ins_cmd) in ins.cmd.as_slice().iter().enumerate() {
            let ans_cmd = &ans.cmd.as_slice()[i];
            assert!(compare_cmd(ans_cmd, ins_cmd));
        }
    }

    Ok(())
}

fn compare_cmd(lhs: &MutableCommand, rhs: &MutableCommand) -> bool {
    match lhs.kind {
        CommandKind::Get(ref lhs) => match rhs.kind {
            CommandKind::Get(ref rhs) => lhs.key == rhs.key,
            _ => false,
        },
        CommandKind::Set(ref lhs) => match rhs.kind {
            CommandKind::Set(ref rhs) => lhs.key == rhs.key && lhs.value == rhs.value,
            _ => false,
        },
        CommandKind::Del(ref lhs) => match rhs.kind {
            CommandKind::Del(ref rhs) => lhs.key == rhs.key,
            _ => false,
        },
        CommandKind::Nop(_) => matches!(rhs.kind, CommandKind::Nop(_)),
        CommandKind::Fence(_) => matches!(rhs.kind, CommandKind::Fence(_)),
    }
}
