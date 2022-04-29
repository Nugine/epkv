use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Clone, Serialize, Deserialize)]
pub enum CommandKind {
    Get(Get),
    Set(Set),
    Del(Del),
    Nop(Nop),
    Fence(Fence),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Get {
    pub key: Bytes,
    #[serde(skip)]
    pub tx: Option<mpsc::Sender<Option<Bytes>>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Set {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Del {
    pub key: Bytes,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Nop {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Fence {}
