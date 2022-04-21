use crate::key::Key;
use crate::value::Value;

use serde::{Deserialize, Serialize};

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
    pub key: Key,
    pub value: Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Set {
    pub key: Key,
    pub value: Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Del {
    pub key: Key,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Nop {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Fence {}
