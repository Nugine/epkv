#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

pub mod bench;
pub mod client;
pub mod cluster;

// -------------------------------

use epkv_protocol::rpc::RpcClientConfig;
use epkv_utils::utf8;

use anyhow::Result;
use bytes::Bytes;
use rand::RngCore;
use serde::Serialize;

fn default_rpc_client_config() -> RpcClientConfig {
    RpcClientConfig {
        max_frame_length: 16777216, // 16 MiB
        op_chan_size: 65536,
        forward_chan_size: 65536,
    }
}

fn random_bytes(size: usize) -> Bytes {
    let mut buf: Vec<u8> = vec![0; size];
    rand::thread_rng().fill_bytes(&mut buf);
    Bytes::from(buf)
}

fn pretty_json<T: Serialize>(value: &T) -> Result<String> {
    let mut buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent("    ".as_ref());
    let mut serializer = serde_json::Serializer::with_formatter(&mut buf, formatter.clone());
    value.serialize(&mut serializer)?;
    let ans = utf8::vec_to_string(buf)?;
    Ok(ans)
}
