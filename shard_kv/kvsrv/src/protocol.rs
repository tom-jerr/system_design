//! Application-level RPC types. Transport errors are separate from these replies.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: String,
    pub version: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Error)]
pub enum KvError {
    #[error("key does not exist")]
    NoKey,
    #[error("version does not match")]
    Version,
    #[error("the operation may have committed before the reply was lost")]
    Maybe,
    #[error("the operation may have committed; subsequent failure: {0}")]
    MaybeWithCause(Box<KvError>),
    #[error("lock lease is no longer valid")]
    LeaseLost,
    #[error("key version is exhausted")]
    VersionExhausted,
    #[error("lock fencing token is exhausted")]
    FenceExhausted,
    #[error("lease duration is invalid or cannot be represented")]
    InvalidLease,
    #[error("server state was poisoned")]
    StatePoisoned,
    #[error("permanent transport or codec failure: {0}")]
    Transport(String),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum AcquireReply {
    Acquired { token: u64, lease_ms: u64 },
    Busy { retry_after_ms: u64 },
}

#[tarpc::service]
pub trait KvRpc {
    async fn get(key: String) -> Result<VersionedValue, KvError>;
    async fn put(key: String, value: String, version: u64) -> Result<u64, KvError>;
    async fn put_fenced(
        key: String,
        value: String,
        version: u64,
        lock: String,
        token: u64,
    ) -> Result<u64, KvError>;
    async fn try_acquire(lock: String, acquisition_id: String) -> Result<AcquireReply, KvError>;
    async fn renew(lock: String, acquisition_id: String, token: u64) -> Result<(), KvError>;
    async fn release(lock: String, acquisition_id: String, token: u64) -> Result<(), KvError>;
}
