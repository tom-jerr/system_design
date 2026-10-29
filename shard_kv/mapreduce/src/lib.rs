//! A small asynchronous MapReduce runtime for local multi-process clusters.

pub mod master;
pub mod mrapps;
pub mod protocol;
pub mod storage;
pub mod worker;

/// One intermediate MapReduce record.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

impl KeyValue {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}
