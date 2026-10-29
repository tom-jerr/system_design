//! Versioned JSON-lines control RPCs and binary data-transfer metadata.

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Map,
    Reduce,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskRef {
    pub stage: Stage,
    pub task_id: usize,
    pub attempt: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Envelope<T> {
    pub version: u16,
    pub request_id: String,
    pub body: T,
}

impl<T> Envelope<T> {
    pub fn new(request_id: impl Into<String>, body: T) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            request_id: request_id.into(),
            body,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "message", rename_all = "snake_case")]
pub enum Request {
    Register {
        worker_id: String,
        data_socket: String,
    },
    Poll {
        worker_id: String,
    },
    Heartbeat {
        worker_id: String,
        active: Option<TaskRef>,
    },
    FetchReduceInputs {
        worker_id: String,
        task_id: usize,
        attempt: u64,
        known: Vec<(usize, u64)>,
    },
    CompleteMap {
        worker_id: String,
        task_id: usize,
        attempt: u64,
        outputs: Vec<String>,
    },
    CompleteReduce {
        worker_id: String,
        task_id: usize,
        attempt: u64,
        output: String,
    },
    MapOutputFailure {
        worker_id: String,
        task_id: usize,
        attempt: u64,
        map_id: usize,
        source_worker: String,
        version: u64,
        error: String,
    },
    Fail {
        worker_id: String,
        task: TaskRef,
        fatal: bool,
        error: String,
    },
    Cancelled {
        worker_id: String,
        task: TaskRef,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "reply", rename_all = "snake_case")]
pub enum Reply {
    Registered {
        lease_ms: u64,
    },
    Task(Task),
    ReduceInputs {
        outputs: Vec<MapOutput>,
        all_maps_done: bool,
    },
    Wait,
    Stop,
    Heartbeat {
        directive: Directive,
    },
    Complete {
        accepted: bool,
    },
    CommitReduce {
        data_socket: String,
    },
    Ack {
        accepted: bool,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Directive {
    Continue,
    Cancel,
    Stop,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "task", rename_all = "snake_case")]
pub enum Task {
    Map {
        task_id: usize,
        attempt: u64,
        filename: String,
        data_dir: String,
        n_reduce: usize,
    },
    Reduce {
        task_id: usize,
        attempt: u64,
        data_dir: String,
        n_maps: usize,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MapOutput {
    pub map_id: usize,
    pub version: u64,
    pub source_worker: String,
    pub data_socket: String,
    pub filename: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataRequest {
    pub filename: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataReply {
    pub length: Option<u64>,
    pub error: Option<String>,
}
