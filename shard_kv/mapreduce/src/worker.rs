//! Async worker loop, cooperative cancellation, and attempt-scoped outputs.

use crate::master::rpc;
use crate::mrapps;
use crate::protocol::{Directive, Reply, Request, Stage, Task, TaskRef};
use crate::storage::{fetch_file, DataServer};
use crate::KeyValue;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeartbeatEnd {
    Cancel,
    Stop,
    Lost,
}

enum Completion {
    Map(Vec<String>),
    Reduce(String),
}

#[derive(Debug, thiserror::Error)]
enum TaskFailure {
    #[error("fatal task error: {0}")]
    Fatal(#[source] io::Error),
    #[error("retryable task error: {0}")]
    Retryable(#[source] io::Error),
}

impl TaskFailure {
    fn classify(error: io::Error) -> Self {
        if matches!(
            error.kind(),
            io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput | io::ErrorKind::NotFound
        ) {
            Self::Fatal(error)
        } else {
            Self::Retryable(error)
        }
    }

    fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }
}

pub async fn run_worker(app: &str, socket: impl AsRef<Path>) -> io::Result<()> {
    let socket = socket.as_ref().to_path_buf();
    let worker_id = format!("{}-{}", std::process::id(), nonce());
    let root = socket.parent().unwrap_or_else(|| Path::new("."));
    let data_root = root.join(format!("node-{worker_id}"));
    let data_socket = std::env::temp_dir().join(format!("mr-data-{worker_id}.sock"));
    let _data_server = DataServer::start(&data_socket, &data_root).await?;

    let mut connected = false;
    let mut heartbeat_ms = 2_000_u64;
    loop {
        let response = if !connected {
            match rpc(
                &socket,
                Request::Register {
                    worker_id: worker_id.clone(),
                    data_socket: data_socket.to_string_lossy().into_owned(),
                },
            )
            .await
            {
                Ok(Reply::Registered { lease_ms }) => {
                    let requested = std::env::var("MR_HEARTBEAT_MS")
                        .ok()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or((lease_ms / 5).max(1));
                    heartbeat_ms = requested.max(1).min((lease_ms / 3).max(1));
                    connected = true;
                }
                Ok(Reply::Stop) => return Ok(()),
                Ok(Reply::Error { message }) => return Err(io::Error::other(message)),
                Ok(other) => {
                    return Err(invalid_data(format!(
                        "unexpected registration reply: {other:?}"
                    )))
                }
                Err(err) if is_startup_error(&err) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(err) => return Err(err),
            }
            continue;
        } else {
            match rpc(
                &socket,
                Request::Poll {
                    worker_id: worker_id.clone(),
                },
            )
            .await
            {
                Ok(reply) => reply,
                Err(err) if is_closed_socket(&err) => return Ok(()),
                Err(_err) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        };

        match response {
            Reply::Task(task) => {
                run_assigned(&task, app, &socket, &worker_id, heartbeat_ms).await?
            }
            Reply::Wait => tokio::time::sleep(Duration::from_millis(40)).await,
            Reply::Stop => return Ok(()),
            Reply::Error { message } => return Err(io::Error::other(message)),
            other => return Err(invalid_data(format!("unexpected poll reply: {other:?}"))),
        }
    }
}

async fn run_assigned(
    task: &Task,
    app: &str,
    socket: &Path,
    worker: &str,
    heartbeat_ms: u64,
) -> io::Result<()> {
    let (stage, task_id, attempt) = task_key(task);
    match stage {
        Stage::Map => maybe_crash("before_map"),
        Stage::Reduce => maybe_crash("before_reduce"),
    }
    let task_ref = TaskRef {
        stage,
        task_id,
        attempt,
    };
    let cancellation = CancellationToken::new();
    let work_token = cancellation.clone();
    let socket_owned = socket.to_path_buf();
    let worker_owned = worker.to_owned();
    let app_owned = app.to_owned();
    let task_owned = task.clone();
    let work = async move {
        match task_owned {
            Task::Map {
                task_id,
                attempt,
                filename,
                data_dir,
                n_reduce,
            } => {
                let local = worker_local_dir(Path::new(&data_dir), &worker_owned);
                tokio::task::spawn_blocking(move || {
                    execute_map(
                        task_id,
                        attempt,
                        &filename,
                        &app_owned,
                        &local,
                        n_reduce,
                        &work_token,
                    )
                })
                .await
                .map_err(|err| io::Error::other(format!("map task panicked: {err}")))?
                .map(Completion::Map)
            }
            Task::Reduce {
                task_id,
                attempt,
                data_dir,
                n_maps,
            } => execute_reduce(
                task_id,
                attempt,
                &app_owned,
                Path::new(&data_dir),
                n_maps,
                &socket_owned,
                &worker_owned,
                &work_token,
            )
            .await
            .map(Completion::Reduce),
        }
    };
    tokio::pin!(work);
    let heartbeat = heartbeat_loop(
        socket.to_path_buf(),
        worker.to_owned(),
        task_ref.clone(),
        cancellation.clone(),
        heartbeat_ms,
    );
    tokio::pin!(heartbeat);
    let completion = tokio::select! {
        result = &mut work => result,
        ended = &mut heartbeat => {
            cancellation.cancel();
            let _ = work.await;
            match ended {
                HeartbeatEnd::Cancel => { let _ = rpc(socket, Request::Cancelled { worker_id: worker.to_owned(), task: task_ref }).await; return Ok(()); }
                HeartbeatEnd::Stop => return Ok(()),
                HeartbeatEnd::Lost => return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "lost master while task was active")),
            }
        }
    };
    let report = async {
        match completion {
            Ok(Completion::Map(outputs)) => {
                maybe_crash("after_map_write");
                match rpc(
                    socket,
                    Request::CompleteMap {
                        worker_id: worker.into(),
                        task_id,
                        attempt,
                        outputs,
                    },
                )
                .await?
                {
                    Reply::Complete { accepted: _ } | Reply::Ack { accepted: _ } => {
                        maybe_crash("after_map_commit")
                    }
                    Reply::Error { message } => return Err(io::Error::other(message)),
                    other => {
                        return Err(invalid_data(format!(
                            "unexpected CompleteMap reply: {other:?}"
                        )))
                    }
                }
            }
            Ok(Completion::Reduce(output)) => {
                maybe_crash("after_reduce_write");
                match rpc(
                    socket,
                    Request::CompleteReduce {
                        worker_id: worker.into(),
                        task_id,
                        attempt,
                        output,
                    },
                )
                .await?
                {
                    Reply::Complete { .. } => (),
                    Reply::Error { message } => {
                        eprintln!("reduce completion was not accepted: {message}")
                    }
                    other => {
                        return Err(invalid_data(format!(
                            "unexpected CompleteReduce reply: {other:?}"
                        )))
                    }
                }
            }
            Err(err) => {
                let err = TaskFailure::classify(err);
                let fatal = err.is_fatal();
                let _ = rpc(
                    socket,
                    Request::Fail {
                        worker_id: worker.into(),
                        task: task_ref.clone(),
                        fatal,
                        error: err.to_string(),
                    },
                )
                .await;
            }
        }
        Ok::<(), io::Error>(())
    };
    tokio::pin!(report);
    tokio::select! {
        result = &mut report => {
            cancellation.cancel();
            let _ = heartbeat.await;
            result
        }
        ended = &mut heartbeat => {
            cancellation.cancel();
            match ended {
                HeartbeatEnd::Cancel => { let _ = rpc(socket, Request::Cancelled { worker_id: worker.to_owned(), task: task_ref.clone() }).await; Ok(()) }
                HeartbeatEnd::Stop => Ok(()),
                HeartbeatEnd::Lost => Err(io::Error::new(io::ErrorKind::ConnectionAborted, "lost master while committing task")),
            }
        }
    }
}

async fn heartbeat_loop(
    socket: PathBuf,
    worker: String,
    task: TaskRef,
    token: CancellationToken,
    heartbeat_ms: u64,
) -> HeartbeatEnd {
    let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_ms.max(1)));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = token.cancelled() => return HeartbeatEnd::Lost,
            _ = interval.tick() => match rpc(&socket, Request::Heartbeat { worker_id: worker.clone(), active: Some(task.clone()) }).await {
                Ok(Reply::Heartbeat { directive: Directive::Continue }) => (),
                Ok(Reply::Heartbeat { directive: Directive::Cancel }) => return HeartbeatEnd::Cancel,
                Ok(Reply::Heartbeat { directive: Directive::Stop }) | Ok(Reply::Stop) => return HeartbeatEnd::Stop,
                Ok(Reply::Error { .. }) => return HeartbeatEnd::Cancel,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                Ok(other) => { eprintln!("unexpected heartbeat reply: {other:?}"); return HeartbeatEnd::Lost; }
            }
        }
    }
}

fn task_key(task: &Task) -> (Stage, usize, u64) {
    match task {
        Task::Map {
            task_id, attempt, ..
        } => (Stage::Map, *task_id, *attempt),
        Task::Reduce {
            task_id, attempt, ..
        } => (Stage::Reduce, *task_id, *attempt),
    }
}

fn worker_local_dir(root: &Path, worker: &str) -> PathBuf {
    root.join(format!("node-{worker}"))
}

fn execute_map(
    task_id: usize,
    attempt: u64,
    filename: &str,
    app: &str,
    data_dir: &Path,
    n_reduce: usize,
    cancel: &CancellationToken,
) -> io::Result<Vec<String>> {
    if let Some(delay) = std::env::var("MR_TEST_MAP_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        let deadline = std::time::Instant::now() + Duration::from_millis(delay);
        while std::time::Instant::now() < deadline {
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    mrapps::validate_app(app).map_err(invalid_data)?;
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let input = std::fs::File::open(filename)?;
    let mut records = Vec::new();
    let mut unique_index = std::collections::BTreeSet::<(String, String)>::new();
    use std::io::BufRead;
    for line in std::io::BufReader::new(input).lines() {
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        let line = line?;
        let mapped = mrapps::map(app, filename, &line).map_err(invalid_data)?;
        if app == "indexer" {
            for record in mapped {
                unique_index.insert((record.key, record.value));
            }
        } else {
            records.extend(mapped);
        }
    }
    if app == "indexer" {
        records.extend(
            unique_index
                .into_iter()
                .map(|(key, value)| KeyValue::new(key, value)),
        );
    }
    let mut partitions = vec![Vec::<KeyValue>::new(); n_reduce];
    for record in records {
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        record.key.hash(&mut hash);
        partitions[(hash.finish() as usize) % n_reduce].push(record);
    }
    std::fs::create_dir_all(data_dir)?;
    let mut names = Vec::with_capacity(n_reduce);
    for (partition, records) in partitions.into_iter().enumerate() {
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        let name = format!("map-{task_id}-attempt-{attempt}-reduce-{partition}.jsonl");
        let path = data_dir.join(&name);
        let temporary = data_dir.join(format!(".{name}.tmp"));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let mut writer = std::io::BufWriter::new(file);
        for record in records {
            if cancel.is_cancelled() {
                let _ = std::fs::remove_file(&temporary);
                return Err(cancelled());
            }
            serde_json::to_writer(&mut writer, &record).map_err(invalid_data)?;
            use std::io::Write;
            writer.write_all(b"\n")?;
        }
        use std::io::Write;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        std::fs::rename(&temporary, &path)?;
        names.push(name);
        if partition == 0 {
            maybe_crash("map_after_first_partition");
        }
    }
    Ok(names)
}

async fn execute_reduce(
    task_id: usize,
    attempt: u64,
    app: &str,
    data_dir: &Path,
    n_maps: usize,
    master: &Path,
    worker: &str,
    cancel: &CancellationToken,
) -> io::Result<String> {
    mrapps::validate_app(app).map_err(invalid_data)?;
    let mut known = HashMap::<usize, u64>::new();
    let mut records = Vec::<KeyValue>::new();
    loop {
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        let response = rpc(
            master,
            Request::FetchReduceInputs {
                worker_id: worker.to_owned(),
                task_id,
                attempt,
                known: known.iter().map(|(id, version)| (*id, *version)).collect(),
            },
        )
        .await?;
        let Reply::ReduceInputs {
            outputs,
            all_maps_done,
        } = response
        else {
            return match response {
                Reply::Error { message } => {
                    Err(io::Error::new(io::ErrorKind::Interrupted, message))
                }
                other => Err(invalid_data(format!(
                    "unexpected reduce-input reply: {other:?}"
                ))),
            };
        };
        for output in outputs {
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            let fetched = tokio::select! {
                result = fetch_file(&output.data_socket, &output.filename) => result,
                _ = cancel.cancelled() => return Err(cancelled()),
            };
            match fetched {
                Ok(bytes) => {
                    for line in bytes.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
                        if cancel.is_cancelled() {
                            return Err(cancelled());
                        }
                        records.push(serde_json::from_slice(line).map_err(invalid_data)?);
                    }
                    known.insert(output.map_id, output.version);
                    maybe_crash("reduce_after_first_pull");
                }
                Err(err) => {
                    let _ = rpc(
                        master,
                        Request::MapOutputFailure {
                            worker_id: worker.to_owned(),
                            task_id,
                            attempt,
                            map_id: output.map_id,
                            source_worker: output.source_worker,
                            version: output.version,
                            error: err.to_string(),
                        },
                    )
                    .await;
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        format!("map output unavailable: {err}"),
                    ));
                }
            }
        }
        if all_maps_done && known.len() == n_maps {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let output = tokio::task::spawn_blocking({
        let app = app.to_owned();
        let cancel = cancel.clone();
        move || {
            records.sort_by(|a, b| a.key.cmp(&b.key));
            let mut grouped = BTreeMap::<String, Vec<String>>::new();
            for record in records {
                if cancel.is_cancelled() {
                    return Err(cancelled());
                }
                grouped.entry(record.key).or_default().push(record.value);
            }
            let mut result = String::new();
            for (key, values) in grouped {
                if cancel.is_cancelled() {
                    return Err(cancelled());
                }
                let value = mrapps::reduce(&app, &key, &values).map_err(invalid_data)?;
                result.push_str(&key);
                result.push(' ');
                result.push_str(&value);
                result.push('\n');
            }
            Ok::<_, io::Error>(result)
        }
    })
    .await
    .map_err(|err| io::Error::other(format!("reduce task panicked: {err}")))??;
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let local_dir = worker_local_dir(data_dir, worker);
    tokio::fs::create_dir_all(&local_dir).await?;
    let name = format!("reduce-{task_id}-attempt-{attempt}.out");
    tokio::fs::write(local_dir.join(&name), output).await?;
    Ok(name)
}

fn maybe_crash(point: &str) {
    if std::env::var("MR_CRASH_POINT").ok().as_deref() == Some(point) {
        std::process::exit(86);
    }
}
fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
fn cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "task cancelled")
}
fn invalid_data(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}
fn is_startup_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
    )
}
fn is_closed_socket(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
    )
}
