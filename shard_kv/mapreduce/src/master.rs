//! Async master, serialized state machine, task leases, and UDS control RPC.

use crate::protocol::{
    Directive, Envelope, MapOutput, Reply, Request, Stage, Task, TaskRef, PROTOCOL_VERSION,
};
use crate::storage::{
    fetch_file, is_plain_filename, read_json_line, write_json_line, write_staging_file,
};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::BufReader;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const MAX_ATTEMPTS: u8 = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReducePhase {
    Shuffling,
    Reducing,
    Committing,
}

#[derive(Clone)]
enum TaskStatus {
    Pending,
    Running {
        worker: String,
        attempt: u64,
        deadline: Instant,
        phase: ReducePhase,
    },
    Committed {
        worker: String,
        attempt: u64,
    },
    Failed,
}

struct TaskState {
    status: TaskStatus,
    attempts: u8,
}
impl TaskState {
    fn new() -> Self {
        Self {
            status: TaskStatus::Pending,
            attempts: 0,
        }
    }
}

struct WorkerState {
    data_socket: PathBuf,
    last_seen: Instant,
    alive: bool,
}
struct Manifest {
    owner: String,
    version: u64,
    files: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JobStatus {
    Running,
    Succeeded,
    Failed,
}

struct State {
    data_dir: PathBuf,
    inputs: Vec<String>,
    n_reduce: usize,
    lease: Duration,
    maps: Vec<TaskState>,
    reduces: Vec<TaskState>,
    manifests: Vec<Option<Manifest>>,
    versions: Vec<u64>,
    workers: HashMap<String, WorkerState>,
    status: JobStatus,
    failure: Option<String>,
}

impl State {
    fn new(data_dir: PathBuf, inputs: Vec<String>, n_reduce: usize, lease: Duration) -> Self {
        let n_maps = inputs.len();
        Self {
            data_dir,
            inputs,
            n_reduce,
            lease,
            maps: (0..n_maps).map(|_| TaskState::new()).collect(),
            reduces: (0..n_reduce).map(|_| TaskState::new()).collect(),
            manifests: (0..n_maps).map(|_| None).collect(),
            versions: vec![0; n_maps],
            workers: HashMap::new(),
            status: JobStatus::Running,
            failure: None,
        }
    }

    fn reclaim_expired(&mut self, now: Instant) {
        if self.status != JobStatus::Running {
            return;
        }
        let dead: Vec<_> = self
            .workers
            .iter()
            .filter(|(_, w)| w.alive && now.duration_since(w.last_seen) >= self.lease)
            .map(|(id, _)| id.clone())
            .collect();
        for worker in dead {
            self.mark_worker_down(&worker);
        }

        for task in &mut self.maps {
            let expired =
                matches!(&task.status, TaskStatus::Running { deadline, .. } if *deadline <= now);
            if expired {
                retry(task);
            }
        }
        for task in &mut self.reduces {
            let expired =
                matches!(&task.status, TaskStatus::Running { deadline, .. } if *deadline <= now);
            if expired {
                retry(task);
            }
        }
        if self
            .maps
            .iter()
            .chain(&self.reduces)
            .any(|t| matches!(t.status, TaskStatus::Failed))
        {
            self.fail_job("task exhausted its retry limit".into());
        }
    }

    fn register(&mut self, worker: String, data_socket: String) -> Reply {
        if self.status != JobStatus::Running {
            return self.terminal_reply();
        }
        if let Some(existing) = self.workers.get_mut(&worker) {
            if !existing.alive || existing.data_socket != Path::new(&data_socket) {
                return Reply::Error {
                    message: "worker ID is already retired or bound to another socket".into(),
                };
            }
            existing.last_seen = Instant::now();
        } else {
            self.workers.insert(
                worker,
                WorkerState {
                    data_socket: data_socket.into(),
                    last_seen: Instant::now(),
                    alive: true,
                },
            );
        }
        Reply::Registered {
            lease_ms: self.lease.as_millis().min(u64::MAX as u128) as u64,
        }
    }

    fn handle(&mut self, request: Request) -> Reply {
        self.reclaim_expired(Instant::now());
        match request {
            Request::Register {
                worker_id,
                data_socket,
            } => self.register(worker_id, data_socket),
            Request::Poll { worker_id } => self.poll(&worker_id),
            Request::Heartbeat { worker_id, active } => self.heartbeat(&worker_id, active),
            Request::FetchReduceInputs {
                worker_id,
                task_id,
                attempt,
                known,
            } => self.fetch_inputs(&worker_id, task_id, attempt, &known),
            Request::CompleteMap {
                worker_id,
                task_id,
                attempt,
                outputs,
            } => self.complete_map(&worker_id, task_id, attempt, outputs),
            Request::CompleteReduce {
                worker_id,
                task_id,
                attempt,
                ..
            } => self.prepare_reduce_commit(&worker_id, task_id, attempt),
            Request::MapOutputFailure {
                worker_id,
                task_id,
                attempt,
                map_id,
                source_worker,
                version,
                error,
            } => self.map_output_failure(
                &worker_id,
                task_id,
                attempt,
                map_id,
                &source_worker,
                version,
                &error,
            ),
            Request::Fail {
                worker_id,
                task,
                fatal,
                error,
            } => self.task_failed(&worker_id, task, fatal, error),
            Request::Cancelled { worker_id, task } => Reply::Ack {
                accepted: self.owns(&worker_id, &task),
            },
        }
    }

    fn poll(&mut self, worker: &str) -> Reply {
        if self.status != JobStatus::Running {
            return self.terminal_reply();
        }
        if !self.touch_worker(worker) {
            return Reply::Stop;
        }
        if let Some((id, task)) = self
            .maps
            .iter_mut()
            .enumerate()
            .find(|(_, t)| matches!(t.status, TaskStatus::Pending))
        {
            let attempt = assign(task, worker, self.lease, ReducePhase::Shuffling);
            return Reply::Task(Task::Map {
                task_id: id,
                attempt,
                filename: self.inputs[id].clone(),
                data_dir: self.data_dir.to_string_lossy().into_owned(),
                n_reduce: self.n_reduce,
            });
        }
        let maps_started = self
            .maps
            .iter()
            .all(|t| !matches!(t.status, TaskStatus::Pending));
        if maps_started {
            if let Some((id, task)) = self
                .reduces
                .iter_mut()
                .enumerate()
                .find(|(_, t)| matches!(t.status, TaskStatus::Pending))
            {
                let attempt = assign(task, worker, self.lease, ReducePhase::Shuffling);
                return Reply::Task(Task::Reduce {
                    task_id: id,
                    attempt,
                    data_dir: self.data_dir.to_string_lossy().into_owned(),
                    n_maps: self.maps.len(),
                });
            }
        }
        Reply::Wait
    }

    fn heartbeat(&mut self, worker: &str, active: Option<TaskRef>) -> Reply {
        if self.status != JobStatus::Running {
            return Reply::Heartbeat {
                directive: Directive::Stop,
            };
        }
        if !self.touch_worker(worker) {
            return Reply::Heartbeat {
                directive: Directive::Stop,
            };
        }
        let Some(active) = active else {
            return Reply::Heartbeat {
                directive: Directive::Continue,
            };
        };
        let lease = self.lease;
        let tasks = self.tasks_mut(active.stage);
        let Some(task) = tasks.get_mut(active.task_id) else {
            return Reply::Heartbeat {
                directive: Directive::Cancel,
            };
        };
        match &mut task.status {
            TaskStatus::Running {
                worker: owner,
                attempt,
                deadline,
                ..
            } if owner == worker && *attempt == active.attempt && *deadline > Instant::now() => {
                *deadline = Instant::now() + lease;
                Reply::Heartbeat {
                    directive: Directive::Continue,
                }
            }
            TaskStatus::Committed {
                worker: owner,
                attempt,
            } if owner == worker && *attempt == active.attempt => Reply::Heartbeat {
                directive: Directive::Continue,
            },
            _ => Reply::Heartbeat {
                directive: Directive::Cancel,
            },
        }
    }

    fn fetch_inputs(
        &mut self,
        worker: &str,
        task_id: usize,
        attempt: u64,
        known: &[(usize, u64)],
    ) -> Reply {
        if self.status != JobStatus::Running {
            return self.terminal_reply();
        }
        if !self.touch_worker(worker)
            || !self.owns(
                worker,
                &TaskRef {
                    stage: Stage::Reduce,
                    task_id,
                    attempt,
                },
            )
        {
            return Reply::Error {
                message: "reduce attempt is no longer owned".into(),
            };
        }
        let outputs: Vec<MapOutput> = self
            .manifests
            .iter()
            .enumerate()
            .filter_map(|(map_id, manifest)| {
                let manifest = manifest.as_ref()?;
                if known.contains(&(map_id, manifest.version)) {
                    return None;
                }
                let owner = self.workers.get(&manifest.owner)?;
                if !owner.alive {
                    return None;
                }
                Some(MapOutput {
                    map_id,
                    version: manifest.version,
                    source_worker: manifest.owner.clone(),
                    data_socket: owner.data_socket.to_string_lossy().into_owned(),
                    filename: manifest.files.get(task_id)?.clone(),
                })
            })
            .collect();
        let all_maps_done = self
            .maps
            .iter()
            .all(|t| matches!(t.status, TaskStatus::Committed { .. }));
        if all_maps_done && known.len() + outputs.len() == self.maps.len() {
            if let Some(TaskState {
                status: TaskStatus::Running { phase, .. },
                ..
            }) = self.reduces.get_mut(task_id)
            {
                *phase = ReducePhase::Reducing;
            }
        }
        Reply::ReduceInputs {
            outputs,
            all_maps_done,
        }
    }

    fn complete_map(
        &mut self,
        worker: &str,
        task_id: usize,
        attempt: u64,
        outputs: Vec<String>,
    ) -> Reply {
        if self.status != JobStatus::Running
            || !self.touch_worker(worker)
            || outputs.len() != self.n_reduce
            || outputs.iter().any(|f| !is_plain_filename(Path::new(f)))
        {
            return Reply::Complete { accepted: false };
        }
        let Some(_task) = self.maps.get(task_id) else {
            return Reply::Complete { accepted: false };
        };
        if !self.owns(
            worker,
            &TaskRef {
                stage: Stage::Map,
                task_id,
                attempt,
            },
        ) {
            return Reply::Complete { accepted: false };
        }
        let local = worker_local_dir(&self.data_dir, worker);
        if outputs.iter().any(|f| !local.join(f).is_file()) {
            return Reply::Complete { accepted: false };
        }
        self.versions[task_id] += 1;
        self.manifests[task_id] = Some(Manifest {
            owner: worker.to_owned(),
            version: self.versions[task_id],
            files: outputs,
        });
        self.maps[task_id].status = TaskStatus::Committed {
            worker: worker.to_owned(),
            attempt,
        };
        self.check_success();
        Reply::Complete { accepted: true }
    }

    fn prepare_reduce_commit(&mut self, worker: &str, task_id: usize, attempt: u64) -> Reply {
        if self.status != JobStatus::Running
            || !self.touch_worker(worker)
            || !self.owns(
                worker,
                &TaskRef {
                    stage: Stage::Reduce,
                    task_id,
                    attempt,
                },
            )
        {
            return Reply::Complete { accepted: false };
        }
        let Some(socket) = self
            .workers
            .get(worker)
            .filter(|w| w.alive)
            .map(|w| w.data_socket.to_string_lossy().into_owned())
        else {
            return Reply::Complete { accepted: false };
        };
        if let Some(TaskState {
            status: TaskStatus::Running {
                phase, deadline, ..
            },
            ..
        }) = self.reduces.get_mut(task_id)
        {
            if *phase == ReducePhase::Committing {
                return Reply::Complete { accepted: false };
            }
            *phase = ReducePhase::Committing;
            *deadline = Instant::now() + self.lease;
        }
        Reply::CommitReduce {
            data_socket: socket,
        }
    }

    async fn finish_reduce_commit(
        &mut self,
        worker: &str,
        task_id: usize,
        attempt: u64,
        staged: PathBuf,
    ) -> Reply {
        self.reclaim_expired(Instant::now());
        if self.status != JobStatus::Running
            || !self.owns(
                worker,
                &TaskRef {
                    stage: Stage::Reduce,
                    task_id,
                    attempt,
                },
            )
        {
            let _ = tokio::fs::remove_file(staged).await;
            return Reply::Complete { accepted: false };
        }
        let destination = self.data_dir.join(format!("mr-out-{task_id}"));
        let result = tokio::fs::rename(&staged, &destination).await;
        match result {
            Ok(()) => {
                self.reduces[task_id].status = TaskStatus::Committed {
                    worker: worker.to_owned(),
                    attempt,
                };
                self.check_success();
                Reply::Complete { accepted: true }
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(staged).await;
                Reply::Error {
                    message: format!("publish reduce output: {err}"),
                }
            }
        }
    }

    fn map_output_failure(
        &mut self,
        worker: &str,
        task_id: usize,
        attempt: u64,
        map_id: usize,
        source: &str,
        version: u64,
        error: &str,
    ) -> Reply {
        if !self.owns(
            worker,
            &TaskRef {
                stage: Stage::Reduce,
                task_id,
                attempt,
            },
        ) {
            return Reply::Ack { accepted: false };
        }
        let current = self
            .manifests
            .get(map_id)
            .and_then(Option::as_ref)
            .is_some_and(|m| m.owner == source && m.version == version);
        if current {
            eprintln!("map output unavailable: map {map_id}, worker {source}: {error}");
            self.mark_worker_down(source);
            Reply::Ack { accepted: true }
        } else {
            Reply::Ack { accepted: false }
        }
    }

    fn task_failed(&mut self, worker: &str, task: TaskRef, fatal: bool, error: String) -> Reply {
        if self.status != JobStatus::Running || !self.owns(worker, &task) {
            return Reply::Ack { accepted: false };
        }
        if fatal {
            self.fail_job(format!(
                "{:?} task {} failed: {error}",
                task.stage, task.task_id
            ));
        } else {
            retry(
                self.tasks_mut(task.stage)
                    .get_mut(task.task_id)
                    .expect("checked task"),
            );
            if task.stage == Stage::Map {
                self.revoke_reducers();
            }
            if self
                .tasks(&task)
                .get(task.task_id)
                .is_some_and(|t| matches!(t.status, TaskStatus::Failed))
            {
                self.fail_job("task exhausted its retry limit".into());
            }
        }
        Reply::Ack { accepted: true }
    }

    fn mark_worker_down(&mut self, worker: &str) {
        if let Some(state) = self.workers.get_mut(worker) {
            state.alive = false;
        }
        let mut maps_changed = false;
        for task in &mut self.maps {
            if matches!(&task.status, TaskStatus::Running { worker: owner, .. } if owner == worker)
            {
                retry(task);
                maps_changed = true;
            }
        }
        for task in &mut self.reduces {
            if matches!(&task.status, TaskStatus::Running { worker: owner, .. } if owner == worker)
            {
                retry(task);
            }
        }
        for id in 0..self.manifests.len() {
            if self.manifests[id]
                .as_ref()
                .is_some_and(|m| m.owner == worker)
            {
                self.manifests[id] = None;
                self.versions[id] += 1;
                retry(&mut self.maps[id]);
                maps_changed = true;
            }
        }
        if maps_changed {
            self.revoke_reducers();
        }
        if self
            .maps
            .iter()
            .chain(&self.reduces)
            .any(|t| matches!(t.status, TaskStatus::Failed))
        {
            self.fail_job("task exhausted its retry limit".into());
        }
    }

    fn revoke_reducers(&mut self) {
        for task in &mut self.reduces {
            if matches!(task.status, TaskStatus::Running { .. }) {
                retry(task);
            }
        }
    }

    fn touch_worker(&mut self, id: &str) -> bool {
        if let Some(w) = self.workers.get_mut(id) {
            if w.alive {
                w.last_seen = Instant::now();
                return true;
            }
        }
        false
    }
    fn tasks_mut(&mut self, stage: Stage) -> &mut Vec<TaskState> {
        match stage {
            Stage::Map => &mut self.maps,
            Stage::Reduce => &mut self.reduces,
        }
    }
    fn tasks(&self, task: &TaskRef) -> &Vec<TaskState> {
        match task.stage {
            Stage::Map => &self.maps,
            Stage::Reduce => &self.reduces,
        }
    }
    fn owns(&self, worker: &str, task: &TaskRef) -> bool {
        self.tasks(task).get(task.task_id).is_some_and(|t| {
            matches!(&t.status,
            TaskStatus::Running { worker: owner, attempt, deadline, .. }
            if owner == worker && *attempt == task.attempt && *deadline > Instant::now())
        })
    }
    fn check_success(&mut self) {
        if self
            .maps
            .iter()
            .all(|t| matches!(t.status, TaskStatus::Committed { .. }))
            && self
                .reduces
                .iter()
                .all(|t| matches!(t.status, TaskStatus::Committed { .. }))
        {
            self.status = JobStatus::Succeeded;
        }
    }
    fn fail_job(&mut self, reason: String) {
        if self.status == JobStatus::Running {
            self.status = JobStatus::Failed;
            self.failure = Some(reason);
        }
    }
    fn terminal_reply(&self) -> Reply {
        match self.status {
            JobStatus::Succeeded => Reply::Stop,
            JobStatus::Failed => Reply::Error {
                message: self.failure.clone().unwrap_or_else(|| "job failed".into()),
            },
            JobStatus::Running => Reply::Wait,
        }
    }
}

fn assign(task: &mut TaskState, worker: &str, lease: Duration, phase: ReducePhase) -> u64 {
    task.attempts += 1;
    let attempt = task.attempts as u64;
    task.status = TaskStatus::Running {
        worker: worker.to_owned(),
        attempt,
        deadline: Instant::now() + lease,
        phase,
    };
    attempt
}

fn retry(task: &mut TaskState) {
    if task.attempts >= MAX_ATTEMPTS {
        task.status = TaskStatus::Failed;
    } else {
        task.status = TaskStatus::Pending;
    }
}

fn worker_local_dir(root: &Path, worker: &str) -> PathBuf {
    root.join(format!("node-{worker}"))
}

enum Command {
    Rpc(Envelope<Request>, oneshot::Sender<Envelope<Reply>>),
    FinishReduce {
        worker: String,
        task_id: usize,
        attempt: u64,
        staged: PathBuf,
        reply: oneshot::Sender<Reply>,
    },
    Status(oneshot::Sender<(JobStatus, Option<String>)>),
}

async fn call_state(
    tx: &mpsc::Sender<Command>,
    request_id: String,
    request: Request,
) -> io::Result<Envelope<Reply>> {
    let (send, recv) = oneshot::channel();
    tx.send(Command::Rpc(
        Envelope {
            version: PROTOCOL_VERSION,
            request_id,
            body: request,
        },
        send,
    ))
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "master scheduler stopped"))?;
    recv.await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "master scheduler stopped"))
}

pub struct MasterServer {
    tx: mpsc::Sender<Command>,
    stop: CancellationToken,
    accept: Option<JoinHandle<()>>,
    scheduler: Option<JoinHandle<()>>,
    socket: PathBuf,
}

impl MasterServer {
    pub async fn start(
        socket: impl AsRef<Path>,
        inputs: Vec<String>,
        n_reduce: usize,
    ) -> io::Result<Self> {
        Self::start_with_lease(socket, inputs, n_reduce, Duration::from_secs(10)).await
    }

    pub async fn start_with_lease(
        socket: impl AsRef<Path>,
        inputs: Vec<String>,
        n_reduce: usize,
        lease: Duration,
    ) -> io::Result<Self> {
        if n_reduce == 0 || lease.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "n_reduce and lease must be positive",
            ));
        }
        let socket = if socket.as_ref().is_absolute() {
            socket.as_ref().to_path_buf()
        } else {
            std::env::current_dir()?.join(socket)
        };
        let root = socket
            .parent()
            .unwrap_or_else(|| Path::new("/"))
            .to_path_buf();
        tokio::fs::create_dir_all(&root).await?;
        if socket.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("socket path already exists: {}", socket.display()),
            ));
        }
        let listener = UnixListener::bind(&socket)?;
        let cwd = std::env::current_dir()?;
        let inputs = inputs
            .into_iter()
            .map(|p| {
                let p = PathBuf::from(p);
                if p.is_absolute() { p } else { cwd.join(p) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        let (tx, mut rx) = mpsc::channel::<Command>(256);
        let mut state = State::new(root, inputs, n_reduce, lease);
        let scheduler = tokio::spawn(async move {
            let mut sweep = tokio::time::interval((lease / 4).max(Duration::from_millis(10)));
            loop {
                tokio::select! {
                    _ = sweep.tick() => state.reclaim_expired(Instant::now()),
                    command = rx.recv() => match command {
                        Some(Command::Rpc(envelope, reply)) => {
                            let request_id = envelope.request_id;
                            let response = if envelope.version != PROTOCOL_VERSION { Reply::Error { message: format!("unsupported protocol version {}", envelope.version) } }
                                else { state.handle(envelope.body) };
                            let _ = reply.send(Envelope::new(request_id, response));
                        }
                        Some(Command::FinishReduce { worker, task_id, attempt, staged, reply }) => {
                            let result = state.finish_reduce_commit(&worker, task_id, attempt, staged).await;
                            let _ = reply.send(result);
                        }
                        Some(Command::Status(reply)) => { let _ = reply.send((state.status, state.failure.clone())); }
                        None => break,
                    }
                }
            }
        });
        let accept_tx = tx.clone();
        let stop = CancellationToken::new();
        let accept_stop = stop.clone();
        let accept = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_stop.cancelled() => break,
                    result = listener.accept() => match result {
                        Ok((stream, _)) => { let tx = accept_tx.clone(); tokio::spawn(async move { if let Err(err) = serve_one(stream, tx).await { eprintln!("master RPC failed: {err}"); } }); }
                        Err(err) => { eprintln!("master accept failed: {err}"); break; }
                    }
                }
            }
        });
        Ok(Self {
            tx,
            stop,
            accept: Some(accept),
            scheduler: Some(scheduler),
            socket,
        })
    }

    pub async fn wait(&self) -> io::Result<()> {
        loop {
            let (send, recv) = oneshot::channel();
            self.tx
                .send(Command::Status(send))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "scheduler stopped"))?;
            let (status, failure) = recv
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "scheduler stopped"))?;
            match status {
                JobStatus::Running => tokio::time::sleep(Duration::from_millis(50)).await,
                JobStatus::Succeeded => return Ok(()),
                JobStatus::Failed => {
                    return Err(io::Error::other(
                        failure.unwrap_or_else(|| "job failed".into()),
                    ))
                }
            }
        }
    }
}

impl Drop for MasterServer {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.accept.take() {
            task.abort();
        }
        if let Some(task) = self.scheduler.take() {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.socket);
    }
}

static RPC_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

async fn serve_one(stream: UnixStream, tx: mpsc::Sender<Command>) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let envelope: Envelope<Request> = read_json_line(&mut reader).await?;
    let request_id = envelope.request_id.clone();
    let is_reduce = match &envelope.body {
        Request::CompleteReduce {
            worker_id,
            task_id,
            attempt,
            output,
        } => Some((worker_id.clone(), *task_id, *attempt, output.clone())),
        _ => None,
    };
    let response = call_state(&tx, request_id, envelope.body).await?;
    let mut response = response;
    if let Some((worker, task_id, attempt, output)) = is_reduce {
        if let Reply::CommitReduce { data_socket } = &response.body {
            let final_reply = match fetch_file(data_socket, &output).await {
                Ok(bytes) => {
                    let staged = std::env::temp_dir().join(format!(
                        "mr-publish-{}-{task_id}-{attempt}",
                        std::process::id()
                    ));
                    if let Err(err) = write_staging_file(&staged, &bytes).await {
                        let _ = tokio::fs::remove_file(&staged).await;
                        Reply::Error {
                            message: format!("stage reduce output: {err}"),
                        }
                    } else {
                        let (send, recv) = oneshot::channel();
                        tx.send(Command::FinishReduce {
                            worker,
                            task_id,
                            attempt,
                            staged,
                            reply: send,
                        })
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::BrokenPipe, "scheduler stopped")
                        })?;
                        recv.await.map_err(|_| {
                            io::Error::new(io::ErrorKind::BrokenPipe, "scheduler stopped")
                        })?
                    }
                }
                Err(err) => Reply::Error {
                    message: format!("fetch reduce output: {err}"),
                },
            };
            response.body = final_reply;
        }
    }
    write_json_line(reader.get_mut(), &response).await
}

pub async fn rpc(socket: impl AsRef<Path>, request: Request) -> io::Result<Reply> {
    let mut stream = tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(socket))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "master connect timed out"))??;
    let id = RPC_ID
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .to_string();
    write_json_line(&mut stream, &Envelope::new(id.clone(), request)).await?;
    let mut reader = BufReader::new(stream);
    let response: Envelope<Reply> = read_json_line(&mut reader).await?;
    if response.version != PROTOCOL_VERSION || response.request_id != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RPC response envelope mismatch",
        ));
    }
    Ok(response.body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn lease_expiration_fences_old_attempt_and_requeues() {
        let dir = tempdir().unwrap();
        let mut state = State::new(
            dir.path().into(),
            vec!["input".into()],
            1,
            Duration::from_secs(1),
        );
        state.register("w1".into(), "/tmp/w1.sock".into());
        let task = state.poll("w1");
        let attempt = match task {
            Reply::Task(Task::Map { attempt, .. }) => attempt,
            other => panic!("{other:?}"),
        };
        if let TaskStatus::Running { deadline, .. } = &mut state.maps[0].status {
            *deadline = Instant::now() - Duration::from_millis(1);
        }
        state.reclaim_expired(Instant::now());
        assert!(matches!(
            state.heartbeat(
                "w1",
                Some(TaskRef {
                    stage: Stage::Map,
                    task_id: 0,
                    attempt
                })
            ),
            Reply::Heartbeat {
                directive: Directive::Cancel
            }
        ));
        state.register("w2".into(), "/tmp/w2.sock".into());
        assert!(
            matches!(state.poll("w2"), Reply::Task(Task::Map { attempt: next, .. }) if next > attempt)
        );
        assert!(!state.owns(
            "w1",
            &TaskRef {
                stage: Stage::Map,
                task_id: 0,
                attempt
            }
        ));
    }

    #[test]
    fn worker_down_invalidates_all_owned_manifests_and_reduces() {
        let dir = tempdir().unwrap();
        let mut state = State::new(
            dir.path().into(),
            vec!["a".into(), "b".into()],
            1,
            Duration::from_secs(5),
        );
        state.register("w1".into(), "/tmp/w1.sock".into());
        std::fs::create_dir_all(worker_local_dir(dir.path(), "w1")).unwrap();
        for id in 0..2 {
            let attempt = match state.poll("w1") {
                Reply::Task(Task::Map { attempt, .. }) => attempt,
                other => panic!("{other:?}"),
            };
            let file = format!("m{id}");
            std::fs::write(worker_local_dir(dir.path(), "w1").join(&file), b"x").unwrap();
            assert!(matches!(
                state.complete_map("w1", id, attempt, vec![file]),
                Reply::Complete { accepted: true }
            ));
        }
        state.mark_worker_down("w1");
        assert!(state.manifests.iter().all(Option::is_none));
        assert!(state
            .maps
            .iter()
            .all(|t| matches!(t.status, TaskStatus::Pending)));
    }

    #[test]
    fn committed_map_heartbeat_does_not_race_completion_and_duplicate_commit_is_rejected() {
        let dir = tempdir().unwrap();
        let mut state = State::new(
            dir.path().into(),
            vec!["input".into()],
            1,
            Duration::from_secs(5),
        );
        state.register("w1".into(), "/tmp/w1.sock".into());
        std::fs::create_dir_all(worker_local_dir(dir.path(), "w1")).unwrap();
        let attempt = match state.poll("w1") {
            Reply::Task(Task::Map { attempt, .. }) => attempt,
            other => panic!("{other:?}"),
        };
        let output = "part".to_owned();
        std::fs::write(worker_local_dir(dir.path(), "w1").join(&output), b"").unwrap();
        assert!(matches!(
            state.complete_map("w1", 0, attempt, vec![output.clone()]),
            Reply::Complete { accepted: true }
        ));
        assert!(matches!(
            state.heartbeat(
                "w1",
                Some(TaskRef {
                    stage: Stage::Map,
                    task_id: 0,
                    attempt
                })
            ),
            Reply::Heartbeat {
                directive: Directive::Continue
            }
        ));
        assert!(matches!(
            state.complete_map("w1", 0, attempt, vec![output]),
            Reply::Complete { accepted: false }
        ));
    }

    #[test]
    fn reduce_completion_is_single_flight_and_fatal_errors_end_the_job() {
        let dir = tempdir().unwrap();
        let mut state = State::new(dir.path().into(), vec![], 1, Duration::from_secs(5));
        state.register("w1".into(), "/tmp/w1.sock".into());
        let attempt = match state.poll("w1") {
            Reply::Task(Task::Reduce { attempt, .. }) => attempt,
            other => panic!("{other:?}"),
        };
        assert!(matches!(
            state.prepare_reduce_commit("w1", 0, attempt),
            Reply::CommitReduce { .. }
        ));
        assert!(matches!(
            state.prepare_reduce_commit("w1", 0, attempt),
            Reply::Complete { accepted: false }
        ));
        assert!(matches!(
            state.task_failed(
                "w1",
                TaskRef {
                    stage: Stage::Reduce,
                    task_id: 0,
                    attempt
                },
                true,
                "invalid app".into()
            ),
            Reply::Ack { accepted: true }
        ));
        assert_eq!(state.status, JobStatus::Failed);
    }

    #[test]
    fn map_owner_loss_revokes_reducer_attempt_and_requeues_map() {
        let dir = tempdir().unwrap();
        let mut state = State::new(
            dir.path().into(),
            vec!["input".into()],
            1,
            Duration::from_secs(5),
        );
        state.register("map-worker".into(), "/tmp/map.sock".into());
        state.register("reduce-worker".into(), "/tmp/reduce.sock".into());
        std::fs::create_dir_all(worker_local_dir(dir.path(), "map-worker")).unwrap();
        let map_attempt = match state.poll("map-worker") {
            Reply::Task(Task::Map { attempt, .. }) => attempt,
            other => panic!("{other:?}"),
        };
        std::fs::write(worker_local_dir(dir.path(), "map-worker").join("part"), b"").unwrap();
        assert!(matches!(
            state.complete_map("map-worker", 0, map_attempt, vec!["part".into()]),
            Reply::Complete { accepted: true }
        ));
        let reduce_attempt = match state.poll("reduce-worker") {
            Reply::Task(Task::Reduce { attempt, .. }) => attempt,
            other => panic!("{other:?}"),
        };
        state.mark_worker_down("map-worker");
        assert!(matches!(state.maps[0].status, TaskStatus::Pending));
        assert!(matches!(state.reduces[0].status, TaskStatus::Pending));
        assert!(!state.owns(
            "reduce-worker",
            &TaskRef {
                stage: Stage::Reduce,
                task_id: 0,
                attempt: reduce_attempt
            }
        ));
        assert!(matches!(
            state.complete_map("map-worker", 0, map_attempt, vec!["part".into()]),
            Reply::Complete { accepted: false }
        ));
    }

    #[test]
    fn tenth_failed_attempt_transitions_task_and_job_to_failed() {
        let mut task = TaskState {
            status: TaskStatus::Running {
                worker: "w".into(),
                attempt: 10,
                deadline: Instant::now(),
                phase: ReducePhase::Shuffling,
            },
            attempts: 10,
        };
        retry(&mut task);
        assert!(matches!(task.status, TaskStatus::Failed));
    }
}
