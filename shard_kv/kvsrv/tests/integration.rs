use kvsrv::client::Clerk;
use kvsrv::protocol::{AcquireReply, KvError, KvRpcClient, VersionedValue};
use kvsrv::server::KvServer;
use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use tarpc::context;
use tarpc::tokio_serde::formats::Json;
use tokio::sync::oneshot;

async fn server(ttl: Duration) -> KvServer {
    KvServer::start("127.0.0.1:0", ttl).await.unwrap()
}

async fn rpc(addr: SocketAddr) -> KvRpcClient {
    let transport = tarpc::serde_transport::tcp::connect(addr, Json::default)
        .await
        .unwrap();
    KvRpcClient::new(tarpc::client::Config::default(), transport).spawn()
}

fn token(reply: AcquireReply) -> u64 {
    match reply {
        AcquireReply::Acquired { token, .. } => token,
        other => panic!("expected an acquired lease, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_put_is_atomic_with_concurrent_clients() {
    let server = server(Duration::from_secs(2)).await;
    let ck = Clerk::new(server.local_addr());
    assert_eq!(ck.get("absent").await, Err(KvError::NoKey));
    assert_eq!(ck.put("absent", "v", 1).await, Err(KvError::NoKey));
    assert_eq!(ck.put("key", "initial", 0).await, Ok(1));
    assert_eq!(ck.put("key", "wrong", 0).await, Err(KvError::Version));

    let start = Arc::new(tokio::sync::Barrier::new(17));
    let mut jobs = Vec::new();
    for id in 0..16 {
        let clerk = ck.clone();
        let start = start.clone();
        jobs.push(tokio::spawn(async move {
            start.wait().await;
            clerk.put("key", &format!("writer-{id}"), 1).await
        }));
    }
    start.wait().await;
    let mut successes = 0;
    for job in jobs {
        match job.await.unwrap() {
            Ok(2) => successes += 1,
            Err(KvError::Version) => {}
            result => panic!("unexpected result: {result:?}"),
        }
    }
    assert_eq!(successes, 1);
    let observed = ck.get("key").await.unwrap();
    assert_eq!(observed.version, 2);
    assert!(observed.value.starts_with("writer-"));
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_clients_retry_conflicting_compare_and_put() {
    const CLIENTS: usize = 16;
    const ROUNDS: usize = 10;
    let server = server(Duration::from_secs(5)).await;
    let clerk = Clerk::with_timeout(server.local_addr(), Duration::from_secs(5));
    assert_eq!(clerk.put("counter", "0", 0).await, Ok(1));
    let barrier = Arc::new(tokio::sync::Barrier::new(CLIENTS));
    let mut tasks = Vec::new();
    for _ in 0..CLIENTS {
        let (clerk, barrier) = (clerk.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..ROUNDS {
                loop {
                    let current = clerk.get("counter").await.unwrap();
                    let next = current.value.parse::<u64>().unwrap() + 1;
                    match clerk
                        .put("counter", &next.to_string(), current.version)
                        .await
                    {
                        Ok(version) => {
                            assert_eq!(version, current.version + 1);
                            break;
                        }
                        Err(KvError::Version) => continue,
                        other => panic!("unexpected reliable-network result: {other:?}"),
                    }
                }
            }
        }));
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .expect("conditional writes did not finish");
    assert_eq!(
        clerk.get("counter").await,
        Ok(VersionedValue {
            value: (CLIENTS * ROUNDS).to_string(),
            version: 1 + (CLIENTS * ROUNDS) as u64,
        })
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_read_respects_completed_conditional_write() {
    let server = server(Duration::from_secs(2)).await;
    let clerk = Clerk::new(server.local_addr());
    assert_eq!(clerk.put("key", "initial", 0).await, Ok(1));
    let order = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let mut writers = Vec::new();
    for value in ["one", "two"] {
        let (clerk, order, barrier) = (clerk.clone(), order.clone(), barrier.clone());
        writers.push(tokio::spawn(async move {
            barrier.wait().await;
            let start = order.fetch_add(1, Ordering::SeqCst);
            let result = clerk.put("key", value, 1).await;
            let end = order.fetch_add(1, Ordering::SeqCst);
            (start, end, value, result)
        }));
    }
    let (reader, read_order, read_barrier) = (clerk.clone(), order.clone(), barrier.clone());
    let reader = tokio::spawn(async move {
        read_barrier.wait().await;
        let start = read_order.fetch_add(1, Ordering::SeqCst);
        let result = reader.get("key").await.unwrap();
        let end = read_order.fetch_add(1, Ordering::SeqCst);
        (start, end, result)
    });
    barrier.wait().await;
    let first = writers.remove(0).await.unwrap();
    let second = writers.remove(0).await.unwrap();
    let winner = match (&first.3, &second.3) {
        (Ok(2), Err(KvError::Version)) => first,
        (Err(KvError::Version), Ok(2)) => second,
        other => panic!("unexpected competing writes: {other:?}"),
    };
    let (read_start, read_end, observed) = reader.await.unwrap();
    let initial = VersionedValue {
        value: "initial".into(),
        version: 1,
    };
    let final_value = VersionedValue {
        value: winner.2.into(),
        version: 2,
    };
    assert!(observed == initial || observed == final_value);
    if winner.1 < read_start {
        assert_eq!(observed, final_value);
    }
    if read_end < winner.0 {
        assert_eq!(observed, initial);
    }
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_renewal_release_and_independent_locks() {
    let server = server(Duration::from_millis(600)).await;
    let ck = Clerk::new(server.local_addr());
    let guard = ck.acquire("one").await.unwrap();
    let initial = guard.token();
    let other = ck.acquire("two").await.unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(!guard.is_lost());

    let raw = rpc(server.local_addr()).await;
    assert!(matches!(
        raw.try_acquire(context::current(), "one".into(), "waiting".into())
            .await
            .unwrap()
            .unwrap(),
        AcquireReply::Busy { .. }
    ));
    guard.release().await.unwrap();
    let next = raw
        .try_acquire(context::current(), "one".into(), "waiting".into())
        .await
        .unwrap()
        .unwrap();
    assert!(token(next) > initial);
    other.release().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_acquire_and_stale_token_cannot_affect_new_owner() {
    let server = server(Duration::from_millis(250)).await;
    let raw = rpc(server.local_addr()).await;
    let old = token(
        raw.try_acquire(context::current(), "lock".into(), "old".into())
            .await
            .unwrap()
            .unwrap(),
    );
    let duplicate = token(
        raw.try_acquire(context::current(), "lock".into(), "old".into())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(old, duplicate);
    tokio::time::sleep(Duration::from_millis(350)).await;
    let new = token(
        raw.try_acquire(context::current(), "lock".into(), "new".into())
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(new > old);
    assert_eq!(
        raw.release(context::current(), "lock".into(), "old".into(), old)
            .await
            .unwrap(),
        Err(KvError::LeaseLost)
    );
    assert_eq!(
        raw.renew(context::current(), "lock".into(), "old".into(), old)
            .await
            .unwrap(),
        Err(KvError::LeaseLost)
    );
    assert_eq!(
        raw.try_acquire(context::current(), "lock".into(), "old".into())
            .await
            .unwrap(),
        Err(KvError::LeaseLost)
    );
    assert_eq!(
        raw.put_fenced(
            context::current(),
            "key".into(),
            "stale".into(),
            0,
            "lock".into(),
            old
        )
        .await
        .unwrap(),
        Err(KvError::LeaseLost)
    );
    assert_eq!(
        raw.put_fenced(
            context::current(),
            "key".into(),
            "fresh".into(),
            0,
            "lock".into(),
            new
        )
        .await
        .unwrap(),
        Ok(1)
    );
    assert_eq!(
        raw.release(context::current(), "lock".into(), "new".into(), new)
            .await
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        raw.try_acquire(context::current(), "lock".into(), "old".into())
            .await
            .unwrap(),
        Err(KvError::LeaseLost)
    );
    let fresh = raw
        .try_acquire(context::current(), "lock".into(), "fresh".into())
        .await
        .unwrap()
        .unwrap();
    assert!(token(fresh) > new);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fenced_write_racing_release_has_one_atomic_order() {
    let server = server(Duration::from_secs(3)).await;
    let raw = rpc(server.local_addr()).await;
    let token = token(
        raw.try_acquire(context::current(), "lock".into(), "owner".into())
            .await
            .unwrap()
            .unwrap(),
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let (writer, writer_barrier) = (raw.clone(), barrier.clone());
    let put = tokio::spawn(async move {
        writer_barrier.wait().await;
        writer
            .put_fenced(
                context::current(),
                "key".into(),
                "written".into(),
                0,
                "lock".into(),
                token,
            )
            .await
            .unwrap()
    });
    let (releaser, release_barrier) = (raw.clone(), barrier.clone());
    let release = tokio::spawn(async move {
        release_barrier.wait().await;
        releaser
            .release(context::current(), "lock".into(), "owner".into(), token)
            .await
            .unwrap()
    });
    barrier.wait().await;
    let result = put.await.unwrap();
    assert_eq!(release.await.unwrap(), Ok(()));
    let value = raw.get(context::current(), "key".into()).await.unwrap();
    match result {
        Ok(1) => assert_eq!(
            value,
            Ok(VersionedValue {
                value: "written".into(),
                version: 1
            })
        ),
        Err(KvError::LeaseLost) => assert_eq!(value, Err(KvError::NoKey)),
        other => panic!("unexpected fenced write result: {other:?}"),
    }
    server.shutdown().await.unwrap();
}

// Each client RPC uses a new TCP connection. Queued proxy faults can drop,
// delay, or corrupt individual requests and replies, or partition the client.
enum Fault {
    DropRequest,
    CorruptResponse,
    HoldRequest {
        observed: oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
        completed: oneshot::Sender<()>,
    },
    HoldResponse {
        observed: oneshot::Sender<()>,
        release: mpsc::Receiver<()>,
    },
    DropResponse {
        observed: oneshot::Sender<()>,
        release: Option<mpsc::Receiver<()>>,
    },
}

struct Proxy {
    addr: SocketAddr,
    next: Arc<Mutex<VecDeque<Fault>>>,
    running: Arc<AtomicBool>,
    partitioned: Arc<AtomicBool>,
    blocked_attempts: Arc<AtomicU64>,
    accept_thread: Option<thread::JoinHandle<()>>,
}

impl Proxy {
    fn new(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let next = Arc::new(Mutex::new(VecDeque::new()));
        let running = Arc::new(AtomicBool::new(true));
        let partitioned = Arc::new(AtomicBool::new(false));
        let blocked_attempts = Arc::new(AtomicU64::new(0));
        let faults = next.clone();
        let alive = running.clone();
        let blocked = partitioned.clone();
        let blocked_count = blocked_attempts.clone();
        let accept_thread = thread::spawn(move || {
            while alive.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        if blocked.load(Ordering::SeqCst) {
                            blocked_count.fetch_add(1, Ordering::SeqCst);
                            let _ = socket.shutdown(Shutdown::Both);
                            continue;
                        }
                        let fault = faults.lock().unwrap().pop_front();
                        thread::spawn(move || proxy_connection(socket, target, fault));
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(err) => panic!("proxy accept: {err}"),
                }
            }
        });
        Self {
            addr,
            next,
            running,
            partitioned,
            blocked_attempts,
            accept_thread: Some(accept_thread),
        }
    }

    fn drop_request(&self) {
        self.next.lock().unwrap().push_back(Fault::DropRequest);
    }

    fn corrupt_response(&self) {
        self.next.lock().unwrap().push_back(Fault::CorruptResponse);
    }

    fn partition(&self, blocked: bool) {
        self.partitioned.store(blocked, Ordering::SeqCst);
    }

    fn hold_request(
        &self,
    ) -> (
        oneshot::Receiver<()>,
        mpsc::Sender<()>,
        oneshot::Receiver<()>,
    ) {
        let (seen_tx, seen_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (done_tx, done_rx) = oneshot::channel();
        self.next.lock().unwrap().push_back(Fault::HoldRequest {
            observed: seen_tx,
            release: release_rx,
            completed: done_tx,
        });
        (seen_rx, release_tx, done_rx)
    }

    fn hold_response(&self) -> (oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (seen_tx, seen_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        self.next.lock().unwrap().push_back(Fault::HoldResponse {
            observed: seen_tx,
            release: release_rx,
        });
        (seen_rx, release_tx)
    }

    fn drop_response(&self, hold: bool) -> (oneshot::Receiver<()>, Option<mpsc::Sender<()>>) {
        let (seen_tx, seen_rx) = oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        self.next.lock().unwrap().push_back(Fault::DropResponse {
            observed: seen_tx,
            release: hold.then_some(release_rx),
        });
        (seen_rx, hold.then_some(release_tx))
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(thread) = self.accept_thread.take() {
            thread.join().unwrap();
        }
    }
}

fn proxy_connection(mut downstream: TcpStream, target: SocketAddr, fault: Option<Fault>) {
    if matches!(fault, Some(Fault::DropRequest)) {
        let _ = downstream.shutdown(Shutdown::Both);
        return;
    }
    let mut upstream = match TcpStream::connect(target) {
        Ok(stream) => stream,
        Err(_) => return,
    };
    let fault = match fault {
        Some(Fault::HoldRequest {
            observed,
            release,
            completed,
        }) => {
            let mut header = [0u8; 4];
            if downstream.read_exact(&mut header).is_err() {
                return;
            }
            let mut body = vec![0; u32::from_be_bytes(header) as usize];
            if downstream.read_exact(&mut body).is_err() {
                return;
            }
            let _ = observed.send(());
            if release.recv_timeout(Duration::from_secs(10)).is_ok() {
                let _ = upstream
                    .write_all(&header)
                    .and_then(|_| upstream.write_all(&body));
                let _ = upstream.set_read_timeout(Some(Duration::from_secs(1)));
                let _ = io::copy(&mut upstream, &mut downstream);
            }
            let _ = completed.send(());
            return;
        }
        other => other,
    };
    let mut client_reader = downstream.try_clone().unwrap();
    let mut server_writer = upstream.try_clone().unwrap();
    thread::spawn(move || {
        let _ = io::copy(&mut client_reader, &mut server_writer);
        let _ = server_writer.shutdown(Shutdown::Write);
    });
    match fault {
        Some(Fault::HoldResponse { observed, release }) => {
            let mut header = [0u8; 4];
            if upstream.read_exact(&mut header).is_ok() {
                let mut body = vec![0; u32::from_be_bytes(header) as usize];
                if upstream.read_exact(&mut body).is_ok() {
                    let _ = observed.send(());
                    if release.recv_timeout(Duration::from_secs(10)).is_ok() {
                        let _ = downstream
                            .write_all(&header)
                            .and_then(|_| downstream.write_all(&body));
                    }
                }
            }
        }
        Some(Fault::CorruptResponse) => {
            let mut bytes = [0u8; 8192];
            if upstream.read(&mut bytes).unwrap_or(0) != 0 {
                // A complete length-delimited frame containing invalid JSON.
                let _ = downstream.write_all(&[0, 0, 0, 1, b'!']);
            }
        }
        Some(Fault::DropResponse { observed, release }) => {
            let mut bytes = [0u8; 8192];
            if upstream.read(&mut bytes).unwrap_or(0) != 0 {
                let _ = observed.send(());
                if let Some(release) = release {
                    let _ = release.recv_timeout(Duration::from_secs(10));
                }
            }
        }
        _ => {
            let _ = io::copy(&mut upstream, &mut downstream);
        }
    }
    let _ = downstream.shutdown(Shutdown::Both);
    let _ = upstream.shutdown(Shutdown::Both);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_write_reply_reports_uncertainty_and_transport_cause() {
    let server = server(Duration::from_secs(2)).await;
    let proxy = Proxy::new(server.local_addr());
    let ck = Clerk::with_timeout(proxy.addr, Duration::from_secs(1));
    proxy.corrupt_response();
    let result = tokio::time::timeout(Duration::from_secs(3), ck.put("key", "value", 0))
        .await
        .expect("permanent codec failure must not retry forever");
    assert!(
        matches!(&result, Err(KvError::MaybeWithCause(cause)) if matches!(**cause, KvError::Transport(_))),
        "unexpected result: {result:?}"
    );
    assert_eq!(
        Clerk::new(server.local_addr())
            .get("key")
            .await
            .unwrap()
            .version,
        1
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_transports_values_larger_than_the_default_frame_limit() {
    let server = server(Duration::from_secs(5)).await;
    let ck = Clerk::with_timeout(server.local_addr(), Duration::from_secs(10));
    let value = "x".repeat(8 * 1024 * 1024 + 1);
    assert_eq!(ck.put("large", &value, 0).await, Ok(1));
    assert_eq!(ck.get("large").await.unwrap().value, value);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_and_response_loss_have_distinct_put_outcomes() {
    let server = server(Duration::from_secs(2)).await;
    let direct = Clerk::new(server.local_addr());
    let proxy = Proxy::new(server.local_addr());
    let ck = Clerk::with_timeout(proxy.addr, Duration::from_millis(500));

    proxy.drop_request();
    assert_eq!(ck.put("request-lost", "applied", 0).await, Ok(1));
    assert_eq!(direct.get("request-lost").await.unwrap().version, 1);

    let (seen, _) = proxy.drop_response(false);
    let put = ck.put("response-lost", "applied", 0);
    let (result, seen) = tokio::join!(put, seen);
    seen.unwrap();
    assert_eq!(result, Err(KvError::Maybe));
    assert_eq!(
        direct.get("response-lost").await,
        Ok(VersionedValue {
            value: "applied".into(),
            version: 1,
        })
    );
    assert_eq!(
        ck.put("response-lost", "other", 0).await,
        Err(KvError::Version)
    );

    let (seen, _) = proxy.drop_response(false);
    let (result, seen) = tokio::join!(ck.get("response-lost"), seen);
    seen.unwrap();
    assert_eq!(
        result,
        Ok(VersionedValue {
            value: "applied".into(),
            version: 1
        })
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_conflict_reply_returns_maybe_even_when_write_never_committed() {
    let server = server(Duration::from_secs(2)).await;
    let direct = Clerk::new(server.local_addr());
    assert_eq!(direct.put("key", "first", 0).await, Ok(1));
    assert_eq!(direct.put("key", "winner", 1).await, Ok(2));
    let proxy = Proxy::new(server.local_addr());
    let clerk = Clerk::with_timeout(proxy.addr, Duration::from_millis(500));
    let (seen, _) = proxy.drop_response(false);
    let (result, observed) = tokio::join!(clerk.put("key", "never-written", 1), seen);
    observed.unwrap();
    assert_eq!(result, Err(KvError::Maybe));
    assert_eq!(
        direct.get("key").await,
        Ok(VersionedValue {
            value: "winner".into(),
            version: 2
        })
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_request_and_reply_loss_does_not_duplicate_put() {
    let server = server(Duration::from_secs(2)).await;
    let proxy = Proxy::new(server.local_addr());
    let clerk = Clerk::with_timeout(proxy.addr, Duration::from_millis(500));
    proxy.drop_request();
    let (first_reply, _) = proxy.drop_response(false);
    proxy.drop_request();
    let (second_reply, _) = proxy.drop_response(false);
    let result = tokio::time::timeout(Duration::from_secs(5), clerk.put("key", "value", 0))
        .await
        .expect("retries did not finish");
    first_reply.await.unwrap();
    second_reply.await.unwrap();
    assert_eq!(result, Err(KvError::Maybe));
    assert_eq!(
        Clerk::new(server.local_addr()).get("key").await,
        Ok(VersionedValue {
            value: "value".into(),
            version: 1
        })
    );
    assert!(proxy.next.lock().unwrap().is_empty());
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_first_request_cannot_overwrite_a_later_version() {
    let server = server(Duration::from_secs(2)).await;
    let direct = Clerk::new(server.local_addr());
    let proxy = Proxy::new(server.local_addr());
    let clerk = Clerk::with_timeout(proxy.addr, Duration::from_millis(200));
    let (seen, release, completed) = proxy.hold_request();
    let write = tokio::spawn(async move { clerk.put("key", "first", 0).await });
    tokio::time::timeout(Duration::from_secs(2), seen)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(4), write)
            .await
            .unwrap()
            .unwrap(),
        Ok(1)
    );
    assert_eq!(direct.put("key", "later", 1).await, Ok(2));
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), completed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        direct.get("key").await,
        Ok(VersionedValue {
            value: "later".into(),
            version: 2
        })
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replies_can_arrive_out_of_order_without_losing_writes() {
    let server = server(Duration::from_secs(3)).await;
    let direct = Clerk::new(server.local_addr());
    let proxy = Proxy::new(server.local_addr());
    let clerk = Clerk::with_timeout(proxy.addr, Duration::from_secs(3));
    let (seen, release) = proxy.hold_response();
    let first = tokio::spawn(async move { clerk.put("key", "first", 0).await });
    tokio::time::timeout(Duration::from_secs(2), seen)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(direct.put("key", "second", 1).await, Ok(2));
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), first)
            .await
            .unwrap()
            .unwrap(),
        Ok(1)
    );
    assert_eq!(
        direct.get("key").await,
        Ok(VersionedValue {
            value: "second".into(),
            version: 2
        })
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_after_server_applies_put_does_not_undo_write() {
    let server = server(Duration::from_secs(2)).await;
    let proxy = Proxy::new(server.local_addr());
    let clerk = Clerk::with_timeout(proxy.addr, Duration::from_secs(2));
    let (seen, release) = proxy.hold_response();
    let write = tokio::spawn(async move { clerk.put("key", "committed", 0).await });
    tokio::time::timeout(Duration::from_secs(2), seen)
        .await
        .unwrap()
        .unwrap();
    write.abort();
    assert!(write.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    assert_eq!(
        Clerk::new(server.local_addr()).get("key").await,
        Ok(VersionedValue {
            value: "committed".into(),
            version: 1
        })
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_after_grant_leaves_only_a_temporary_orphan_lease() {
    let server = server(Duration::from_millis(600)).await;
    let proxy = Proxy::new(server.local_addr());
    let clerk = Clerk::with_timeout(proxy.addr, Duration::from_secs(2));
    let (seen, release) = proxy.hold_response();
    let acquire = tokio::spawn(async move { clerk.acquire("lock").await });
    tokio::time::timeout(Duration::from_secs(2), seen)
        .await
        .unwrap()
        .unwrap();
    acquire.abort();
    assert!(acquire.await.is_err());
    let raw = rpc(server.local_addr()).await;
    assert!(matches!(
        raw.try_acquire(context::current(), "lock".into(), "next".into())
            .await
            .unwrap()
            .unwrap(),
        AcquireReply::Busy { .. }
    ));
    release.send(()).unwrap();
    let successor = tokio::time::timeout(
        Duration::from_secs(4),
        Clerk::new(server.local_addr()).acquire("lock"),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(successor.token() > 1);
    assert_eq!(
        Clerk::new(server.local_addr())
            .put_fenced("key", "stale", 0, "lock", 1)
            .await,
        Err(KvError::LeaseLost)
    );
    successor.release().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_frame_does_not_execute_or_stop_server() {
    let server = server(Duration::from_secs(2)).await;
    let mut connection = TcpStream::connect(server.local_addr()).unwrap();
    connection.write_all(&[0, 0, 0, 100, b'{']).unwrap();
    connection.shutdown(Shutdown::Both).unwrap();
    let clerk = Clerk::new(server.local_addr());
    assert_eq!(clerk.get("key").await, Err(KvError::NoKey));
    assert_eq!(clerk.put("key", "valid", 0).await, Ok(1));
    assert_eq!(clerk.get("key").await.unwrap().value, "valid");
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_recovers_after_multiple_partitioned_attempts() {
    let server = server(Duration::from_secs(2)).await;
    let direct = Clerk::new(server.local_addr());
    let proxy = Proxy::new(server.local_addr());
    proxy.partition(true);
    let clerk = Clerk::with_timeout(proxy.addr, Duration::from_millis(200));
    let write = tokio::spawn(async move { clerk.put("key", "after-recovery", 0).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while proxy.blocked_attempts.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("client did not retry while partitioned");
    assert_eq!(direct.get("key").await, Err(KvError::NoKey));
    proxy.partition(false);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), write)
            .await
            .unwrap()
            .unwrap(),
        Ok(1)
    );
    assert_eq!(direct.get("key").await.unwrap().value, "after-recovery");
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_holder_loses_lease_and_cannot_affect_successor() {
    let server = server(Duration::from_millis(900)).await;
    let proxy = Proxy::new(server.local_addr());
    let remote = Clerk::with_timeout(proxy.addr, Duration::from_millis(300));
    let mut old = remote.acquire("lock").await.unwrap();
    let old_token = old.token();
    proxy.partition(true);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), old.wait_lost())
            .await
            .unwrap(),
        KvError::LeaseLost
    );
    let direct = Clerk::new(server.local_addr());
    let successor = tokio::time::timeout(Duration::from_secs(5), direct.acquire("lock"))
        .await
        .unwrap()
        .unwrap();
    assert!(successor.token() > old_token);
    proxy.partition(false);
    assert_eq!(
        direct.put_fenced("key", "old", 0, "lock", old_token).await,
        Err(KvError::LeaseLost)
    );
    assert_eq!(old.release().await, Err(KvError::LeaseLost));
    assert_eq!(
        direct
            .put_fenced("key", "new", 0, "lock", successor.token())
            .await,
        Ok(1)
    );
    successor.release().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_clients_contend_for_one_lease_without_lost_updates() {
    const CLIENTS: usize = 8;
    const ROUNDS: usize = 3;
    let server = server(Duration::from_secs(5)).await;
    let clerk = Clerk::new(server.local_addr());
    assert_eq!(clerk.put("counter", "0", 0).await, Ok(1));
    let active = Arc::new(AtomicU64::new(0));
    let start = Arc::new(tokio::sync::Barrier::new(CLIENTS));
    let mut tasks = Vec::new();
    for _ in 0..CLIENTS {
        let (clerk, active, start) = (clerk.clone(), active.clone(), start.clone());
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            for _ in 0..ROUNDS {
                let guard = clerk.acquire("counter-lock").await.unwrap();
                assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                let before = clerk.get("counter").await.unwrap();
                let next = before.value.parse::<u64>().unwrap() + 1;
                assert_eq!(
                    clerk
                        .put_fenced(
                            "counter",
                            &next.to_string(),
                            before.version,
                            "counter-lock",
                            guard.token()
                        )
                        .await,
                    Ok(before.version + 1)
                );
                assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                guard.release().await.unwrap();
            }
        }));
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .expect("lock contention did not finish");
    assert_eq!(
        clerk.get("counter").await,
        Ok(VersionedValue {
            value: (CLIENTS * ROUNDS).to_string(),
            version: 1 + (CLIENTS * ROUNDS) as u64
        })
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_acquire_and_release_replies_do_not_change_the_next_lease() {
    let server = server(Duration::from_secs(3)).await;
    let raw = rpc(server.local_addr()).await;
    let proxy = Proxy::new(server.local_addr());
    let ck = Clerk::with_timeout(proxy.addr, Duration::from_secs(1));

    let (seen, _) = proxy.drop_response(false);
    let (guard, seen) = tokio::join!(ck.acquire("lock"), seen);
    seen.unwrap();
    let guard = guard.unwrap();
    // A reissued acquire returns the first grant, instead of generating a
    // second fence for the same logical acquisition.
    assert_eq!(guard.token(), 1);
    assert_eq!(
        token(
            raw.try_acquire(
                context::current(),
                "lock".into(),
                guard.acquisition_id().to_owned()
            )
            .await
            .unwrap()
            .unwrap()
        ),
        1
    );

    let (seen, release) = proxy.drop_response(true);
    let release_task = tokio::spawn(async move { guard.release().await });
    tokio::time::timeout(Duration::from_secs(2), seen)
        .await
        .unwrap()
        .unwrap();
    let new = token(
        raw.try_acquire(context::current(), "lock".into(), "new".into())
            .await
            .unwrap()
            .unwrap(),
    );
    assert!(new > 1);
    release.unwrap().send(()).unwrap();
    // The retry can report LeaseLost because the successful release's reply
    // was lost, but it must not release the newly granted lease.
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), release_task)
            .await
            .unwrap()
            .unwrap(),
        Err(KvError::LeaseLost)
    );
    assert!(matches!(
        raw.try_acquire(context::current(), "lock".into(), "third".into())
            .await
            .unwrap()
            .unwrap(),
        AcquireReply::Busy { .. }
    ));
    assert_eq!(
        raw.put_fenced(
            context::current(),
            "key".into(),
            "new".into(),
            0,
            "lock".into(),
            new
        )
        .await
        .unwrap(),
        Ok(1)
    );
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_renewal_reply_conservatively_reports_lease_lost() {
    let server = server(Duration::from_millis(900)).await;
    let proxy = Proxy::new(server.local_addr());
    let ck = Clerk::with_timeout(proxy.addr, Duration::from_millis(400));
    let mut guard = ck.acquire("renew").await.unwrap();
    let (seen, _) = proxy.drop_response(false);
    tokio::time::timeout(Duration::from_secs(3), seen)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), guard.wait_lost())
            .await
            .unwrap(),
        KvError::LeaseLost
    );
    assert!(guard.is_lost());
    drop(guard);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fenced_put_reply_lost_then_lease_expires_is_maybe() {
    let ttl = Duration::from_millis(250);
    let server = server(ttl).await;
    let direct = Clerk::new(server.local_addr());
    let guard = direct.acquire("fence").await.unwrap();
    let old = guard.token();
    let proxy = Proxy::new(server.local_addr());
    let ck = Clerk::with_timeout(proxy.addr, Duration::from_secs(2));
    let (seen, release) = proxy.drop_response(true);
    let task =
        tokio::spawn(async move { ck.put_fenced("key", "committed", 0, "fence", old).await });
    tokio::time::timeout(Duration::from_secs(2), seen)
        .await
        .unwrap()
        .unwrap();
    drop(guard); // no further renewals; the already applied write's reply is held
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;
    release.unwrap().send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap(),
        Err(KvError::Maybe)
    );
    assert_eq!(direct.get("key").await.unwrap().value, "committed");
    server.shutdown().await.unwrap();
}

struct ChildProcess(Child);
impl Drop for ChildProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Spawned by the test below. Running normally as part of cargo test is a no-op.
#[test]
fn child_lock_holder() {
    let Ok(addr) = std::env::var("KVSRV_TEST_CHILD_ADDR") else {
        return;
    };
    let addr: SocketAddr = addr.parse().unwrap();
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let guard = Clerk::new(addr).acquire("crash").await.unwrap();
            println!("READY {} {}", guard.token(), guard.acquisition_id());
            io::stdout().flush().unwrap();
            loop {
                std::hint::black_box(&guard);
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crashed_holder_stops_renewing_and_lock_can_be_reacquired() {
    let server = server(Duration::from_millis(500)).await;
    let mut child = ChildProcess(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_lock_holder", "--nocapture"])
            .env("KVSRV_TEST_CHILD_ADDR", server.local_addr().to_string())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(pos) = line.find("READY ") {
                let parts: Vec<_> = line[pos + 6..].split_whitespace().collect();
                let _ = ready_tx.send((parts[0].parse::<u64>().unwrap(), parts[1].to_owned()));
                return;
            }
        }
    });
    let (old_token, old_id) = tokio::time::timeout(Duration::from_secs(5), ready_rx)
        .await
        .unwrap()
        .unwrap();
    let raw = rpc(server.local_addr()).await;
    assert!(matches!(
        raw.try_acquire(context::current(), "crash".into(), "new".into())
            .await
            .unwrap()
            .unwrap(),
        AcquireReply::Busy { .. }
    ));
    child.0.kill().unwrap();
    child.0.wait().unwrap();

    let new_token = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match raw
                .try_acquire(context::current(), "crash".into(), "new".into())
                .await
                .unwrap()
                .unwrap()
            {
                AcquireReply::Acquired { token, .. } => break token,
                AcquireReply::Busy { .. } => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .unwrap();
    assert!(new_token > old_token);
    assert_eq!(
        raw.release(context::current(), "crash".into(), old_id, old_token)
            .await
            .unwrap(),
        Err(KvError::LeaseLost)
    );
    assert_eq!(
        raw.put_fenced(
            context::current(),
            "crash-key".into(),
            "stale".into(),
            0,
            "crash".into(),
            old_token
        )
        .await
        .unwrap(),
        Err(KvError::LeaseLost)
    );
    server.shutdown().await.unwrap();
}

// A small exhaustive checker for the versioned, single-key model. It uses
// invocation/response order just as a history checker does; it is deliberately
// bounded to the short histories generated below.
#[derive(Clone, Debug)]
enum HistoryCall {
    Get(Result<VersionedValue, KvError>),
    Put {
        value: String,
        version: u64,
        result: Result<u64, KvError>,
    },
}

#[derive(Clone, Debug)]
struct HistoryEntry {
    key: String,
    start: u64,
    end: u64,
    call: HistoryCall,
}

type History = Arc<Mutex<Vec<HistoryEntry>>>;

async fn record_get(clerk: &Clerk, key: &str, clock: &AtomicU64, history: &History) {
    let start = clock.fetch_add(1, Ordering::SeqCst);
    let result = clerk.get(key).await;
    let end = clock.fetch_add(1, Ordering::SeqCst);
    history.lock().unwrap().push(HistoryEntry {
        key: key.into(),
        start,
        end,
        call: HistoryCall::Get(result),
    });
}

async fn record_put(
    clerk: &Clerk,
    key: &str,
    value: &str,
    version: u64,
    clock: &AtomicU64,
    history: &History,
) {
    let start = clock.fetch_add(1, Ordering::SeqCst);
    let result = clerk.put(key, value, version).await;
    let end = clock.fetch_add(1, Ordering::SeqCst);
    history.lock().unwrap().push(HistoryEntry {
        key: key.into(),
        start,
        end,
        call: HistoryCall::Put {
            value: value.into(),
            version,
            result,
        },
    });
}

fn history_is_linearizable(entries: &[HistoryEntry], initial: VersionedValue) -> bool {
    assert!(entries.len() < u64::BITS as usize);
    fn search(entries: &[HistoryEntry], used: u64, current: &VersionedValue) -> bool {
        if used.count_ones() as usize == entries.len() {
            return true;
        }
        for (index, entry) in entries.iter().enumerate() {
            let bit = 1_u64 << index;
            if used & bit != 0
                || entries.iter().enumerate().any(|(other, preceding)| {
                    used & (1_u64 << other) == 0 && other != index && preceding.end < entry.start
                })
            {
                continue;
            }
            let next = match &entry.call {
                HistoryCall::Get(Ok(value)) if value == current => Some(current.clone()),
                HistoryCall::Put {
                    value,
                    version,
                    result: Ok(next_version),
                } if *version == current.version
                    && current.version.checked_add(1) == Some(*next_version) =>
                {
                    Some(VersionedValue {
                        value: value.clone(),
                        version: *next_version,
                    })
                }
                HistoryCall::Put {
                    version,
                    result: Err(KvError::Version),
                    ..
                } if *version != current.version => Some(current.clone()),
                HistoryCall::Put {
                    value,
                    version,
                    result: Err(KvError::Maybe),
                } if *version == current.version => {
                    current
                        .version
                        .checked_add(1)
                        .map(|next_version| VersionedValue {
                            value: value.clone(),
                            version: next_version,
                        })
                }
                HistoryCall::Put {
                    version,
                    result: Err(KvError::Maybe),
                    ..
                } if *version != current.version => Some(current.clone()),
                _ => None,
            };
            if next.is_some_and(|next| search(entries, used | bit, &next)) {
                return true;
            }
        }
        false
    }
    search(entries, 0, &initial)
}

#[test]
fn history_checker_rejects_stale_read_after_completed_write() {
    let initial = VersionedValue {
        value: "old".into(),
        version: 1,
    };
    let history = [
        HistoryEntry {
            key: "k".into(),
            start: 0,
            end: 1,
            call: HistoryCall::Put {
                value: "new".into(),
                version: 1,
                result: Ok(2),
            },
        },
        HistoryEntry {
            key: "k".into(),
            start: 2,
            end: 3,
            call: HistoryCall::Get(Ok(initial.clone())),
        },
    ];
    assert!(!history_is_linearizable(&history, initial));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_faulty_history_has_a_linearizable_explanation() {
    for _ in 0..5 {
        check_faulty_history_once().await;
    }
}

async fn check_faulty_history_once() {
    let server = server(Duration::from_secs(3)).await;
    let direct = Clerk::new(server.local_addr());
    assert_eq!(direct.put("hot", "seed", 0).await, Ok(1));
    assert_eq!(direct.put("other", "seed", 0).await, Ok(1));
    let proxy = Proxy::new(server.local_addr());
    let faulty = Clerk::with_timeout(proxy.addr, Duration::from_millis(500));
    let (seen, _) = proxy.drop_response(false);
    let clock = Arc::new(AtomicU64::new(0));
    let history: History = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(6));
    let mut tasks = Vec::new();
    for index in 0..6 {
        let (clerk, faulty, clock, history, barrier) = (
            direct.clone(),
            faulty.clone(),
            clock.clone(),
            history.clone(),
            barrier.clone(),
        );
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            match index {
                0 => record_put(&faulty, "hot", "faulty", 1, &clock, &history).await,
                1 => record_put(&clerk, "hot", "competitor", 1, &clock, &history).await,
                2 => record_get(&clerk, "hot", &clock, &history).await,
                3 => record_put(&clerk, "other", "one", 1, &clock, &history).await,
                4 => {
                    record_get(&clerk, "other", &clock, &history).await;
                    record_put(&clerk, "other", "two", 1, &clock, &history).await;
                }
                _ => {
                    record_get(&clerk, "hot", &clock, &history).await;
                    record_put(&clerk, "hot", "later", 2, &clock, &history).await;
                }
            }
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .expect("history workload did not finish");
    seen.await.unwrap();
    record_get(&direct, "hot", &clock, &history).await;
    record_get(&direct, "other", &clock, &history).await;
    let history = history.lock().unwrap().clone();
    assert!(history.iter().any(|entry| matches!(&entry.call, HistoryCall::Put { value, result: Err(KvError::Maybe), .. } if value == "faulty")), "the dropped reply must exercise Maybe: {history:?}");
    for key in ["hot", "other"] {
        let entries: Vec<_> = history
            .iter()
            .filter(|entry| entry.key == key)
            .cloned()
            .collect();
        assert!(
            history_is_linearizable(
                &entries,
                VersionedValue {
                    value: "seed".into(),
                    version: 1
                }
            ),
            "non-linearizable history for {key}: {entries:?}"
        );
    }
    server.shutdown().await.unwrap();
}
