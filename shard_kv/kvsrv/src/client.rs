//! Retrying RPC client and a lease guard. The server remains the authority on
//! lease validity; a client-side loss notification is deliberately conservative.

use crate::protocol::{AcquireReply, KvError, KvRpcClient, VersionedValue};
use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

const RETRY_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(1);

/// A lightweight client handle. Each attempt opens a new TCP connection, so a
/// disconnected transport never prevents the next attempt from reconnecting.
#[derive(Clone, Debug)]
pub struct Clerk {
    addr: SocketAddr,
    rpc_timeout: Duration,
}

impl Clerk {
    pub fn new(addr: SocketAddr) -> Self {
        Self::with_timeout(addr, DEFAULT_RPC_TIMEOUT)
    }

    /// Sets a deadline for each complete connection-and-RPC attempt.
    pub fn with_timeout(addr: SocketAddr, rpc_timeout: Duration) -> Self {
        assert!(
            !rpc_timeout.is_zero() && Instant::now().checked_add(rpc_timeout).is_some(),
            "RPC timeout must be positive and representable"
        );
        Self { addr, rpc_timeout }
    }

    // None means no application reply arrived. A permanent codec/protocol
    // error is surfaced separately instead of retrying the same bad input forever.
    async fn attempt<T, F, Fut>(&self, timeout: Duration, call: F) -> Result<Option<T>, KvError>
    where
        F: FnOnce(KvRpcClient, context::Context) -> Fut,
        Fut: Future<Output = Result<T, tarpc::client::RpcError>>,
    {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| KvError::Transport("RPC deadline cannot be represented".into()))?;
        match tokio::time::timeout(timeout, async {
            let mut connect = tarpc::serde_transport::tcp::connect(self.addr, Json::default);
            connect.config_mut().max_frame_length(u32::MAX as usize);
            let transport = match connect.await {
                Ok(transport) => transport,
                Err(err) if permanent_error(&err) => {
                    return Err(KvError::Transport(error_chain(&err)))
                }
                Err(_) => return Ok(None),
            };
            let rpc = KvRpcClient::new(client::Config::default(), transport).spawn();
            let mut ctx = context::current();
            ctx.deadline = deadline;
            match call(rpc, ctx).await {
                Ok(reply) => Ok(Some(reply)),
                Err(err) if permanent_rpc_error(&err) => Err(KvError::Transport(error_chain(&err))),
                Err(_) => Ok(None),
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Ok(None),
        }
    }

    pub async fn get(&self, key: &str) -> Result<VersionedValue, KvError> {
        loop {
            let key = key.to_owned();
            if let Some(reply) = self
                .attempt(self.rpc_timeout, |rpc, ctx| async move {
                    rpc.get(ctx, key).await
                })
                .await?
            {
                return reply;
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    pub async fn put(&self, key: &str, value: &str, version: u64) -> Result<u64, KvError> {
        let mut retried = false;
        loop {
            let (key, value) = (key.to_owned(), value.to_owned());
            match self
                .attempt(self.rpc_timeout, |rpc, ctx| async move {
                    rpc.put(ctx, key, value, version).await
                })
                .await
            {
                Ok(Some(Err(err))) if retried => return Err(uncertain(err)),
                Ok(Some(reply)) => return reply,
                Ok(None) => {
                    retried = true;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(err) => return Err(KvError::MaybeWithCause(Box::new(err))),
            }
        }
    }

    /// An earlier timed-out fenced write may have committed before a later
    /// response reports either a version mismatch or a lost lease.
    pub async fn put_fenced(
        &self,
        key: &str,
        value: &str,
        version: u64,
        lock: &str,
        token: u64,
    ) -> Result<u64, KvError> {
        let mut retried = false;
        loop {
            let (key, value, lock) = (key.to_owned(), value.to_owned(), lock.to_owned());
            match self
                .attempt(self.rpc_timeout, |rpc, ctx| async move {
                    rpc.put_fenced(ctx, key, value, version, lock, token).await
                })
                .await
            {
                Ok(Some(Err(err))) if retried => return Err(uncertain(err)),
                Ok(Some(reply)) => return reply,
                Ok(None) => {
                    retried = true;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(err) => return Err(KvError::MaybeWithCause(Box::new(err))),
            }
        }
    }

    /// Wait for a lease. Retransmissions of an in-flight acquisition use the
    /// same ID; an ID whose grant expired is retired and replaced with a new ID.
    pub async fn acquire(&self, lock: &str) -> Result<LeaseGuard, KvError> {
        let mut acquisition_id = Uuid::new_v4().to_string();
        loop {
            let (lock_name, attempt_id) = (lock.to_owned(), acquisition_id.clone());
            match self
                .attempt(self.rpc_timeout, |rpc, ctx| async move {
                    rpc.try_acquire(ctx, lock_name, attempt_id).await
                })
                .await?
            {
                Some(Ok(AcquireReply::Acquired { token, lease_ms })) => {
                    return Ok(LeaseGuard::new(
                        self.clone(),
                        lock.to_owned(),
                        acquisition_id,
                        token,
                        lease_ms,
                    ));
                }
                Some(Ok(AcquireReply::Busy { retry_after_ms })) => {
                    tokio::time::sleep(Duration::from_millis(retry_after_ms.clamp(1, 100))).await;
                }
                Some(Err(KvError::LeaseLost)) => {
                    acquisition_id = Uuid::new_v4().to_string();
                }
                Some(Err(err)) => return Err(err),
                None => tokio::time::sleep(RETRY_DELAY).await,
            }
        }
    }
}

/// Holds a fencing token and maintains its lease while alive. Dropping the
/// guard stops renewal; explicit release is asynchronous.
pub struct LeaseGuard {
    clerk: Clerk,
    lock: String,
    acquisition_id: String,
    token: u64,
    lost: watch::Receiver<bool>,
    renew_task: Option<JoinHandle<()>>,
}

impl LeaseGuard {
    fn new(clerk: Clerk, lock: String, acquisition_id: String, token: u64, lease_ms: u64) -> Self {
        let (lost_tx, lost) = watch::channel(false);
        let renewal_clerk = clerk.clone();
        let renewal_lock = lock.clone();
        let renewal_id = acquisition_id.clone();
        // A replay reports the remaining time at the server; transit can consume
        // any amount of that time, so only server-side fencing proves validity.
        let interval = (Duration::from_millis(lease_ms) / 3).max(Duration::from_millis(1));
        let renewal_timeout = renewal_clerk.rpc_timeout.min(interval);
        let renew_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let (lock, id) = (renewal_lock.clone(), renewal_id.clone());
                let reply = renewal_clerk
                    .attempt(renewal_timeout, |rpc, ctx| async move {
                        rpc.renew(ctx, lock, id, token).await
                    })
                    .await;
                if !matches!(reply, Ok(Some(Ok(())))) {
                    let _ = lost_tx.send(true);
                    break;
                }
            }
        });
        Self {
            clerk,
            lock,
            acquisition_id,
            token,
            lost,
            renew_task: Some(renew_task),
        }
    }

    pub fn token(&self) -> u64 {
        self.token
    }

    pub fn acquisition_id(&self) -> &str {
        &self.acquisition_id
    }

    pub fn lock_name(&self) -> &str {
        &self.lock
    }

    pub fn is_lost(&self) -> bool {
        *self.lost.borrow()
    }

    /// Resolves when a renewal is rejected or cannot be confirmed. This is a
    /// signal to stop relying on the lease, not proof that the server expired it.
    pub async fn wait_lost(&mut self) -> KvError {
        while !*self.lost.borrow_and_update() {
            if self.lost.changed().await.is_err() {
                break;
            }
        }
        KvError::LeaseLost
    }

    /// Stop renewal, then ask the server to release only this lease generation.
    /// A retry never affects a subsequent holder because the server checks ID
    /// and fencing token together.
    pub async fn release(mut self) -> Result<(), KvError> {
        if let Some(task) = self.renew_task.take() {
            task.abort();
            let _ = task.await;
        }
        loop {
            let (lock, id, token) = (self.lock.clone(), self.acquisition_id.clone(), self.token);
            match self
                .clerk
                .attempt(self.clerk.rpc_timeout, |rpc, ctx| async move {
                    rpc.release(ctx, lock, id, token).await
                })
                .await?
            {
                Some(reply) => return reply,
                None => tokio::time::sleep(RETRY_DELAY).await,
            }
        }
    }
}

fn uncertain(err: KvError) -> KvError {
    match err {
        KvError::Version | KvError::LeaseLost => KvError::Maybe,
        other => KvError::MaybeWithCause(Box::new(other)),
    }
}

fn permanent_error(err: &(dyn Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(cause) = current {
        if let Some(io_err) = cause.downcast_ref::<io::Error>() {
            if matches!(
                io_err.kind(),
                io::ErrorKind::InvalidInput
                    | io::ErrorKind::InvalidData
                    | io::ErrorKind::PermissionDenied
            ) {
                return true;
            }
            // io::Error may wrap another io::Error whose kind is not exposed
            // through the outer error's source() chain.
            if io_err.get_ref().is_some_and(|inner| permanent_error(inner)) {
                return true;
            }
        }
        current = cause.source();
    }
    false
}

fn permanent_rpc_error(err: &tarpc::client::RpcError) -> bool {
    use tarpc::{client::RpcError, ChannelError};
    match err {
        RpcError::Server(_) => true,
        RpcError::Send(source) => permanent_error(source.as_ref()),
        RpcError::Channel(channel) => match channel {
            ChannelError::Read(source)
            | ChannelError::Ready(source)
            | ChannelError::Write(source)
            | ChannelError::Flush(source)
            | ChannelError::Close(source) => permanent_error(source.as_ref()),
        },
        RpcError::Shutdown | RpcError::DeadlineExceeded => false,
    }
}

fn error_chain(err: &(dyn Error + 'static)) -> String {
    let mut message = err.to_string();
    let mut current = err.source();
    while let Some(cause) = current {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        current = cause.source();
    }
    message
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Some(task) = self.renew_task.take() {
            task.abort();
        }
    }
}
