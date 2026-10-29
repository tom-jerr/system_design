//! TCP RPC server. All application transitions share one synchronous mutex.

use crate::protocol::{AcquireReply, KvError, KvRpc, VersionedValue};
use crate::state::KvState;
use futures::{StreamExt, TryStreamExt};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tarpc::context::Context;
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Json;
use tokio::net::ToSocketAddrs;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone)]
struct Service {
    state: Arc<Mutex<KvState>>,
}

impl KvRpc for Service {
    async fn get(self, _: Context, key: String) -> Result<VersionedValue, KvError> {
        let state = self.state.lock().map_err(|_| KvError::StatePoisoned)?;
        state.get(&key)
    }

    async fn put(
        self,
        _: Context,
        key: String,
        value: String,
        version: u64,
    ) -> Result<u64, KvError> {
        let mut state = self.state.lock().map_err(|_| KvError::StatePoisoned)?;
        state.put(&key, &value, version)
    }

    async fn put_fenced(
        self,
        _: Context,
        key: String,
        value: String,
        version: u64,
        lock: String,
        token: u64,
    ) -> Result<u64, KvError> {
        let mut state = self.state.lock().map_err(|_| KvError::StatePoisoned)?;
        state.put_fenced_at(&key, &value, version, &lock, token, Instant::now())
    }

    async fn try_acquire(
        self,
        _: Context,
        lock: String,
        acquisition_id: String,
    ) -> Result<AcquireReply, KvError> {
        let mut state = self.state.lock().map_err(|_| KvError::StatePoisoned)?;
        state.try_acquire_at(&lock, &acquisition_id, Instant::now())
    }

    async fn renew(
        self,
        _: Context,
        lock: String,
        acquisition_id: String,
        token: u64,
    ) -> Result<(), KvError> {
        let mut state = self.state.lock().map_err(|_| KvError::StatePoisoned)?;
        state.renew_at(&lock, &acquisition_id, token, Instant::now())
    }

    async fn release(
        self,
        _: Context,
        lock: String,
        acquisition_id: String,
        token: u64,
    ) -> Result<(), KvError> {
        let mut state = self.state.lock().map_err(|_| KvError::StatePoisoned)?;
        state.release_at(&lock, &acquisition_id, token, Instant::now())
    }
}

/// Owns a listening socket and all connection tasks for one volatile KV server.
pub struct KvServer {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl KvServer {
    pub async fn start(address: impl ToSocketAddrs, lease_ttl: Duration) -> io::Result<Self> {
        let state = KvState::new(lease_ttl)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let mut incoming = tarpc::serde_transport::tcp::listen(address, Json::default).await?;
        // Match the four-byte length field's full range; the Tokio default is 8 MiB.
        incoming.config_mut().max_frame_length(u32::MAX as usize);
        let address = incoming.local_addr();
        let service = Service {
            state: Arc::new(Mutex::new(state)),
        };
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    result = connections.join_next(), if !connections.is_empty() => {
                        if let Some(Err(err)) = result {
                            eprintln!("KV connection task failed: {err}");
                        }
                    }
                    accepted = incoming.try_next() => match accepted? {
                        Some(transport) => {
                            let service = service.clone();
                            connections.spawn(async move {
                                BaseChannel::with_defaults(transport)
                                    .execute(service.serve())
                                    .for_each_concurrent(None, |response| response)
                                    .await;
                            });
                        }
                        None => break,
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            Ok(())
        });
        Ok(Self {
            address,
            stop: Some(stop),
            task: Some(task),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Wait for the server task to stop, preserving listener or task errors.
    /// Cancelling this wait does not stop the server.
    pub async fn wait(&mut self) -> io::Result<()> {
        let task = self.task.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "server has already stopped")
        })?;
        let result = task.await;
        self.task.take();
        result.map_err(io::Error::other)?
    }

    /// Stop accepting connections and cancel all in-flight connection tasks.
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        match self.task.take() {
            Some(task) => task.await.map_err(io::Error::other)?,
            None => Ok(()),
        }
    }
}

impl Drop for KvServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
