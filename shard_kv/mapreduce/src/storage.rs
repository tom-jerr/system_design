//! Worker-local intermediate-file RPC over Unix domain sockets.

use crate::protocol::{DataReply, DataRequest};
use serde::{de::DeserializeOwned, Serialize};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static TEMP_ID: AtomicU64 = AtomicU64::new(1);

pub struct DataServer {
    socket: PathBuf,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl DataServer {
    pub async fn start(socket: impl AsRef<Path>, root: impl AsRef<Path>) -> io::Result<Self> {
        let socket = socket.as_ref().to_path_buf();
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root).await?;
        if socket.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("data socket already exists: {}", socket.display()),
            ));
        }
        let listener = UnixListener::bind(&socket)?;
        let stop = CancellationToken::new();
        let child_stop = stop.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = child_stop.cancelled() => break,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => { let root = root.clone(); tokio::spawn(async move { let _ = serve_file(stream, root).await; }); }
                        Err(err) => { eprintln!("data socket accept failed: {err}"); break; }
                    }
                }
            }
        });
        Ok(Self { socket, stop, task })
    }
}

impl Drop for DataServer {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket);
    }
}

async fn serve_file(stream: UnixStream, root: PathBuf) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let request: DataRequest = read_json_line(&mut reader).await?;
    let path = Path::new(&request.filename);
    if !is_plain_filename(path) {
        return write_header(
            reader.get_mut(),
            DataReply {
                length: None,
                error: Some("invalid data filename".into()),
            },
        )
        .await;
    }
    let full_path = root.join(path);
    let data = match tokio::fs::read(&full_path).await {
        Ok(data) => data,
        Err(err) => {
            return write_header(
                reader.get_mut(),
                DataReply {
                    length: None,
                    error: Some(err.to_string()),
                },
            )
            .await
        }
    };
    write_header(
        reader.get_mut(),
        DataReply {
            length: Some(data.len() as u64),
            error: None,
        },
    )
    .await?;
    reader.get_mut().write_all(&data).await?;
    reader.get_mut().flush().await
}

async fn write_header(stream: &mut UnixStream, header: DataReply) -> io::Result<()> {
    write_json_line(stream, &header).await
}

pub async fn fetch_file(socket: impl AsRef<Path>, filename: &str) -> io::Result<Vec<u8>> {
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        UnixStream::connect(socket),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "data socket connect timed out"))??;
    write_json_line(
        &mut stream,
        &DataRequest {
            filename: filename.to_owned(),
        },
    )
    .await?;
    let mut reader = BufReader::new(stream);
    let header: DataReply = read_json_line(&mut reader).await?;
    let Some(length) = header.length else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            header.error.unwrap_or_else(|| "data unavailable".into()),
        ));
    };
    let size = usize::try_from(length).map_err(invalid_data)?;
    let mut bytes = vec![0; size];
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        reader.read_exact(&mut bytes),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "data transfer timed out"))??;
    Ok(bytes)
}

pub async fn publish_file_atomically(path: &Path, data: &[u8]) -> io::Result<()> {
    let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("publishing-{id}"));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await?;
    file.write_all(data).await?;
    file.sync_all().await?;
    drop(file);
    if let Err(err) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(err);
    }
    Ok(())
}

pub async fn write_staging_file(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    file.write_all(data).await?;
    file.sync_all().await
}

pub fn is_plain_filename(path: &Path) -> bool {
    let mut components = path.components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

pub async fn write_json_line<T: Serialize>(stream: &mut UnixStream, value: &T) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(invalid_data)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    stream.flush().await
}

pub async fn read_json_line<R, T>(reader: &mut R) -> io::Result<T>
where
    R: AsyncBufRead + Unpin,
    T: DeserializeOwned,
{
    let mut line = String::new();
    let length = reader.read_line(&mut line).await?;
    if length == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "peer closed RPC",
        ));
    }
    serde_json::from_str(&line).map_err(invalid_data)
}

fn invalid_data(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transfers_binary_file_and_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("node");
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::write(root.join("part"), [0, 1, 255, 10])
            .await
            .unwrap();
        let socket = dir.path().join("data.sock");
        let _server = DataServer::start(&socket, &root).await.unwrap();
        assert_eq!(fetch_file(&socket, "part").await.unwrap(), [0, 1, 255, 10]);
        assert!(fetch_file(&socket, "../secret").await.is_err());
    }
}
