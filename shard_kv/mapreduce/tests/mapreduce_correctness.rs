//! End-to-end correctness checks modeled on 6.5840's TestWc and TestIndexer.
//!
//! The implementation under test provides three executables with the same
//! argument contract as the lab: `mrsequential APP INPUT...`,
//! `mrcoordinator SOCKET INPUT...`, and `mrworker APP SOCKET`. The executable
//! paths can be overridden with MR_SEQUENTIAL, MR_COORDINATOR, and MR_WORKER.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct TestDir(PathBuf);

impl TestDir {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("mr-rust-{label}-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).expect("create test directory");
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn executable(env_name: &str, default: &str) -> PathBuf {
    std::env::var_os(env_name)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target/debug")
                .join(default)
        })
}

fn inputs(dir: &Path, app: &str) -> Vec<PathBuf> {
    let examples: Vec<(&str, &str)> = match app {
        "wc" | "indexer" => vec![
            ("doc-a.txt", "Rust makes systems reliable. Rust makes systems fast!\n"),
            ("doc-b.txt", "Systems need tests; reliable tests.\n"),
            ("doc-c.txt", "Fast systems use Rust?\n"),
        ],
        "grep:Rust" => vec![
            ("grep-a.txt", "Rust is distributed.\nGo is also distributed.\n"),
            ("grep-b.txt", "Rust makes systems reliable.\n"),
        ],
        "url_access" => vec![
            ("access-a.log", "127.0.0.1 - - [date] \"GET /home HTTP/1.1\" 200\n127.0.0.2 - - [date] \"GET /about HTTP/1.1\" 200\n"),
            ("access-b.log", "127.0.0.3 - - [date] \"GET /home HTTP/1.1\" 200\n"),
        ],
        "reverse_links" => vec![
            ("https___a.test", "https://b.test/page https://c.test/\n"),
            ("https___b.test", "https://c.test/ https://a.test/\n"),
        ],
        "term_vector" => vec![
            ("doc-a@news.test", "rust rust systems cloud\n"),
            ("doc-b@news.test", "rust systems storage\n"),
            ("doc-c@blog.test", "rust rust systems\n"),
        ],
        "inverted_index" => vec![
            ("doc-a.txt", "rust systems rust\n"),
            ("doc-b.txt", "systems storage\n"),
            ("doc-c.txt", "rust storage\n"),
        ],
        "distributed_sort" => vec![
            ("records-a.txt", "c 3\na 1\n"),
            ("records-b.txt", "b 2\na 4\n"),
        ],
        _ => panic!("no local fixtures for application {app}"),
    };
    examples
        .into_iter()
        .map(|(name, text)| {
            let path = dir.join(name);
            fs::write(&path, text).expect("write fixture input");
            path
        })
        .collect()
}

fn run_sequential(dir: &Path, app: &str, files: &[PathBuf]) -> io::Result<Vec<String>> {
    let status = Command::new(executable("MR_SEQUENTIAL", "mrsequential"))
        .arg(app)
        .args(files)
        .current_dir(dir)
        .status()?;
    assert!(status.success(), "mrsequential failed for {app}");
    let output = dir.join("mr-out-0");
    assert!(output.exists(), "mrsequential did not create mr-out-0");
    let lines = read_lines(&output)?;
    fs::remove_file(output)?;
    Ok(lines)
}

fn read_lines(path: &Path) -> io::Result<Vec<String>> {
    let content = fs::read_to_string(path)?;
    let mut lines: Vec<_> = content.lines().map(str::to_owned).collect();
    lines.sort();
    Ok(lines)
}

fn wait_for_socket(socket: &Path, coordinator: &mut Child) {
    for _ in 0..100 {
        if socket.exists() {
            return;
        }
        if let Some(status) = coordinator.try_wait().expect("poll coordinator") {
            panic!("coordinator exited before creating socket: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("coordinator did not create socket {}", socket.display());
}

fn run_distributed(
    dir: &Path,
    app: &str,
    files: &[PathBuf],
    worker_count: usize,
) -> io::Result<Vec<String>> {
    let socket = dir.join("coordinator.sock");
    let mut coordinator = Command::new(executable("MR_COORDINATOR", "mrcoordinator"))
        .arg(&socket)
        .args(files)
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    wait_for_socket(&socket, &mut coordinator);

    let mut workers = Vec::new();
    for _ in 0..worker_count {
        workers.push(
            Command::new(executable("MR_WORKER", "mrworker"))
                .arg(app)
                .arg(&socket)
                .current_dir(dir)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
    }

    let coord_status = coordinator.wait()?;
    assert!(coord_status.success(), "mrcoordinator failed");
    for worker in &mut workers {
        let status = worker.wait()?;
        assert!(status.success(), "mrworker failed");
    }

    let mut output_files: Vec<_> = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                n.starts_with("mr-out-") && n[7..].chars().all(|c| c.is_ascii_digit())
            })
        })
        .collect();
    output_files.sort();
    assert!(
        !output_files.is_empty(),
        "reducers created no mr-out-X output files"
    );
    let mut lines = Vec::new();
    for path in output_files {
        lines.extend(read_lines(&path)?);
    }
    lines.sort();
    Ok(lines)
}

fn check_application(app: &str, workers: usize, label: &str) -> io::Result<()> {
    let dir = TestDir::new(label);
    let files = inputs(dir.path(), app);
    let expected = run_sequential(dir.path(), app, &files)?;
    let actual = run_distributed(dir.path(), app, &files, workers)?;
    assert_eq!(
        actual, expected,
        "incorrect combined reduce output for {app}"
    );
    Ok(())
}

fn large_inputs(dir: &Path, file_count: usize, lines_per_file: usize) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::with_capacity(file_count);
    for file_id in 0..file_count {
        let path = dir.join(format!("bulk-{file_id:02}.txt"));
        let mut writer = std::io::BufWriter::new(fs::File::create(&path)?);
        for line_id in 0..lines_per_file {
            writeln!(
                writer,
                "rust common common shard{file_id} term{} payload",
                line_id % 257
            )?;
        }
        writer.flush()?;
        files.push(path);
    }
    Ok(files)
}

#[test]
fn word_count_matches_sequential_reference() -> io::Result<()> {
    check_application("wc", 3, "wc")
}

#[test]
fn indexer_matches_sequential_reference() -> io::Result<()> {
    check_application("indexer", 2, "indexer")
}

#[test]
fn distributed_grep_matches_sequential_reference() -> io::Result<()> {
    check_application("grep:Rust", 2, "grep")
}

#[test]
fn url_access_counts_match_sequential_reference() -> io::Result<()> {
    check_application("url_access", 2, "url-access")
}

#[test]
fn reverse_link_graph_matches_sequential_reference() -> io::Result<()> {
    check_application("reverse_links", 2, "reverse-links")
}

#[test]
fn host_term_vectors_match_sequential_reference() -> io::Result<()> {
    check_application("term_vector", 2, "term-vector")
}

#[test]
fn inverted_index_matches_sequential_reference() -> io::Result<()> {
    check_application("inverted_index", 2, "inverted-index")
}

#[test]
fn distributed_sort_matches_sequential_reference() -> io::Result<()> {
    check_application("distributed_sort", 2, "distributed-sort")
}

#[test]
fn large_multi_file_word_count_matches_sequential_reference() -> io::Result<()> {
    let dir = TestDir::new("large-multi-file");
    let lines_per_file = std::env::var("MR_LARGE_TEST_LINES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20_000);
    let files = large_inputs(dir.path(), 4, lines_per_file)?;
    let expected = run_sequential(dir.path(), "wc", &files)?;
    let actual = run_distributed(dir.path(), "wc", &files, 4)?;
    assert_eq!(
        actual, expected,
        "large multi-file output differs from mrsequential"
    );
    Ok(())
}

fn launch_coordinator(dir: &Path, files: &[PathBuf], n_reduce: usize) -> io::Result<Child> {
    Command::new(executable("MR_COORDINATOR", "mrcoordinator"))
        .arg(dir.join("coordinator.sock"))
        .args(files)
        .current_dir(dir)
        .env("MR_N_REDUCE", n_reduce.to_string())
        .env("MR_TASK_LEASE_MS", "600")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
}

fn launch_worker(
    dir: &Path,
    app: &str,
    crash_point: Option<&str>,
    map_delay_ms: Option<u64>,
) -> io::Result<Child> {
    let mut command = Command::new(executable("MR_WORKER", "mrworker"));
    command
        .arg(app)
        .arg(dir.join("coordinator.sock"))
        .current_dir(dir)
        .env("MR_HEARTBEAT_MS", "100")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    if let Some(point) = crash_point {
        command.env("MR_CRASH_POINT", point);
    }
    if let Some(delay) = map_delay_ms {
        command.env("MR_TEST_MAP_DELAY_MS", delay.to_string());
    }
    command.spawn()
}

fn assert_distributed_matches(dir: &Path, expected: &[String]) -> io::Result<()> {
    let mut files: Vec<_> = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                n.starts_with("mr-out-") && n[7..].chars().all(|c| c.is_ascii_digit())
            })
        })
        .collect();
    files.sort();
    assert!(!files.is_empty(), "reducers created no output files");
    let mut actual = Vec::new();
    for file in files {
        actual.extend(read_lines(&file)?);
    }
    actual.sort();
    assert_eq!(
        actual, expected,
        "output differs after worker crash recovery"
    );
    Ok(())
}

fn crash_during_stage(app: &str, point: &str, label: &str) -> io::Result<()> {
    let dir = TestDir::new(label);
    let files = inputs(dir.path(), app);
    let expected = run_sequential(dir.path(), app, &files)?;
    let mut coordinator = launch_coordinator(dir.path(), &files, 3)?;
    wait_for_socket(&dir.path().join("coordinator.sock"), &mut coordinator);

    let mut crashed = launch_worker(dir.path(), app, Some(point), None)?;
    let status = crashed.wait()?;
    assert_eq!(
        status.code(),
        Some(86),
        "failpoint {point} did not simulate a crash"
    );

    let mut workers = Vec::new();
    for _ in 0..3 {
        workers.push(launch_worker(dir.path(), app, None, None)?);
    }
    let status = coordinator.wait()?;
    assert!(status.success(), "coordinator failed after {point}");
    for worker in &mut workers {
        assert!(worker.wait()?.success(), "replacement worker failed");
    }
    assert_distributed_matches(dir.path(), &expected)
}

#[test]
fn recovers_worker_crash_before_map_execution() -> io::Result<()> {
    crash_during_stage("wc", "before_map", "crash-before-map")
}

#[test]
fn recovers_worker_crash_after_first_map_partition_write() -> io::Result<()> {
    crash_during_stage("wc", "map_after_first_partition", "crash-partial-map")
}

#[test]
fn recovers_worker_crash_after_map_output_before_completion_rpc() -> io::Result<()> {
    crash_during_stage("wc", "after_map_write", "crash-map-before-ack")
}

#[test]
fn reruns_map_when_its_committed_worker_node_dies() -> io::Result<()> {
    crash_during_stage("wc", "after_map_commit", "crash-map-owner-after-commit")
}

#[test]
fn recovers_worker_crash_before_reduce_execution() -> io::Result<()> {
    crash_during_stage("wc", "before_reduce", "crash-before-reduce")
}

#[test]
fn recovers_worker_crash_after_reduce_output_before_commit() -> io::Result<()> {
    crash_during_stage("wc", "after_reduce_write", "crash-reduce-before-commit")
}

#[test]
fn recovers_crash_while_reducer_pulls_early_map_output() -> io::Result<()> {
    let dir = TestDir::new("crash-during-pull");
    let files = [dir.path().join("map-a.txt"), dir.path().join("map-b.txt")];
    fs::write(&files[0], "the slow map has a unique token\n")?;
    fs::write(&files[1], "the fast map also has a token\n")?;
    let expected = run_sequential(dir.path(), "wc", &files)?;
    let mut coordinator = launch_coordinator(dir.path(), &files, 1)?;
    wait_for_socket(&dir.path().join("coordinator.sock"), &mut coordinator);

    // Keep map 0 running while map 1 completes. The second worker receives a
    // reducer task, pulls map 1's file, then exits without reporting progress.
    let mut slow_map_worker = launch_worker(dir.path(), "wc", None, Some(1_500))?;
    thread::sleep(Duration::from_millis(150));
    let mut crashed_reducer =
        launch_worker(dir.path(), "wc", Some("reduce_after_first_pull"), None)?;
    assert_eq!(
        crashed_reducer.wait()?.code(),
        Some(86),
        "reducer never pulled a completed map output"
    );

    let mut replacement = launch_worker(dir.path(), "wc", None, None)?;
    assert!(
        coordinator.wait()?.success(),
        "coordinator failed after reducer pull crash"
    );
    assert!(slow_map_worker.wait()?.success(), "slow map worker failed");
    assert!(
        replacement.wait()?.success(),
        "replacement reducer worker failed"
    );
    assert_distributed_matches(dir.path(), &expected)
}
