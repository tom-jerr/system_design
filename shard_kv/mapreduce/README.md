# Rust MapReduce

This crate implements the MapReduce master/worker flow with Tokio tasks and Unix domain sockets. The control plane uses versioned, one-request-per-connection JSON Lines. Intermediate and reduce output files travel through a separate worker-local data socket as binary bytes; the coordinator publishes accepted reducer output atomically.

Build with `cargo build --bins`. Start a coordinator with `mrcoordinator SOCKET INPUT...`, then start workers with `mrworker APP SOCKET`. `APP` is `wc`, `indexer`, `grep:PATTERN`, `url_access`, `reverse_links`, `term_vector`, `inverted_index`, or `distributed_sort`. `MR_N_REDUCE` selects R (default 3). The coordinator writes `mr-out-0` through `mr-out-(R-1)` beside the socket. `mrsequential APP INPUT...` is the local reference runner.

The default task/worker lease is 10 seconds (`MR_TASK_LEASE_MS`); the registration reply gives the lease to each worker. Workers default to a 2-second heartbeat (`MR_HEARTBEAT_MS`), clamped to at most one third of the lease. A lease timeout may suspect a delayed worker, so task attempts are fenced with monotonically increasing attempt IDs. Stale attempts cannot publish outputs. Worker loss invalidates its local map manifests and requeues those map tasks; in-flight reducers are revoked and re-fetch current manifests. A task gets at most 10 attempts before the job fails.

Map and reduce callbacks retain the existing application semantics. Map output uses per-attempt filenames and is exposed only after all R partitions are written. Reduce output is staged by the coordinator and atomically renamed only after the current owner/attempt is revalidated.

This is a single-host multi-process prototype. A real multi-host cluster needs a network RPC transport and shared or remotely addressable input storage. Master state is in memory and is not recovered after coordinator restart.

See [failure and state-machine design](docs/MAPREDUCE_FAILURE_DESIGN.md) for message contracts, complete transitions, lease boundaries, cancellation, and recovery details.

## Tests and large inputs

Run all tests with `cargo test`. The integration suite compares each distributed app (`wc`, `indexer`, `grep`, `url_access`, `reverse_links`, `term_vector`, `inverted_index`, `distributed_sort`) with `mrsequential`. It also creates four temporary `.txt` files with 20,000 lines each and uses `mrsequential` as the correctness anchor. Increase the lines per file with:

```sh
MR_LARGE_TEST_LINES=100000 cargo test --test mapreduce_correctness large_multi_file_word_count_matches_sequential_reference -- --exact --nocapture
```

The seven crash/recovery tests simulate process exit at these points: before Map execution; after writing the first Map partition; after Map output but before its completion RPC; after a committed Map's owner exits; before Reduce execution; after Reduce output but before commit; and while a reducer is pulling an early Map output. Run an individual scenario, for example:

```sh
cargo test --test mapreduce_correctness recovers_worker_crash_after_first_map_partition_write -- --exact --nocapture
```

Run the complete suite, including all seven crash scenarios, with:

```sh
cargo test --test mapreduce_correctness -- --test-threads=1 --nocapture
```

For reusable input files outside the test harness, generate a configurable corpus:

```sh
python3 tools/generate_test_inputs.py --outdir /tmp/mr-inputs --files 4 --lines 100000
```

The large integration test and generator use the same deterministic record pattern; generated test-run files are removed with their temporary directory.
