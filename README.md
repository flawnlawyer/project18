# Project18 — GFS Baseline Simulator (M0)

A modular, observable simulation of the classical Google File System
architecture: Client → Master → Chunkservers, with configurable chunking,
replication, heartbeats, failure detection, re-replication, leases, stale
version detection, and master persistence (operation log + checkpoint).

This is milestone **M0**: "how does GFS behave?" Nothing here improves on
GFS yet — that's deliberately out of scope until M0 is solid, per the
project brief.

## Running it

```sh
go run ./cmd/demo      # scripted walkthrough of the full M0 sequence
go test ./...          # unit + integration tests (deterministic, no sleeps)
go test -race ./...    # same, with the race detector
```

`cmd/demo/main.go` creates a file, writes/reads it, runs steady-state
heartbeats, kills a chunkserver, watches failure detection and
re-replication happen, recovers the node, injects a stale replica, then
checkpoints/kills/restarts the master and shows metadata survives. It ends
by dumping the full event log and final metrics — read that log top to
bottom to see *why* the master did what it did.

## Layout

```
internal/
  common/       ChunkHandle, ServerID, Path, ChunkVersion, Clock (+ FakeClock
                for deterministic tests)
  events/       Log — the structured event log every component writes to
  metrics/      Metrics — counters for reads/writes/replication/detection/etc.
  network/      NetworkModel — in-process call routing with injectable
                per-link delay/drop (no real sockets; see "Why no real
                networking" below)
  chunkserver/  StorageNode interface + ChunkServer (replicas, versions,
                online/offline state, capacity)
  master/       MetadataStore, ReplicaPlacementPolicy, FailureDetector,
                LeaseManager, ReplicationManager, Scheduler — each its own
                interface + one GFS-baseline implementation — wired together
                by Master. persistence.go has the operation log + checkpoint.
  client/       Client — talks to Master for metadata, directly to
                chunkservers for data (never routes bytes through the
                master)
  sim/          Simulator — wires everything together, drives heartbeats via
                Tick(), and exposes the failure-injection API
cmd/demo/       scripted end-to-end walkthrough
```

## Replaceable interfaces

Per the project's core rule, every major subsystem is an interface with one
baseline implementation, so a later experiment can swap e.g.
`GFSReplicaPlacementPolicy` for `Project18AdaptivePlacementPolicy` without
touching anything else:

| Interface | Baseline impl | File |
|---|---|---|
| `MetadataStore` | `InMemoryMetadataStore` | `master/metadata_store.go` |
| `ReplicaPlacementPolicy` | `GFSReplicaPlacementPolicy` (random) | `master/placement.go` |
| `FailureDetector` | `HeartbeatFailureDetector` (timeout-based) | `master/failure_detector.go` |
| `LeaseManager` | `GFSLeaseManager` (lease-on-demand) | `master/lease_manager.go` |
| `ReplicationManager` | `GFSReplicationManager` | `master/replication_manager.go` |
| `Scheduler` | `PeriodicScheduler` | `master/scheduler.go` |
| `StorageNode` | `ChunkServer` | `chunkserver/chunkserver.go` |
| `NetworkModel` | `InProcessNetwork` | `network/network.go` |

## Failure injection

All on `sim.Simulator`: `KillChunkServer`, `RecoverChunkServer`, `KillMaster`,
`RecoverMaster`, `DropHeartbeat`/`RestoreHeartbeat`, `DelayHeartbeat`,
`SimulateNetworkDelay`, `CreateStaleReplica`, `SimulateReplicationFailure`.
These are controlled, named experiments, not random chaos, per the brief.

## Why no real networking

Client/Master/Chunkserver run in one process; every interaction between them
goes through `NetworkModel.Call(from, to, fn)`. That's the seam where
delay/drop injection lives. Building real sockets/RPC would add
infrastructure work without adding architectural fidelity — the brief is
explicit that this is a simulation, and that real networking should only get
built "if necessary for a particular experiment."

## Deliberate M0 simplifications (not gaps we forgot about)

- **Flat namespace.** Files are just `Path -> []ChunkHandle`; no directories.
  Nothing in the M0 milestone needs a directory tree.
- **Chunk size is tiny by default** (`Config.ChunkSize`, default 64 bytes,
  not 64MB) so the demo/tests exercise multi-chunk files without allocating
  real memory per the "represent chunks as metadata/state objects" note in
  the brief.
- **Master restart preserves chunkserver liveness state.** Real GFS has the
  restarted master re-poll every chunkserver for its chunk set before
  trusting anything. Here, `Master.Recover()` rebuilds file/chunk metadata
  from checkpoint + replayed op log (so that part is faithful), but does
  *not* reset the failure detector's heartbeat history, and it does
  explicitly clear all leases (matching real GFS, where lease state is
  never persisted). Modeling the full re-poll handshake is straightforward
  to add later but was left out of M0 rather than half-implemented.
- **No client-side write buffering/retry.** `Client.Write` pushes each piece
  to the primary, which pushes to secondaries, synchronously, once. Real GFS
  has a separate data-push phase and a retryable mutation-commit phase; we
  model the *ordering* guarantee (primary-coordinates-secondaries) without
  the full control-flow.

## Reference repos

`uttam-li/dfs`, `merrymercy/goGFS`, and `pixperk/juzfs` were reviewed for
architectural patterns (manager decomposition, lease-on-demand + version
bump, operation-log/checkpoint persistence). No code was copied
## Next (not started — stop-point per the brief)

M0 demonstrates the milestone sequence end-to-end with tests. Nothing beyond
that has been started: no adaptive placement, no consensus-based master
HA, no real networking. Those are exactly the things Project18 exists to
experiment with once M0 is trusted as the baseline.
