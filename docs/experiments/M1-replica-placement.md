# M1 — Replica placement: baseline behavior under hostile conditions

Status: experiments implemented, deterministic, and run. M0 remains the frozen baseline.
Raw output of every run below: [`M1-results.txt`](M1-results.txt). Reproduce with:

```sh
go run ./cmd/experiment            # all experiments (identical output on every run)
go run ./cmd/experiment -only C    # one experiment; also -chunks N, -seeds N
go test -race ./...
```

Two consecutive runs of the full experiment binary were compared byte-for-byte and are identical.

## 1. The baseline placement algorithm (derived from the code, not assumed)

Files: `internal/master/placement.go`, `master.go`, `replication_manager.go`.

**Interface.** `ReplicaPlacementPolicy.Choose(candidates []ServerID, exclude map[ServerID]bool, count int) ([]ServerID, error)`.
The placement abstraction already existed in M0 and the Master and ReplicationManager depend only on the interface, so no new abstraction was introduced for M1. The M0 implementation is `GFSReplicaPlacementPolicy`; the repo's convention is a `GFS*` prefix for baseline implementations, so it was **not** renamed to `BaselinePlacementPolicy` (a rename would touch every construction site for no behavioral gain). This document is what designates it "the baseline".

**Algorithm.** Remove excluded servers from `candidates`; error if fewer than `count` remain; `rand.Shuffle` the remainder using one `*rand.Rand` seeded from `Config.PlacementSeed`; return the first `count`.

**Where candidates come from.**
- Initial allocation (`Master.AllocateChunk`): every registered chunkserver the failure detector currently considers alive, sorted by ID; `exclude` is empty; `count` = replication factor.
- Re-replication (`GFSReplicationManager.CheckAndSchedule`): every alive server known to the failure detector, sorted by ID; `exclude` = servers already holding an alive replica of that chunk; `count` = RF minus alive replicas. One `Choose` call per under-replicated chunk. The copy source is the alphabetically first alive replica.

**What the policy considers, by inspection of the signature and call sites:**

| Factor | Considered? | Evidence |
|---|---|---|
| Node liveness | Yes, but *before* `Choose` (candidates are pre-filtered to alive nodes) | `onlineServerIDs`, `onlineCandidates` |
| Selection rule | Uniform random among candidates | `placement.go` |
| Capacity | **No** | `Choose` receives no capacity data; not reachable through the interface |
| Existing replica distribution / node load | **No** | same |
| Topology / racks / zones | **No** | same; no topology exists in the simulator |
| Network state, chunk hotness, chunk size | **No** | same |
| Sequencing | One RNG stream shared by *all* decisions (initial + re-replication) | a placement depends on how many draws preceded it |

Consequence of the signature: capacity/load awareness is not merely unimplemented, it is impossible for any policy behind this interface to have, because the data is never passed in. That is a fact about the interface, and is the main architectural observation M1 produces about placement.

## 2. Changes made to M0 (all additive; behavior-preserving except where stated)

1. **Determinism fix (real bug).** Candidate lists, chunk iteration order, and replica-location lists were built by ranging over Go maps, whose order is randomized per process. Because placement uses one shared seeded RNG, identical seeds gave different placements across runs. Every such site now sorts (`metadata_store.go`, `failure_detector.go`, `master.go`, `replication_manager.go`, `chunkserver.go`). Regression test: `TestPlacementIsDeterministicAcrossRuns`. Intentional side effect: the lease primary is now the alphabetically first alive replica instead of an arbitrary one. Placements for a given seed differ from pre-fix runs, which were not reproducible anyway.
2. **Recovery-duration metric wired up.** `Metrics.RecordRecovery` existed but was never called, and `Master` carried an unused `pendingFailureSince` field. Replaced by a recovery-incident timer, added `Metrics.RecoveryIncidents`, `Metrics.PlacementDecisions`, `Master.IsServerAlive`, `sim.NewWithCapacities`, `Simulator.AllocateChunkWithData`.
3. **Measurement correction found during M1.** `ReplicationManager.CheckAndSchedule` is documented as returning chunks "(still) under-replicated after the pass" but actually returns chunks that *needed repair when scanned*, whether or not the repair then succeeded. Consequently the `UnderReplicatedChunks` gauge reads 0 one tick after recovery really finished. My first experiment run reported recovery one tick after detection because of this; it was an artifact. M0's return value and gauge were **left unchanged** (M0 semantics preserved); `GFSReplicationManager.Unresolved()` was added as an additive accessor, the recovery timer uses it, and the experiments measure completion from the master's own chunk-location view.
4. `Snapshot()` in the experiment layer counts **online nodes only**; `RawSnapshot()` includes offline nodes (see finding F5).

Not modeled by the simulator, and therefore reported as unavailable rather than invented: time spent *copying* data during re-replication (copies are synchronous and complete within a scheduler pass unless a link delay is injected); bandwidth; topology/rack effects; per-message network cost beyond a message counter.

## 3. Method notes and caveats

- All runs use a fake clock, fixed seeds, and a 3 s heartbeat timeout ticked every 1.5 s. Durations are therefore quantized: detection always takes 3 ticks (4.5 s) because the gap must *exceed* 3 s. Detection time does not depend on which node died.
- Experiment A sweeps seeds 1–10; B2 and the C blast-radius line sweep seeds 1–10; **C's per-node detail and all of D use seed 1 and one kill order only**. Treat those as single observations.
- The experiment harness reads chunk locations through `GetChunkLocations`, which increments `MasterMetadataOps`; that metric is not reported for that reason.
- `imbalance` = max − min replicas per node. Standard deviation is reported alongside.

## 4. Results

### Experiment A — baseline distribution (120 chunks)

```
Experiment A: baseline distribution (seed=1 detail, then spread across seeds)
=============================================================================
chunks=120; replicas/node figures are per-node counts; imbalance = max-min

nodes  RF  | min    max    mean     imbalance stddev   | imbalance over seeds 1..10 (min/mean/max)
3      2   | 73     84     80.00    11        4.97     | 2 / 10.9 / 19
3      3   | 120    120    120.00   0         0.00     | 0 / 0.0 / 0
5      2   | 41     55     48.00    14        4.94     | 6 / 13.7 / 18
5      3   | 62     80     72.00    18        6.81     | 5 / 14.1 / 22
10     2   | 16     33     24.00    17        5.27     | 11 / 14.9 / 19
10     3   | 23     41     36.00    18        5.22     | 13 / 16.7 / 23

seed=1 per-node detail (nodes=10, RF=3):
  total=360 min=23 max=41 mean=36.00 imbalance=18 stddev=5.22 | CS1=40 CS10=41 CS2=40 CS3=38 CS4=23 CS5=33 CS6=34 CS7=40 CS8=33 CS9=38
  placement decisions recorded: 120
```

Observed: total replicas always equals chunks × RF (e.g. 360 for 120 × RF 3) and placement decisions equal chunks (120). With 3 nodes and RF 3 every node holds everything (imbalance 0 is trivial). For 10 nodes / RF 3, seed-1 stddev is 5.22; independent uniform draws (each chunk includes a given node with probability RF/N) would predict about 5.0. The observed spreads are of that order across configurations.

### Experiment B — uneven cluster conditions

```
Experiment B: uneven cluster conditions
=======================================

B1: capacity awareness (5 nodes, RF=3, 60 chunks x 16B; CS1,CS2 capacity=64B (4 chunks), others 1MiB)
  distribution (physically stored): total=111 min=4 max=37 mean=22.20 imbalance=33 stddev=14.99 | CS1=4 CS2=4 CS3=35 CS4=37 CS5=31
  chunks where a chosen replica could not physically fit: 51 of 60; nodes that overflowed: [CS1 CS2]
  after 3 heartbeat ticks: chunks whose master-listed replicas include a node that does not hold them: 51 of 60 (69 phantom replicas); chunks the master considers under-replicated: 0
  client read of the whole file succeeded: false read C5 from CS1: chunkserver CS1: no such chunk C5

B2: load awareness (CS1 pre-loaded with 500 unrelated chunks, then 60 fresh chunks allocated)
  before: total=500 min=0 max=500 mean=100.00 imbalance=500 stddev=200.00 | CS1=500 CS2=0 CS3=0 CS4=0 CS5=0
  after:  total=680 min=31 max=542 mean=136.00 imbalance=511 stddev=203.01 | CS1=542 CS2=35 CS3=35 CS4=37 CS5=31
  new replicas received by pre-loaded CS1: 42 (expected share if load-blind: ~36)
  across seeds 1..10: mean new replicas on the pre-loaded node = 37.3 (uniform expectation 36)

B3: previously failed and recovered node (6 nodes, RF=3, 60 chunks; kill CS1, re-replicate, recover CS1)
  CS1 physical chunks before kill=26, after recovery=26
  chunks over-replicated per master (alive replicas > RF): 26 of 60 (max replicas seen on one chunk: 4)
  chunks under-replicated: 0
  of a fresh batch of 60 chunks, replicas landing on recovered CS1: 30 (uniform expectation ~30)
```

- **B1 capacity.** Two nodes with room for 4 chunks each were chosen as replica locations for 51 of 60 chunks. Only 4 chunks physically landed on each. The master's own view afterwards: 0 chunks under-replicated, while 51 chunks list at least one replica that does not exist (69 such phantom replicas). A whole-file client read fails at the first affected chunk.
- **B2 load.** A node pre-loaded with 500 unrelated chunks received a normal share of new replicas: 42 in the seed-1 run, mean 37.3 over seeds 1–10, against a uniform expectation of 36.
- **B3 recovered node.** After node CS1 is killed, everything is re-replicated, and CS1 returns holding its old data, the master lists 26 of 60 chunks with 4 alive replicas (RF is 3). Nothing under-replicated. CS1 then received 30 of 60 fresh placements, matching a uniform expectation of ~30.

### Experiment C — targeted failure (10 nodes, RF 3)

```
Experiment C: targeted failure (10 nodes, RF=3)
===============================================
baseline: total=360 min=23 max=41 mean=36.00 imbalance=18 stddev=5.22 | CS1=40 CS10=41 CS2=40 CS3=38 CS4=23 CS5=33 CS6=34 CS7=40 CS8=33 CS9=38
heaviest=CS10 (41 replicas), lightest=CS4 (23 replicas)

  CS10 | affected=41  underRepl(peak)=41  reRepl start/ok/fail=41/41/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=23 max=41 mean=36.00 imbalance=18 stddev=5.22 | CS1=40 CS10=41 CS2=40 CS3=38 CS4=23 CS5=33 CS6=34 CS7=40 CS8=33 CS9=38
       post: total=360 min=32 max=48 mean=40.00 imbalance=16 stddev=4.00 | CS1=48 CS2=43 CS3=40 CS4=32 CS5=38 CS6=39 CS7=40 CS8=39 CS9=41
  CS4  | affected=23  underRepl(peak)=23  reRepl start/ok/fail=23/23/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=23 max=41 mean=36.00 imbalance=18 stddev=5.22 | CS1=40 CS10=41 CS2=40 CS3=38 CS4=23 CS5=33 CS6=34 CS7=40 CS8=33 CS9=38
       post: total=360 min=35 max=44 mean=40.00 imbalance=9 stddev=3.16 | CS1=44 CS10=41 CS2=43 CS3=42 CS5=35 CS6=37 CS7=43 CS8=36 CS9=39

replicas lost if the heaviest / lightest node dies, mean over seeds 1..10: 43.3 / 26.6
```

### Experiment D — cascading failures (10 nodes, RF 3, no recoveries)

```
Experiment D: cascading failures (10 nodes, RF=3; no node is ever recovered)
============================================================================

kill order: [CS2 CS4 CS1]
  CS2  | affected=40  underRepl(peak)=40  reRepl start/ok/fail=40/40/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=23 max=41 mean=36.00 imbalance=18 stddev=5.22 | CS1=40 CS10=41 CS2=40 CS3=38 CS4=23 CS5=33 CS6=34 CS7=40 CS8=33 CS9=38
       post: total=360 min=27 max=48 mean=40.00 imbalance=21 stddev=6.20 | CS1=48 CS10=44 CS3=46 CS4=27 CS5=36 CS6=36 CS7=44 CS8=37 CS9=42
  CS4  | affected=27  underRepl(peak)=27  reRepl start/ok/fail=27/27/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=27 max=48 mean=40.00 imbalance=21 stddev=6.20 | CS1=48 CS10=44 CS3=46 CS4=27 CS5=36 CS6=36 CS7=44 CS8=37 CS9=42
       post: total=360 min=38 max=54 mean=45.00 imbalance=16 stddev=5.10 | CS1=54 CS10=46 CS3=51 CS5=39 CS6=43 CS7=44 CS8=38 CS9=45
  CS1  | affected=54  underRepl(peak)=54  reRepl start/ok/fail=54/54/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=38 max=54 mean=45.00 imbalance=16 stddev=5.10 | CS1=54 CS10=46 CS3=51 CS5=39 CS6=43 CS7=44 CS8=38 CS9=45
       post: total=360 min=43 max=56 mean=51.43 imbalance=13 stddev=4.44 | CS10=53 CS3=56 CS5=43 CS6=56 CS7=52 CS8=47 CS9=53
  new replicas received (whole sequence): CS1=14 CS10=12 CS3=18 CS4=4 CS5=10 CS6=22 CS7=12 CS8=14 CS9=15

kill order: [CS2 CS4 CS1 CS3 CS5 CS6 CS7 CS8]
  CS2  | affected=40  underRepl(peak)=40  reRepl start/ok/fail=40/40/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=23 max=41 mean=36.00 imbalance=18 stddev=5.22 | CS1=40 CS10=41 CS2=40 CS3=38 CS4=23 CS5=33 CS6=34 CS7=40 CS8=33 CS9=38
       post: total=360 min=27 max=48 mean=40.00 imbalance=21 stddev=6.20 | CS1=48 CS10=44 CS3=46 CS4=27 CS5=36 CS6=36 CS7=44 CS8=37 CS9=42
  CS4  | affected=27  underRepl(peak)=27  reRepl start/ok/fail=27/27/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=27 max=48 mean=40.00 imbalance=21 stddev=6.20 | CS1=48 CS10=44 CS3=46 CS4=27 CS5=36 CS6=36 CS7=44 CS8=37 CS9=42
       post: total=360 min=38 max=54 mean=45.00 imbalance=16 stddev=5.10 | CS1=54 CS10=46 CS3=51 CS5=39 CS6=43 CS7=44 CS8=38 CS9=45
  CS1  | affected=54  underRepl(peak)=54  reRepl start/ok/fail=54/54/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=38 max=54 mean=45.00 imbalance=16 stddev=5.10 | CS1=54 CS10=46 CS3=51 CS5=39 CS6=43 CS7=44 CS8=38 CS9=45
       post: total=360 min=43 max=56 mean=51.43 imbalance=13 stddev=4.44 | CS10=53 CS3=56 CS5=43 CS6=56 CS7=52 CS8=47 CS9=53
  CS3  | affected=56  underRepl(peak)=56  reRepl start/ok/fail=56/56/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=43 max=56 mean=51.43 imbalance=13 stddev=4.44 | CS10=53 CS3=56 CS5=43 CS6=56 CS7=52 CS8=47 CS9=53
       post: total=360 min=49 max=69 mean=60.00 imbalance=20 stddev=6.71 | CS10=64 CS5=49 CS6=64 CS7=60 CS8=54 CS9=69
  CS5  | affected=49  underRepl(peak)=49  reRepl start/ok/fail=49/49/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=49 max=69 mean=60.00 imbalance=20 stddev=6.71 | CS10=64 CS5=49 CS6=64 CS7=60 CS8=54 CS9=69
       post: total=360 min=65 max=76 mean=72.00 imbalance=11 stddev=3.85 | CS10=74 CS6=74 CS7=71 CS8=65 CS9=76
  CS6  | affected=74  underRepl(peak)=74  reRepl start/ok/fail=74/74/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=65 max=76 mean=72.00 imbalance=11 stddev=3.85 | CS10=74 CS6=74 CS7=71 CS8=65 CS9=76
       post: total=360 min=80 max=99 mean=90.00 imbalance=19 stddev=6.75 | CS10=91 CS7=90 CS8=80 CS9=99
  CS7  | affected=90  underRepl(peak)=90  reRepl start/ok/fail=90/90/0 | detect=4.5s (3 ticks) restored-to-RF=4.5s (3 ticks, achieved=true) finalUnder(master view)=0
       pre : total=360 min=80 max=99 mean=90.00 imbalance=19 stddev=6.75 | CS10=91 CS7=90 CS8=80 CS9=99
       post: total=360 min=120 max=120 mean=120.00 imbalance=0 stddev=0.00 | CS10=120 CS8=120 CS9=120
  CS8  | affected=120 underRepl(peak)=120 reRepl start/ok/fail=0/0/5760 | detect=4.5s (3 ticks) restored-to-RF=1m15s (50 ticks, achieved=false) finalUnder(master view)=120
       pre : total=360 min=120 max=120 mean=120.00 imbalance=0 stddev=0.00 | CS10=120 CS8=120 CS9=120
       post: total=240 min=120 max=120 mean=120.00 imbalance=0 stddev=0.00 | CS10=120 CS9=120
  new replicas received (whole sequence): CS1=14 CS10=79 CS3=18 CS4=4 CS5=16 CS6=40 CS7=50 CS8=87 CS9=82
```

## 5. Findings

Each finding is split into what was measured, how I interpret it, and what it might motivate. Only the first is established by M1.

**F1 — Placement ignores capacity and existing load.**
*Observed:* by interface (section 1) and by experiment: small nodes were chosen for 51/60 chunks; a pre-loaded node received 37.3 vs 36 expected new replicas.
*Interpretation:* the policy is behaving exactly as a uniform random policy should; there is nothing to "fix" inside it, since the information isn't available to it.
*Potential direction:* any experimental policy needs an interface that passes node state. That is an interface change, not just a new implementation.

**F2 — The master cannot see replicas that were never stored (phantom replicas).**
*Observed:* in B1 the master reports 0 under-replicated chunks while 69 listed replicas do not exist, after 3 heartbeats. From reading the code: `AllocateChunk` records locations before any store happens, heartbeats only report chunks a node holds and never remove a listed location for an unreported chunk, and the replication manager counts metadata locations only. `Client.Read` tries only the first listed replica; I did not measure whether alternate replicas of the failing chunk existed.
*Interpretation:* placement commitment and physical storage are not reconciled in M0, so blind placement can silently reduce real durability below RF.
*Potential direction:* placement-time capacity feedback and/or heartbeat reconciliation. Not designed here.

**F3 — Recovery in this simulator is instantaneous once detected, and failure consequences scale with node load.**
*Observed:* in all 10 successful rounds, restoration to RF happened in the same pass as detection. Re-replication operations equal replicas lost, one for one (e.g. 41/41 for the heaviest node, 23/23 for the lightest). Across seeds 1–10 the heaviest node holds a mean 43.3 replicas and the lightest 26.6.
*Interpretation:* placement makes the *volume* of recovery work differ by which node dies (roughly 1.6× here) but not its duration, because copy time isn't modeled. Whether volume differences would translate into duration differences is untested.

**F4 — Cascading failures amplify copy work but showed no growing imbalance.**
*Observed (seed 1, one kill order):* killing CS2, CS4, CS1, CS3, CS5, CS6, CS7 in turn required 390 successful re-replications, versus 248 replicas that were on those nodes originally, i.e. 142 (57%) more, because some replicas were re-created on nodes that later died. Per-round imbalance after recovery was 21, 16, 13, 20, 11, 19, then 0 (with 3 survivors and RF 3 every node holds everything); stddev/mean fell from about 0.145 initially to 0.05–0.16 without a monotonic trend.
*Interpretation:* repeated movement of the same data is real but inherent to sequential failures with immediate re-replication; this experiment does not show it to be a placement weakness. There is no evidence here of increasing imbalance or concentration; that conclusion covers only this seed and order.

**F5 — Failed nodes are never garbage-collected; over-replication is never corrected.**
*Observed:* a killed node's chunk data stays in its local store untouched (`TestFailedNodeDataIsNotGarbageCollected`); when it returns with current-version data the master re-adds those locations, giving chunks with 4 replicas (B3). From reading the code, nothing removes replicas above RF.
*Interpretation:* storage use grows with each failure/recovery cycle in M0. Real GFS handles this with garbage collection; M0 has none.

**F6 — When survivors < RF, re-replication retries forever with no backoff.**
*Observed:* with 2 of 10 nodes left and RF 3, every one of the 120 chunks stayed at 2 replicas, and 5760 failed attempts were recorded in 48 passes (= 120 chunks × 48 passes).
*Interpretation:* RF is unattainable there by construction, so the shortfall is not a placement flaw; the unbounded per-pass retry is a separate observable behavior.

## 6. What surprised us
1. The determinism bug: seeded placement was not reproducible.
2. The recovery gauge reads one tick late; my first results were wrong until I checked why recovery was always exactly one tick after detection.
3. The scale of phantom replicas (51 of 60 chunks) and that the master's own metrics show a perfectly healthy cluster.
4. That the load-awareness experiment was as uneventful as the interface predicted (37.3 vs 36).

## 7. Not tested
Different chunk counts (how imbalance scales with load), multiple kill orders or seeds for D, simultaneous failures, failures during writes (staleness interactions), non-uniform network delay, and any policy other than the baseline.

## 8. M0 regression status
`go vet`, `go test ./...` and `go test -race ./...` pass (master, sim, experiment packages). All original M0 tests pass unmodified. The M0 demo (`go run ./cmd/demo`) produces the same headline outcomes before and after M1 (2 reads / 1 write successful, 2 re-replications, 1 stale replica detected, 0 under-replicated at end); individual replica choices differ because of the determinism fix.
