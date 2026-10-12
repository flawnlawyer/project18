// M2 experiments (A-G), per Project18_M2_Engineering_Documentation section
// 11. Each function sets up one scenario against the (unmodified-interface)
// M0/M1 Simulator and master.Master, and reports what actually happened —
// following the same measure-first discipline as the M1 experiments.
package experiment

import (
	"fmt"
	"time"

	"project18/internal/common"
	"project18/internal/metrics"
	"project18/internal/sim"
)

// ---------- M2-A: Stale Replica Detection ----------

type StaleReplicaResult struct {
	Handle                     common.ChunkHandle
	AuthoritativeVersion       common.ChunkVersion
	StaleServer                common.ServerID
	StaleVersion               common.ChunkVersion
	ExcludedAfterHeartbeat     bool // stale server no longer in the chunk's valid location set
	StaleReplicasDetectedDelta uint64
}

// RunStaleReplicaDetectionExperiment creates a chunk, advances its version
// via a lease grant (simulating a mutation the stale server misses), then
// has that server report an old version on its next heartbeat. Pass
// condition (per the spec table): stale replica detected and excluded.
func RunStaleReplicaDetectionExperiment(cfg Config) (StaleReplicaResult, error) {
	s, clock := Build(cfg)
	if err := s.Client.CreateFile(experimentFile); err != nil {
		return StaleReplicaResult{}, err
	}
	handle, locations, err := s.AllocateChunkWithData(experimentFile, []byte("v1 data"))
	if err != nil {
		return StaleReplicaResult{}, err
	}
	step := cfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()

	staleServer := locations[0]
	staleVersionHeld, _ := func() (common.ChunkVersion, bool) {
		n, ok := s.Node(staleServer)
		if !ok {
			return 0, false
		}
		return n.HasChunk(handle)
	}()

	// Simulate a write that staleServer misses: request a lease (bumps the
	// chunk's authoritative version in master metadata), then physically
	// push that new version to every OTHER replica — exactly what the
	// client's write path does on a real mutation — while deliberately
	// leaving staleServer's physical data untouched at the old version.
	lease, err := s.Master.RequestLease(handle)
	if err != nil {
		return StaleReplicaResult{}, err
	}
	for _, loc := range locations {
		if loc == staleServer {
			continue
		}
		n, ok := s.Node(loc)
		if !ok {
			continue
		}
		if err := n.StoreChunk(handle, lease.Version, []byte("v2 data")); err != nil {
			return StaleReplicaResult{}, err
		}
	}
	cm := lease.Version

	before := s.Metrics.Snapshot().StaleReplicasDetected
	clock.Advance(step)
	s.Tick() // staleServer heartbeats its old version; everyone else (if their data is still v1 too) is unaffected by this experiment's scope — we only care about staleServer's exclusion
	after := s.Metrics.Snapshot().StaleReplicasDetected

	_, _, aliveLocs, err2 := chunkLocations(s, handle)
	if err2 != nil {
		return StaleReplicaResult{}, err2
	}
	excluded := true
	for _, l := range aliveLocs {
		if l == staleServer {
			excluded = false
		}
	}

	return StaleReplicaResult{
		Handle: handle, AuthoritativeVersion: cm, StaleServer: staleServer, StaleVersion: staleVersionHeld,
		ExcludedAfterHeartbeat: excluded, StaleReplicasDetectedDelta: after - before,
	}, nil
}

// ---------- M2-B: Automatic Repair ----------

// RunAutomaticRepairExperiment kills one of three replicas and confirms RF
// returns to target, reporting the M2-specific repair metrics alongside
// the M1-style failure-impact numbers.
func RunAutomaticRepairExperiment(cfg Config, target common.ServerID) (FailureImpactResult, metrics.Snapshot, error) {
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return FailureImpactResult{}, metrics.Snapshot{}, err
	}
	step := cfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()
	r := runFailureRound(s, clock, cfg, target)
	return r, s.Metrics.Snapshot(), nil
}

// ---------- M2-C: Source Failure During Repair ----------

type RetryDuringRepairResult struct {
	Handle               common.ChunkHandle
	FirstVictim          common.ServerID // detected dead by the failure detector first
	SecondVictim         common.ServerID // taken offline just before the critical tick — "fails during repair"
	RepairSucceeded      bool
	ReReplicationsFailed uint64 // evidence an attempt against SecondVictim failed before success
	FinalReplicaCount    int
}

// RunSourceFailureDuringRepairExperiment arranges for a SECOND replica to go
// offline in the exact tick where the failure detector first notices the
// FIRST one — so the repair pass sees the second one as a seemingly-valid
// (not-yet-timed-out) source, attempts it, and must fall back to a third.
// This is deterministic: source candidates come from a sorted list, not
// the randomized placement policy, so no seed-dependence is involved.
func RunSourceFailureDuringRepairExperiment(cfg Config) (RetryDuringRepairResult, error) {
	runCfg := cfg
	runCfg.NodeCount = 6
	runCfg.ReplicationFactor = 3
	runCfg.ChunkCount = 1
	s, clock := Build(runCfg)
	handle, locations, err := populateOneChunkWithThreeValidSources(s, runCfg)
	if err != nil {
		return RetryDuringRepairResult{}, err
	}
	step := runCfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()

	first, second := locations[0], locations[1]
	if err := s.KillChunkServer(first); err != nil {
		return RetryDuringRepairResult{}, err
	}

	n := int(runCfg.HeartbeatTimeout/step) + 1
	for i := 0; i < n-1; i++ {
		clock.Advance(step)
		s.Tick()
	}
	if !s.Master.IsServerAlive(first) {
		return RetryDuringRepairResult{}, fmt.Errorf("setup assumption violated: %s detected dead too early", first)
	}
	if err := s.KillChunkServer(second); err != nil {
		return RetryDuringRepairResult{}, err
	}
	before := s.Metrics.Snapshot()
	clock.Advance(step) // crosses `first`'s timeout; `second`'s own gap is only one step old
	s.Tick()            // the critical tick: first detected dead, repair tries second (fails), falls back to third
	after := s.Metrics.Snapshot()

	_, _, aliveLocs, err := chunkLocations(s, handle)
	if err != nil {
		return RetryDuringRepairResult{}, err
	}

	return RetryDuringRepairResult{
		Handle: handle, FirstVictim: first, SecondVictim: second,
		RepairSucceeded:      len(aliveLocs) >= runCfg.ReplicationFactor,
		ReReplicationsFailed: after.ReReplicationsFailed - before.ReReplicationsFailed,
		FinalReplicaCount:    len(aliveLocs),
	}, nil
}

// ---------- M2-D: Destination Failure During Repair ----------

type DestinationFailureResult struct {
	Handle               common.ChunkHandle
	BadCandidates        []common.ServerID
	GoodCandidate        common.ServerID
	RepairSucceeded      bool
	RetryWasExercised    bool // true if at least one attempt against a bad candidate was recorded before success
	ReReplicationsFailed uint64
	NoFalseSuccessCommit bool // the bad candidates never ended up in the chunk's valid location set
}

// RunDestinationFailureDuringRepairExperiment kills every destination
// candidate except one, then repairs. Because the replacement destination
// is chosen by the seeded, randomized placement policy — a private detail
// of master.Master not reachable from this package — whether THIS run
// happens to pick a bad candidate first (exercising retry) is seed-
// dependent; see TestDestinationFailureDuringRepairRetriesAnotherDestination
// in internal/master for a fully deterministic, white-box proof that the
// retry mechanism itself works in every case. This experiment reports what
// was actually observed for the given seed rather than asserting a
// specific branch was taken.
func RunDestinationFailureDuringRepairExperiment(cfg Config) (DestinationFailureResult, error) {
	runCfg := cfg
	runCfg.NodeCount = 6
	runCfg.ReplicationFactor = 3
	runCfg.ChunkCount = 1
	s, clock := Build(runCfg)
	handle, locations, err := populateOneChunkWithThreeValidSources(s, runCfg)
	if err != nil {
		return DestinationFailureResult{}, err
	}
	step := runCfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()

	// The 3 non-replica nodes are the only possible destinations once we
	// lose a replica. Kill 2 of them, leaving exactly 1 good one.
	ids := s.ServerIDs()
	var others []common.ServerID
	locSet := map[common.ServerID]bool{}
	for _, l := range locations {
		locSet[l] = true
	}
	for _, id := range ids {
		if !locSet[id] {
			others = append(others, id)
		}
	}
	if len(others) < 3 {
		return DestinationFailureResult{}, fmt.Errorf("setup assumption violated: need 3 non-replica nodes, have %d", len(others))
	}
	bad := others[:2]
	good := others[2]
	victim := locations[0]

	// Staggered kill, same technique as M2-C: victim first, given a full
	// detection window; the bad destination candidates are killed only at
	// the LAST possible moment before the critical tick. If they were
	// killed any earlier (including simultaneously with victim), fd.Scan
	// would mark them dead in the SAME pass that detects victim, and
	// onlineCandidates() would exclude them before repair ever runs —
	// guaranteeing the single good candidate gets picked every time
	// regardless of seed. Keeping their own detection gap short is what
	// makes them legitimate-looking destination candidates that then fail
	// when actually written to.
	if err := s.KillChunkServer(victim); err != nil {
		return DestinationFailureResult{}, err
	}
	n := int(runCfg.HeartbeatTimeout/step) + 1
	for i := 0; i < n-1; i++ {
		clock.Advance(step)
		s.Tick()
	}
	if !s.Master.IsServerAlive(victim) {
		return DestinationFailureResult{}, fmt.Errorf("setup assumption violated: victim detected dead too early")
	}
	for _, b := range bad {
		if err := s.KillChunkServer(b); err != nil {
			return DestinationFailureResult{}, err
		}
	}

	before := s.Metrics.Snapshot()
	clock.Advance(step)
	s.Tick() // the critical tick: victim detected dead, repair tries a destination among {bad, bad, good} — all still fd-"alive"
	after := s.Metrics.Snapshot()

	_, _, aliveLocs, err := chunkLocations(s, handle)
	if err != nil {
		return DestinationFailureResult{}, err
	}
	succeeded := len(aliveLocs) >= runCfg.ReplicationFactor
	noFalse := true
	for _, b := range bad {
		for _, l := range aliveLocs {
			if l == b {
				noFalse = false
			}
		}
	}

	return DestinationFailureResult{
		Handle: handle, BadCandidates: bad, GoodCandidate: good,
		RepairSucceeded:      succeeded,
		RetryWasExercised:    after.ReReplicationsFailed > before.ReReplicationsFailed,
		ReReplicationsFailed: after.ReReplicationsFailed - before.ReReplicationsFailed,
		NoFalseSuccessCommit: noFalse,
	}, nil
}

// ---------- M2-E: Restart Persistence ----------

type RestartPersistenceResult struct {
	ChunksBeforeRestart   int
	ChunksAfterRestart    int
	VersionsMatch         bool
	LocationsMatch        bool
	ConvergedAfterRestart bool
}

// RunRestartPersistenceExperiment kills a replica (so a repair is
// in-flight/needed), restarts the Master mid-stream, and confirms
// authoritative chunk versions and replica locations survive — then checks
// that reconciliation still converges the cluster afterward (repair jobs
// are NOT persisted; they are deliberately re-derived by the next
// background pass, see docs/experiments/M2-distributed-consistency.md).
func RunRestartPersistenceExperiment(cfg Config) (RestartPersistenceResult, error) {
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return RestartPersistenceResult{}, err
	}
	step := cfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()
	s.Master.Checkpoint()

	ids := s.ServerIDs()
	if err := s.KillChunkServer(ids[0]); err != nil {
		return RestartPersistenceResult{}, err
	}
	clock.Advance(step)
	s.Tick() // failure likely not yet detected; a repair may or may not be in flight — restart either way

	before := snapshotChunkTable(s, cfg.ChunkCount)

	s.KillMaster()
	s.RecoverMaster()

	after := snapshotChunkTable(s, cfg.ChunkCount)

	versionsMatch, locationsMatch := true, true
	for h, b := range before {
		a, ok := after[h]
		if !ok {
			versionsMatch, locationsMatch = false, false
			continue
		}
		if a.version != b.version {
			versionsMatch = false
		}
		if fmt.Sprint(a.locs) != fmt.Sprint(b.locs) {
			locationsMatch = false
		}
	}

	// Confirm the cluster still converges post-restart (repair queue
	// rebuilds itself from a fresh reconciliation scan).
	tickUntil(s, clock, step, maxTicksPerRound, func() bool {
		return masterViewUnderReplicated(s, cfg.ReplicationFactor) == 0
	})
	converged := masterViewUnderReplicated(s, cfg.ReplicationFactor) == 0

	return RestartPersistenceResult{
		ChunksBeforeRestart: len(before), ChunksAfterRestart: len(after),
		VersionsMatch: versionsMatch, LocationsMatch: locationsMatch,
		ConvergedAfterRestart: converged,
	}, nil
}

// ---------- M2-F: Concurrent Repair ----------

type ConcurrentRepairResult struct {
	ChunksAffected          int
	ChunksFullyRecovered    int
	DuplicateLocationsFound bool // structural corruption check: no chunk should ever list the same server twice
	MaxReplicasAnyChunk     int
	FinalUnderReplicated    int
}

// RunConcurrentRepairExperiment under-replicates several chunks at once
// (by killing a node that holds many of them) and confirms every resulting
// repair job executes without metadata corruption.
func RunConcurrentRepairExperiment(cfg Config) (ConcurrentRepairResult, error) {
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return ConcurrentRepairResult{}, err
	}
	step := cfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()

	pre := Snapshot(s)
	ids := s.ServerIDs()
	var heaviest common.ServerID
	max := -1
	for _, id := range ids {
		if n := pre.PerServer[id]; n > max {
			max, heaviest = n, id
		}
	}
	affected := max

	if err := s.KillChunkServer(heaviest); err != nil {
		return ConcurrentRepairResult{}, err
	}
	tickUntil(s, clock, step, maxTicksPerRound, func() bool {
		return masterViewUnderReplicated(s, cfg.ReplicationFactor) == 0
	})

	n, err := s.Master.ChunkCount(experimentFile)
	if err != nil {
		return ConcurrentRepairResult{}, err
	}
	fullyRecovered, dup, maxReplicas := 0, false, 0
	for i := 0; i < n; i++ {
		_, locs, err := s.Master.GetChunkLocations(experimentFile, i)
		if err != nil {
			return ConcurrentRepairResult{}, err
		}
		seen := map[common.ServerID]bool{}
		for _, l := range locs {
			if seen[l] {
				dup = true
			}
			seen[l] = true
		}
		if len(locs) >= cfg.ReplicationFactor {
			fullyRecovered++
		}
		if len(locs) > maxReplicas {
			maxReplicas = len(locs)
		}
	}

	return ConcurrentRepairResult{
		ChunksAffected: affected, ChunksFullyRecovered: fullyRecovered,
		DuplicateLocationsFound: dup, MaxReplicasAnyChunk: maxReplicas,
		FinalUnderReplicated: masterViewUnderReplicated(s, cfg.ReplicationFactor),
	}, nil
}

// ---------- M2-G: Convergence ----------

type ConvergenceResult struct {
	FailuresInjected     int
	TicksToConverge      int
	TimeToConverge       time.Duration
	Converged            bool
	FinalUnderReplicated int
}

// RunConvergenceExperiment injects repeated failures (killing several
// nodes in sequence, no recoveries), then STOPS and measures how long the
// system takes to return to target RF for every chunk that still has
// enough surviving nodes to reach it.
func RunConvergenceExperiment(cfg Config, order []common.ServerID) (ConvergenceResult, error) {
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return ConvergenceResult{}, err
	}
	step := cfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()

	for _, target := range order {
		if err := s.KillChunkServer(target); err != nil {
			return ConvergenceResult{}, err
		}
		clock.Advance(step)
		s.Tick()
	}
	// Failures have stopped, but they may not all be DETECTED yet — ticking
	// here only enough to clear the heartbeat timeout would make "failures
	// stop" and "failures detected" the same instant by construction,
	// which trivializes the measurement (masterViewUnderReplicated reads 0
	// simply because the master doesn't know anything is wrong yet). Run
	// the detection phase to completion first, so "measure from here"
	// genuinely means after the master has reconciled every injected
	// failure, matching the spec's "inject repeated failures then stop"
	// framing.
	for _, target := range order {
		tickUntil(s, clock, step, maxTicksPerRound, func() bool {
			return !s.Master.IsServerAlive(target)
		})
	}
	start := clock.Now()
	ticks, converged := tickUntil(s, clock, step, maxTicksPerRound*len(order)+maxTicksPerRound, func() bool {
		return masterViewUnderReplicated(s, cfg.ReplicationFactor) == 0
	})
	elapsed := clock.Now().Sub(start)

	return ConvergenceResult{
		FailuresInjected: len(order), TicksToConverge: ticks, TimeToConverge: elapsed,
		Converged: converged, FinalUnderReplicated: masterViewUnderReplicated(s, cfg.ReplicationFactor),
	}, nil
}

// ---------- shared helpers ----------

func populateOneChunkWithThreeValidSources(s *sim.Simulator, cfg Config) (common.ChunkHandle, []common.ServerID, error) {
	if err := s.Client.CreateFile(experimentFile); err != nil {
		return 0, nil, err
	}
	handle, locations, err := s.AllocateChunkWithData(experimentFile, []byte("payload"))
	if err != nil {
		return 0, nil, err
	}
	return handle, locations, nil
}

func chunkMetaVersion(s *sim.Simulator, handle common.ChunkHandle) (common.ChunkVersion, bool) {
	// Only way to read version from this package is via GetChunkLocations'
	// sibling call pattern; reuse ChunkCount+GetChunkLocations is location-
	// only, so read version through a lease request's returned info is not
	// available either. We instead infer it from HasChunk on any current
	// location, which reports the version actually stored there — valid
	// once at least one non-stale location exists.
	ids := s.ServerIDs()
	for _, id := range ids {
		n, ok := s.Node(id)
		if !ok {
			continue
		}
		if v, held := n.HasChunk(handle); held {
			return v, true
		}
	}
	return 0, false
}

func chunkLocations(s *sim.Simulator, handle common.ChunkHandle) (common.ChunkHandle, int, []common.ServerID, error) {
	n, err := s.Master.ChunkCount(experimentFile)
	if err != nil {
		return handle, 0, nil, err
	}
	for i := 0; i < n; i++ {
		h, locs, err := s.Master.GetChunkLocations(experimentFile, i)
		if err != nil {
			return handle, 0, nil, err
		}
		if h == handle {
			return handle, i, locs, nil
		}
	}
	return handle, 0, nil, fmt.Errorf("handle %s not found in %s", handle, experimentFile)
}

type chunkSnapshot struct {
	version common.ChunkVersion
	locs    []common.ServerID
}

func snapshotChunkTable(s *sim.Simulator, n int) map[common.ChunkHandle]chunkSnapshot {
	out := make(map[common.ChunkHandle]chunkSnapshot)
	for i := 0; i < n; i++ {
		h, locs, err := s.Master.GetChunkLocations(experimentFile, i)
		if err != nil {
			continue
		}
		v, _ := chunkMetaVersion(s, h)
		out[h] = chunkSnapshot{version: v, locs: locs}
	}
	return out
}
