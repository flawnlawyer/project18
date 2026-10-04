package experiment

import (
	"fmt"
	"time"

	"project18/internal/common"
	"project18/internal/sim"
)

const experimentFile = common.Path("/experiment/data")

// ---------- Experiment A: baseline distribution ----------

type DistributionResult struct {
	Config             Config
	ChunksAllocated    int
	Distribution       DistributionSnapshot
	PlacementDecisions uint64
}

// RunDistributionExperiment allocates cfg.ChunkCount chunks against a fresh,
// otherwise-idle cluster and reports where the baseline policy put them.
func RunDistributionExperiment(cfg Config) (DistributionResult, error) {
	s, _ := Build(cfg)
	handles, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize)
	if err != nil {
		return DistributionResult{}, err
	}
	return DistributionResult{
		Config:             cfg,
		ChunksAllocated:    len(handles),
		Distribution:       Snapshot(s),
		PlacementDecisions: s.Metrics.Snapshot().PlacementDecisions,
	}, nil
}

// ---------- Experiment B: uneven cluster conditions ----------

// CapacityAwarenessResult answers one question: does placement change when
// some nodes have far less capacity than others? (Spec: "First determine
// whether the current M0 placement policy understands these differences.
// Do not automatically fix it.")
type CapacityAwarenessResult struct {
	Capacities           []uint64
	Distribution         DistributionSnapshot
	OverflowedNodes      []common.ServerID // nodes chosen as a replica location that then failed to physically store it (capacity exceeded)
	OverflowedChunkCount int

	// Master view vs physical reality, measured after 3 heartbeat ticks:
	PhantomChunks             int // chunks whose master-listed alive replicas include a node that does not hold the chunk
	PhantomReplicas           int
	MasterViewUnderReplicated int // chunks the master itself considers below RF
	ClientReadOK              bool
	ClientReadError           string
}

// RunCapacityAwarenessExperiment gives most nodes generous capacity and a
// couple of nodes barely any, then allocates chunks normally and records
// whether the policy avoided the small nodes (it can't - Choose() is never
// given capacity figures) and what the master believes afterwards about
// replicas that could not physically be stored.
func RunCapacityAwarenessExperiment(cfg Config, tinyNodeCount int, tinyCapacity, normalCapacity uint64) (CapacityAwarenessResult, error) {
	capacities := make([]uint64, cfg.NodeCount)
	for i := range capacities {
		if i < tinyNodeCount {
			capacities[i] = tinyCapacity
		} else {
			capacities[i] = normalCapacity
		}
	}
	runCfg := cfg
	runCfg.Capacities = capacities
	s, clock := Build(runCfg)

	if err := s.Client.CreateFile(experimentFile); err != nil {
		return CapacityAwarenessResult{}, err
	}
	payload := make([]byte, cfg.ChunkPayloadSize)
	overflowed := map[common.ServerID]bool{}
	overflowCount := 0
	for i := 0; i < cfg.ChunkCount; i++ {
		handle, locations, err := s.Master.AllocateChunk(experimentFile)
		if err != nil {
			return CapacityAwarenessResult{}, fmt.Errorf("allocate chunk %d: %w", i, err)
		}
		chunkOverflowed := false
		for _, loc := range locations {
			node, ok := s.Node(loc)
			if !ok {
				continue
			}
			if err := node.StoreChunk(handle, 1, payload); err != nil {
				overflowed[loc] = true
				chunkOverflowed = true
			}
		}
		if chunkOverflowed {
			overflowCount++
		}
	}

	step := cfg.HeartbeatTimeout / tickStepDivisor
	for i := 0; i < 3; i++ {
		clock.Advance(step)
		s.Tick()
	}

	phantomChunks, phantomReplicas := 0, 0
	for i := 0; i < cfg.ChunkCount; i++ {
		handle, locs, err := s.Master.GetChunkLocations(experimentFile, i)
		if err != nil {
			return CapacityAwarenessResult{}, err
		}
		has := false
		for _, l := range locs {
			n, ok := s.Node(l)
			if !ok {
				continue
			}
			if _, held := n.HasChunk(handle); !held {
				phantomReplicas++
				has = true
			}
		}
		if has {
			phantomChunks++
		}
	}
	_, readErr := s.Client.Read(experimentFile)
	readMsg := ""
	if readErr != nil {
		readMsg = readErr.Error()
	}

	ids := make([]common.ServerID, 0, len(overflowed))
	for id := range overflowed {
		ids = append(ids, id)
	}
	return CapacityAwarenessResult{
		Capacities:                capacities,
		Distribution:              Snapshot(s),
		OverflowedNodes:           ids,
		OverflowedChunkCount:      overflowCount,
		PhantomChunks:             phantomChunks,
		PhantomReplicas:           phantomReplicas,
		MasterViewUnderReplicated: masterViewUnderReplicated(s, cfg.ReplicationFactor),
		ClientReadOK:              readErr == nil,
		ClientReadError:           readMsg,
	}, nil
}

// LoadAwarenessResult answers: does placement account for chunks a node is
// already holding when deciding where to put new ones?
type LoadAwarenessResult struct {
	PreloadedNode      common.ServerID
	PreloadedCount     int
	DistributionBefore DistributionSnapshot
	DistributionAfter  DistributionSnapshot
}

// RunLoadAwarenessExperiment pre-loads one node with a large number of
// chunks the master doesn't know about (so it's "already busy" from a
// capacity/load perspective, exactly as a long-lived node would be), then
// allocates a fresh batch of chunks and checks whether that node received
// a smaller share.
func RunLoadAwarenessExperiment(cfg Config, preloadCount int) (LoadAwarenessResult, error) {
	s, _ := Build(cfg)
	ids := s.ServerIDs()
	if len(ids) == 0 {
		return LoadAwarenessResult{}, fmt.Errorf("no servers")
	}
	target := ids[0]
	node, _ := s.Node(target)
	payload := make([]byte, cfg.ChunkPayloadSize)
	for i := 0; i < preloadCount; i++ {
		// Handles far outside the master's normal allocation range, so
		// these never collide with real chunk handles.
		h := common.ChunkHandle(1_000_000 + i)
		_ = node.StoreChunk(h, 1, payload)
	}
	before := Snapshot(s)

	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return LoadAwarenessResult{}, err
	}
	after := Snapshot(s)

	return LoadAwarenessResult{
		PreloadedNode:      target,
		PreloadedCount:     preloadCount,
		DistributionBefore: before,
		DistributionAfter:  after,
	}, nil
}

// ---------- Experiments C & D: targeted and cascading failure ----------

type FailureImpactResult struct {
	Target               common.ServerID
	AffectedChunks       int // chunks that had a replica on Target at kill time
	ReplicasLost         int
	UnderReplicatedPeak  int
	ReReplicationStarted uint64
	ReReplicationOK      uint64
	ReReplicationFailed  uint64
	DetectionTicks       int
	DetectionDuration    time.Duration
	DetectionAchieved    bool
	RecoveryTicks        int
	RecoveryDuration     time.Duration // from kill to fully restored RF (or maxTicks reached)
	RecoveryAchieved     bool
	PreDistribution      DistributionSnapshot
	PostDistribution     DistributionSnapshot
	FinalUnderReplicated int // 0 iff RF was fully restored for every affected chunk
}

const tickStepDivisor = 2 // tick every HeartbeatTimeout/2, matching Master's own scheduler period
const maxTicksPerRound = 50

// masterViewUnderReplicated counts chunks of experimentFile that the master
// currently lists with fewer than rf alive replicas. This is the ground
// truth for "has recovery finished" in the master's own terms - the
// UnderReplicatedChunks gauge is not, because it counts chunks that needed
// repair at scan time even if the repair succeeded in that same pass.
func masterViewUnderReplicated(s *sim.Simulator, rf int) int {
	n, err := s.Master.ChunkCount(experimentFile)
	if err != nil {
		return -1
	}
	under := 0
	for i := 0; i < n; i++ {
		_, locs, err := s.Master.GetChunkLocations(experimentFile, i)
		if err != nil || len(locs) < rf {
			under++
		}
	}
	return under
}

// runFailureRound kills target on an already-populated, already-ticked
// simulator and measures detection + re-replication, without recovering
// target afterward (so Experiment D can chain rounds). Detection is the
// first tick at which the master no longer considers target alive;
// recovery is the first tick after detection at which the master lists
// every chunk with >= RF alive replicas. Both are quantized to the tick
// step. Time spent *copying* data is not modeled by the simulator (a
// re-replication pass completes instantly unless a link delay is
// injected), so "detection -> restored" is 0 ticks whenever destinations
// are available; that is a property of the simulator, not a measurement of
// network cost.
func runFailureRound(s *sim.Simulator, clock *common.FakeClock, cfg Config, target common.ServerID) FailureImpactResult {
	step := cfg.HeartbeatTimeout / tickStepDivisor
	pre := Snapshot(s)
	affected := pre.PerServer[target]

	metricsBefore := s.Metrics.Snapshot()
	killTime := clock.Now()
	_ = s.KillChunkServer(target)

	peak := 0
	var detectTicks, recoverTicks int
	var detectDur, recoverDur time.Duration
	detected, recovered := false, false
	for tick := 1; tick <= maxTicksPerRound; tick++ {
		clock.Advance(step)
		s.Tick()
		if g := int(s.Metrics.Snapshot().UnderReplicatedChunks); g > peak {
			peak = g
		}
		if !detected && !s.Master.IsServerAlive(target) {
			detected, detectTicks, detectDur = true, tick, clock.Now().Sub(killTime)
		}
		if detected && masterViewUnderReplicated(s, cfg.ReplicationFactor) == 0 {
			recovered, recoverTicks, recoverDur = true, tick, clock.Now().Sub(killTime)
			break
		}
	}
	if !detected {
		detectTicks, detectDur = maxTicksPerRound, clock.Now().Sub(killTime)
	}
	if !recovered {
		recoverTicks, recoverDur = maxTicksPerRound, clock.Now().Sub(killTime)
	}

	metricsAfter := s.Metrics.Snapshot()
	return FailureImpactResult{
		Target:               target,
		AffectedChunks:       affected,
		ReplicasLost:         affected,
		UnderReplicatedPeak:  peak,
		ReReplicationStarted: metricsAfter.ReReplicationsStarted - metricsBefore.ReReplicationsStarted,
		ReReplicationOK:      metricsAfter.ReReplicationsOK - metricsBefore.ReReplicationsOK,
		ReReplicationFailed:  metricsAfter.ReReplicationsFailed - metricsBefore.ReReplicationsFailed,
		DetectionTicks:       detectTicks,
		DetectionDuration:    detectDur,
		DetectionAchieved:    detected,
		RecoveryTicks:        recoverTicks,
		RecoveryDuration:     recoverDur,
		RecoveryAchieved:     recovered,
		PreDistribution:      pre,
		PostDistribution:     Snapshot(s),
		FinalUnderReplicated: masterViewUnderReplicated(s, cfg.ReplicationFactor),
	}
}

// RunTargetedFailureExperiment populates a cluster, lets it settle, then
// kills exactly one node and measures the consequences.
func RunTargetedFailureExperiment(cfg Config, target common.ServerID) (FailureImpactResult, error) {
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return FailureImpactResult{}, err
	}
	clock.Advance(cfg.HeartbeatTimeout / tickStepDivisor)
	s.Tick() // one steady-state heartbeat before we start measuring
	return runFailureRound(s, clock, cfg, target), nil
}

// CascadeResult is Experiment D: one FailureImpactResult per round, plus the
// concentration of where replacement replicas actually landed across the
// whole sequence (spec: "replication concentration").
type CascadeResult struct {
	Rounds              []FailureImpactResult
	ConcentrationByNode map[common.ServerID]int // new replicas received, summed across all rounds
}

// RunCascadingFailureExperiment kills each server in order in sequence,
// WITHOUT recovering earlier victims, measuring after every kill. order
// must not include every node in the cluster, or the last round(s) will
// have no healthy destinations left by construction.
func RunCascadingFailureExperiment(cfg Config, order []common.ServerID) (CascadeResult, error) {
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return CascadeResult{}, err
	}
	clock.Advance(cfg.HeartbeatTimeout / tickStepDivisor)
	s.Tick()

	concentration := make(map[common.ServerID]int)
	var rounds []FailureImpactResult
	for _, target := range order {
		before := Snapshot(s)
		result := runFailureRound(s, clock, cfg, target)
		after := Snapshot(s)
		for id, n := range after.PerServer {
			if n > before.PerServer[id] {
				concentration[id] += n - before.PerServer[id]
			}
		}
		rounds = append(rounds, result)
	}
	return CascadeResult{Rounds: rounds, ConcentrationByNode: concentration}, nil
}

// ---------- Experiment B (part 3): previously failed and recovered node ----------

// RecoveredNodeResult records what the master's view of replication looks
// like after a failed node comes back holding its old (now redundant)
// replicas.
type RecoveredNodeResult struct {
	Target                  common.ServerID
	ChunksOnTargetBefore    int // physical chunks on target before it was killed
	ChunksOnTargetAfter     int // physical chunks on target after recovery + heartbeats
	OverReplicatedChunks    int // chunks whose master-known alive replica count exceeds RF
	MaxReplicasSeen         int
	UnderReplicatedChunks   int
	ChunkCount              int
	NewChunksGainedByTarget int // of a fresh post-recovery batch, how many landed on target
	NewBatchSize            int
}

// RunRecoveredNodeExperiment kills target, lets the cluster fully
// re-replicate, brings target back, lets heartbeats flow, then reports
// whether any chunk now has MORE than RF replicas per the master's own
// view, and how much of a fresh allocation batch the recovered node gets.
func RunRecoveredNodeExperiment(cfg Config, target common.ServerID, newBatch int) (RecoveredNodeResult, error) {
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		return RecoveredNodeResult{}, err
	}
	step := cfg.HeartbeatTimeout / tickStepDivisor
	clock.Advance(step)
	s.Tick()

	node, ok := s.Node(target)
	if !ok {
		return RecoveredNodeResult{}, fmt.Errorf("no such server %s", target)
	}
	before := len(node.ChunkHandles())

	runFailureRound(s, clock, cfg, target)
	if err := s.RecoverChunkServer(target); err != nil {
		return RecoveredNodeResult{}, err
	}
	for i := 0; i < 3; i++ {
		clock.Advance(step)
		s.Tick()
	}
	after := len(node.ChunkHandles())

	over, maxSeen, under := 0, 0, 0
	for i := 0; i < cfg.ChunkCount; i++ {
		_, locs, err := s.Master.GetChunkLocations(experimentFile, i)
		if err != nil {
			return RecoveredNodeResult{}, err
		}
		if len(locs) > cfg.ReplicationFactor {
			over++
		}
		if len(locs) < cfg.ReplicationFactor {
			under++
		}
		if len(locs) > maxSeen {
			maxSeen = len(locs)
		}
	}

	gained := 0
	payload := make([]byte, cfg.ChunkPayloadSize)
	for i := 0; i < newBatch; i++ {
		_, locs, err := s.Master.AllocateChunk(experimentFile)
		if err != nil {
			return RecoveredNodeResult{}, err
		}
		for _, l := range locs {
			if l == target {
				gained++
			}
		}
		_ = payload
	}

	return RecoveredNodeResult{
		Target: target, ChunksOnTargetBefore: before, ChunksOnTargetAfter: after,
		OverReplicatedChunks: over, MaxReplicasSeen: maxSeen, UnderReplicatedChunks: under,
		ChunkCount: cfg.ChunkCount, NewChunksGainedByTarget: gained, NewBatchSize: newBatch,
	}, nil
}
