package experiment

import (
	"testing"
	"time"

	"project18/internal/common"
)

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.NodeCount = 5
	cfg.ReplicationFactor = 3
	cfg.ChunkCount = 20
	cfg.ChunkPayloadSize = 8
	cfg.Seed = 7
	cfg.HeartbeatTimeout = 10 * time.Second
	return cfg
}

func TestDistributionExperimentReplicaCountAndSpread(t *testing.T) {
	cfg := testConfig()
	res, err := RunDistributionExperiment(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.ChunksAllocated != cfg.ChunkCount {
		t.Fatalf("allocated %d chunks, want %d", res.ChunksAllocated, cfg.ChunkCount)
	}
	wantTotal := cfg.ChunkCount * cfg.ReplicationFactor
	if res.Distribution.Total != wantTotal {
		t.Fatalf("total replicas = %d, want %d (chunks * RF)", res.Distribution.Total, wantTotal)
	}
	if res.PlacementDecisions != uint64(cfg.ChunkCount) {
		t.Fatalf("placement decisions = %d, want %d (one per chunk allocation)", res.PlacementDecisions, cfg.ChunkCount)
	}
	// Sanity bound, not a tight assertion: with 5 nodes and RF=3, no single
	// node should be able to hold every replica of every chunk.
	if res.Distribution.Max == wantTotal {
		t.Fatalf("one node holds every replica (%d) — placement is not spreading load at all", wantTotal)
	}
}

func TestDistributionExperimentDeterministicAcrossRuns(t *testing.T) {
	cfg := testConfig()
	a, err := RunDistributionExperiment(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := RunDistributionExperiment(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for id, countA := range a.Distribution.PerServer {
		countB, ok := b.Distribution.PerServer[id]
		if !ok || countA != countB {
			t.Fatalf("server %s: run A=%d run B=%d — same seed produced different distributions", id, countA, countB)
		}
	}
}

func TestCapacityAwarenessExperimentIgnoresCapacity(t *testing.T) {
	cfg := testConfig()
	cfg.ChunkCount = 30
	res, err := RunCapacityAwarenessExperiment(cfg, 2, 0 /* tiny nodes can store nothing */, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	// With two zero-capacity nodes among the candidates and no capacity
	// awareness in Choose(), we expect at least one placement to have
	// picked a zero-capacity node and failed to store there.
	if res.OverflowedChunkCount == 0 {
		t.Fatal("expected at least one chunk to have a replica placed on a zero-capacity node (baseline placement is capacity-blind)")
	}
}

func TestLoadAwarenessExperimentIgnoresExistingLoad(t *testing.T) {
	cfg := testConfig()
	res, err := RunLoadAwarenessExperiment(cfg, 500)
	if err != nil {
		t.Fatal(err)
	}
	// The preloaded node should receive a share of the new chunks roughly
	// like everyone else — not zero, not capped — because Choose() has no
	// way to know it's already busy.
	after := res.DistributionAfter.PerServer[res.PreloadedNode]
	before := res.DistributionBefore.PerServer[res.PreloadedNode]
	gained := after - before
	if gained == 0 {
		t.Fatal("preloaded node received zero new replicas out of a full batch — unexpectedly avoided, contradicts a capacity/load-blind policy")
	}
}

func TestFailedNodeDataIsNotGarbageCollected(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 6
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		t.Fatal(err)
	}
	target := common.ServerID("CS1")
	node, ok := s.Node(target)
	if !ok {
		t.Fatal("missing CS1")
	}
	before := len(node.ChunkHandles())
	if before == 0 {
		t.Skip("CS1 happened to receive zero replicas this seed; nothing to observe")
	}

	if err := s.KillChunkServer(target); err != nil {
		t.Fatal(err)
	}
	step := cfg.HeartbeatTimeout / tickStepDivisor
	tickUntil(s, clock, step, maxTicksPerRound, func() bool {
		return s.Metrics.Snapshot().UnderReplicatedChunks == 0
	})

	// The master no longer counts CS1 as a location (that's what
	// re-replication just proved, since Total came back to full RF using
	// other nodes) — but CS1's own local map was never touched.
	after := len(node.ChunkHandles())
	if after != before {
		t.Fatalf("CS1's local chunk map changed after being killed (before=%d after=%d) — expected it to be untouched (no garbage collection in M0/M1)", before, after)
	}

	live := Snapshot(s)
	raw := RawSnapshot(s)
	if raw.Total <= live.Total {
		t.Fatalf("raw total (%d) should exceed live total (%d) once a dead node's orphaned replicas are counted", raw.Total, live.Total)
	}
}

func TestTargetedFailureExperimentRestoresReplicationFactor(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 6
	res, err := RunTargetedFailureExperiment(cfg, common.ServerID("CS1"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.DetectionAchieved {
		t.Fatal("failure detection never completed within the tick budget")
	}
	if !res.RecoveryAchieved {
		t.Fatalf("recovery never completed within the tick budget (final under-replicated=%d)", res.FinalUnderReplicated)
	}
	if res.FinalUnderReplicated != 0 {
		t.Fatalf("expected 0 under-replicated chunks after recovery, got %d", res.FinalUnderReplicated)
	}
	if res.ReReplicationOK == 0 {
		t.Fatal("expected at least one successful re-replication")
	}
	// Post-recovery distribution must have replaced every lost replica.
	if res.PostDistribution.Total != res.PreDistribution.Total {
		t.Fatalf("total replicas before=%d after=%d — recovery did not fully restore replica count", res.PreDistribution.Total, res.PostDistribution.Total)
	}
}

func TestCascadingFailureExperimentCompletesAndTracksConcentration(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 8
	cfg.ReplicationFactor = 3
	order := []common.ServerID{"CS1", "CS3", "CS5"} // leaves 5 of 8 alive: still enough for RF=3
	res, err := RunCascadingFailureExperiment(cfg, order)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rounds) != len(order) {
		t.Fatalf("got %d rounds, want %d", len(res.Rounds), len(order))
	}
	for i, r := range res.Rounds {
		if !r.DetectionAchieved {
			t.Fatalf("round %d (%s): detection never completed", i, r.Target)
		}
	}
	// With 5 survivors and RF=3, every round should still be able to fully
	// recover.
	last := res.Rounds[len(res.Rounds)-1]
	if last.FinalUnderReplicated != 0 {
		t.Fatalf("final round left %d chunks under-replicated even though enough nodes survived", last.FinalUnderReplicated)
	}
	if len(res.ConcentrationByNode) == 0 {
		t.Fatal("expected re-replication concentration data for at least one surviving node")
	}
}

func TestCascadingFailureExhaustsCapacityWhenTooManyNodesDie(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 4
	cfg.ReplicationFactor = 3
	// Killing 2 of 4 nodes leaves 2 alive — not enough to satisfy RF=3, by
	// construction. This is meant to fail to fully recover; that failure
	// IS the expected result.
	order := []common.ServerID{"CS1", "CS2"}
	res, err := RunCascadingFailureExperiment(cfg, order)
	if err != nil {
		t.Fatal(err)
	}
	last := res.Rounds[len(res.Rounds)-1]
	if last.FinalUnderReplicated == 0 {
		t.Fatal("expected chunks to remain under-replicated when too few healthy nodes remain to satisfy RF — got full recovery instead, which would mean the safety bound is wrong")
	}
}

func TestCapacityBlindPlacementLeavesPhantomReplicas(t *testing.T) {
	cfg := testConfig()
	cfg.ChunkCount = 30
	res, err := RunCapacityAwarenessExperiment(cfg, 2, 32, 1<<20) // tiny nodes hold 2 chunks of 16B... payload is 8B here, so 4
	if err != nil {
		t.Fatal(err)
	}
	if res.PhantomReplicas == 0 {
		t.Fatal("expected master-listed replicas that were never physically stored")
	}
	// The master cannot see the gap: by its own accounting every chunk is fully replicated.
	if res.MasterViewUnderReplicated != 0 {
		t.Fatalf("master view reports %d under-replicated chunks; phantom replicas should be invisible to it", res.MasterViewUnderReplicated)
	}
}

func TestRecoveredNodeLeavesOverReplicatedChunks(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 6
	res, err := RunRecoveredNodeExperiment(cfg, "CS1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.ChunksOnTargetBefore == 0 {
		t.Skip("CS1 held nothing for this seed")
	}
	if res.OverReplicatedChunks == 0 || res.MaxReplicasSeen <= cfg.ReplicationFactor {
		t.Fatalf("expected chunks above RF after a node with intact replicas returns; over=%d max=%d", res.OverReplicatedChunks, res.MaxReplicasSeen)
	}
	if res.UnderReplicatedChunks != 0 {
		t.Fatalf("unexpected under-replication after recovery: %d", res.UnderReplicatedChunks)
	}
}

func TestRecoveryCompletesInSameTickAsDetection(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 6
	res, err := RunTargetedFailureExperiment(cfg, "CS2")
	if err != nil {
		t.Fatal(err)
	}
	// Pins a simulator property: re-replication is synchronous within the
	// pass that detects the failure (copy time is not modeled).
	if res.RecoveryTicks != res.DetectionTicks {
		t.Fatalf("recovery at tick %d, detection at tick %d; expected the same pass", res.RecoveryTicks, res.DetectionTicks)
	}
}

func TestFailureRoundMetricsAreConsistent(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 6
	s, clock := Build(cfg)
	if _, err := populateChunks(s, experimentFile, cfg.ChunkCount, cfg.ChunkPayloadSize); err != nil {
		t.Fatal(err)
	}
	clock.Advance(cfg.HeartbeatTimeout / tickStepDivisor)
	s.Tick()
	r := runFailureRound(s, clock, cfg, "CS3")
	m := s.Metrics.Snapshot()

	if m.FailureDetections != 1 {
		t.Fatalf("FailureDetections=%d, want 1", m.FailureDetections)
	}
	if m.RecoveryIncidents != 1 {
		t.Fatalf("RecoveryIncidents=%d, want 1 (recovery timer must close once RF is restored)", m.RecoveryIncidents)
	}
	// One initial placement per chunk, plus one re-placement per chunk that lost a replica.
	if want := uint64(cfg.ChunkCount + r.AffectedChunks); m.PlacementDecisions != want {
		t.Fatalf("PlacementDecisions=%d, want %d", m.PlacementDecisions, want)
	}
	if m.ReReplicationsOK != uint64(r.AffectedChunks) || m.ReReplicationsFailed != 0 {
		t.Fatalf("re-replication ok/failed = %d/%d, want %d/0", m.ReReplicationsOK, m.ReReplicationsFailed, r.AffectedChunks)
	}
	if m.MessagesSent == 0 {
		t.Fatal("expected network messages to be counted")
	}
}

func TestCascadeIsDeterministicAcrossRuns(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 8
	order := []common.ServerID{"CS2", "CS4", "CS1"}
	a, err := RunCascadingFailureExperiment(cfg, order)
	if err != nil {
		t.Fatal(err)
	}
	b, err := RunCascadingFailureExperiment(cfg, order)
	if err != nil {
		t.Fatal(err)
	}
	for i := range a.Rounds {
		if a.Rounds[i].PostDistribution.String() != b.Rounds[i].PostDistribution.String() {
			t.Fatalf("round %d differs between identical runs:\n%s\n%s", i, a.Rounds[i].PostDistribution, b.Rounds[i].PostDistribution)
		}
	}
}
