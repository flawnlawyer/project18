package master

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"project18/internal/chunkserver"
	"project18/internal/common"
	"project18/internal/events"
	"project18/internal/metrics"
	"project18/internal/network"
)

// testHarness builds a master with N registered chunkservers and a fake
// clock, so tests can advance time deterministically instead of sleeping
// past real heartbeat timeouts.
type testHarness struct {
	m     *Master
	clock *common.FakeClock
	nodes map[common.ServerID]*chunkserver.ChunkServer
	net   *network.InProcessNetwork
	log   *events.Log
	met   *metrics.Metrics
}

func newHarness(t *testing.T, numServers int, cfg Config) *testHarness {
	t.Helper()
	log := events.NewLog()
	met := metrics.New()
	net := network.NewInProcessNetwork(met)
	m := NewMaster("MASTER", net, log, met, cfg)
	clock := common.NewFakeClock(time.Unix(0, 0))
	m.SetClock(clock)

	h := &testHarness{m: m, clock: clock, nodes: make(map[common.ServerID]*chunkserver.ChunkServer), net: net, log: log, met: met}
	for i := 1; i <= numServers; i++ {
		id := common.ServerID(fmt.Sprintf("CS%d", i))
		cs := chunkserver.New(id, 1<<20, log)
		h.nodes[id] = cs
		m.RegisterChunkServer(cs)
	}
	// RegisterChunkServer stamped the heartbeat with clock.Now() (t=0),
	// which is what we want as the baseline.
	return h
}

func (h *testHarness) heartbeatAll(t *testing.T) {
	t.Helper()
	for id, node := range h.nodes {
		if !node.IsOnline() {
			continue
		}
		reported := make(map[common.ChunkHandle]common.ChunkVersion)
		for _, hnd := range node.ChunkHandles() {
			v, _ := node.HasChunk(hnd)
			reported[hnd] = v
		}
		if err := h.m.Heartbeat(id, reported, h.clock.Now()); err != nil {
			t.Fatalf("heartbeat from %s: %v", id, err)
		}
	}
}

func TestAllocateChunkPicksReplicationFactorReplicas(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationFactor = 3
	h := newHarness(t, 5, cfg)

	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	handle, locations, err := h.m.AllocateChunk("/f")
	if err != nil {
		t.Fatal(err)
	}
	if len(locations) != cfg.ReplicationFactor {
		t.Fatalf("got %d replicas, want %d", len(locations), cfg.ReplicationFactor)
	}
	seen := map[common.ServerID]bool{}
	for _, l := range locations {
		if seen[l] {
			t.Fatalf("duplicate replica location %s", l)
		}
		seen[l] = true
	}
	meta, ok := h.m.meta.GetChunkMeta(handle)
	if !ok {
		t.Fatal("no metadata recorded for allocated chunk")
	}
	if len(meta.Locations) != cfg.ReplicationFactor {
		t.Fatalf("metadata has %d locations, want %d", len(meta.Locations), cfg.ReplicationFactor)
	}
}

func TestFailureDetectionTriggersReReplication(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationFactor = 3
	cfg.HeartbeatTimeout = 10 * time.Second
	h := newHarness(t, 5, cfg)

	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	handle, locations, err := h.m.AllocateChunk("/f")
	if err != nil {
		t.Fatal(err)
	}
	// Actually store the chunk bytes on its replicas so re-replication has
	// something to read from.
	for _, loc := range locations {
		if err := h.nodes[loc].StoreChunk(handle, 1, []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	h.heartbeatAll(t)

	victim := locations[0]
	h.nodes[victim].SetOnline(false)

	// Advance past the heartbeat timeout without the victim heartbeating.
	h.clock.Advance(cfg.HeartbeatTimeout + time.Second)
	h.heartbeatAll(t) // survivors heartbeat, victim does not (offline)
	h.m.Scheduler().Tick()

	meta, ok := h.m.meta.GetChunkMeta(handle)
	if !ok {
		t.Fatal("chunk metadata disappeared")
	}
	if meta.Locations[victim] {
		t.Fatalf("dead server %s still listed as a location", victim)
	}
	if len(meta.Locations) != cfg.ReplicationFactor {
		t.Fatalf("after re-replication got %d locations, want %d: %v", len(meta.Locations), cfg.ReplicationFactor, meta.Locations)
	}
	snap := h.met.Snapshot()
	if snap.ReReplicationsOK == 0 {
		t.Fatal("expected at least one successful re-replication to be recorded")
	}
	if snap.FailureDetections == 0 {
		t.Fatal("expected a failure detection to be recorded")
	}
}

func TestStaleReplicaDroppedFromLocations(t *testing.T) {
	cfg := DefaultConfig()
	h := newHarness(t, 5, cfg)

	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	handle, locations, err := h.m.AllocateChunk("/f")
	if err != nil {
		t.Fatal(err)
	}
	// Bump the chunk's version (as a lease grant would) so the master
	// expects a newer version than what we're about to report.
	h.m.meta.BumpVersion(handle)

	stale := locations[0]
	reported := map[common.ChunkHandle]common.ChunkVersion{handle: 1} // master is at version 2 now
	if err := h.m.Heartbeat(stale, reported, h.clock.Now()); err != nil {
		t.Fatal(err)
	}

	meta, _ := h.m.meta.GetChunkMeta(handle)
	if meta.Locations[stale] {
		t.Fatalf("stale replica %s should have been dropped from locations", stale)
	}
	if h.met.Snapshot().StaleReplicasDetected == 0 {
		t.Fatal("expected stale replica detection to be recorded in metrics")
	}
}

func TestMasterRestartRestoresCheckpointAndReplaysLog(t *testing.T) {
	cfg := DefaultConfig()
	h := newHarness(t, 5, cfg)

	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.m.AllocateChunk("/f"); err != nil {
		t.Fatal(err)
	}
	h.m.Checkpoint() // snapshot #1: 1 file, 1 chunk

	if err := h.m.CreateFile("/g"); err != nil { // happens only in the op log
		t.Fatal(err)
	}
	if _, _, err := h.m.AllocateChunk("/g"); err != nil {
		t.Fatal(err)
	}

	h.m.Kill()
	if err := h.m.CreateFile("/should-fail"); err == nil {
		t.Fatal("expected killed master to reject operations")
	}
	h.m.Recover()

	if !h.m.IsAlive() {
		t.Fatal("master should be alive after Recover")
	}
	for _, path := range []common.Path{"/f", "/g"} {
		n, err := h.m.ChunkCount(path)
		if err != nil {
			t.Fatalf("%s missing after restart: %v", path, err)
		}
		if n != 1 {
			t.Fatalf("%s: got %d chunks after restart, want 1", path, n)
		}
	}
}

func TestKilledMasterRejectsOperations(t *testing.T) {
	cfg := DefaultConfig()
	h := newHarness(t, 3, cfg)
	h.m.Kill()

	if err := h.m.CreateFile("/f"); err == nil {
		t.Fatal("expected error from killed master")
	}
	if _, _, err := h.m.AllocateChunk("/f"); err == nil {
		t.Fatal("expected error from killed master")
	}
}

// TestPlacementIsDeterministicAcrossRuns is the M1 regression test for the
// determinism bug fixed in this milestone: candidate lists were built by
// iterating Go maps (server registry, failure-detector's known-servers,
// chunk metadata's location set), whose iteration order is randomized
// per-process. That fed a randomized draw order into the placement
// policy's single shared *rand.Rand, so identical seeds produced different
// placements across runs. Every such site now sorts before returning.
func TestPlacementIsDeterministicAcrossRuns(t *testing.T) {
	// Capture full placement for two independent runs with identical
	// config/seed, and compare every chunk's locations.
	type run struct {
		locations map[common.ChunkHandle][]common.ServerID
	}
	doRun := func() run {
		cfg := DefaultConfig()
		cfg.ReplicationFactor = 3
		cfg.PlacementSeed = 42
		h := newHarness(t, 8, cfg)
		if err := h.m.CreateFile("/f"); err != nil {
			t.Fatal(err)
		}
		locs := make(map[common.ChunkHandle][]common.ServerID)
		for i := 0; i < 20; i++ {
			handle, locations, err := h.m.AllocateChunk("/f")
			if err != nil {
				t.Fatal(err)
			}
			sorted := append([]common.ServerID(nil), locations...)
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			locs[handle] = sorted
		}
		return run{locations: locs}
	}

	a := doRun()
	b := doRun()
	if len(a.locations) != len(b.locations) {
		t.Fatalf("run lengths differ: %d vs %d", len(a.locations), len(b.locations))
	}
	for handle, locA := range a.locations {
		locB, ok := b.locations[handle]
		if !ok {
			t.Fatalf("chunk %s missing from second run", handle)
		}
		if fmt.Sprint(locA) != fmt.Sprint(locB) {
			t.Fatalf("chunk %s placement differs across runs with the same seed: %v vs %v", handle, locA, locB)
		}
	}
}
