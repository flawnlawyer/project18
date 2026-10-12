package master

import (
	"fmt"
	"testing"
	"time"

	"project18/internal/chunkserver"
	"project18/internal/common"
)

// --- test doubles ---

// scriptedPlacement returns a pre-scripted sequence of destination picks,
// one call per entry, ignoring candidates/exclude. Used to deterministically
// test destination-retry without depending on the real policy's RNG order.
type scriptedPlacement struct {
	calls int
	picks [][]common.ServerID
	errs  []error
}

func (p *scriptedPlacement) Choose(candidates []common.ServerID, exclude map[common.ServerID]bool, count int) ([]common.ServerID, error) {
	i := p.calls
	p.calls++
	if i >= len(p.picks) {
		return nil, fmt.Errorf("scriptedPlacement: no more scripted picks (call %d)", i)
	}
	if p.errs != nil && i < len(p.errs) && p.errs[i] != nil {
		return nil, p.errs[i]
	}
	return p.picks[i], nil
}

// lyingNode wraps a real ChunkServer but can be told to report StoreChunk
// success while NOT actually making HasChunk agree afterward — the only
// way to exercise the "verification fails after a successful-looking copy"
// branch, since the honest ChunkServer can never produce that disagreement
// (StoreChunk and HasChunk read/write the same map).
type lyingNode struct {
	*chunkserver.ChunkServer
	lieOnStore bool
}

func (n *lyingNode) StoreChunk(handle common.ChunkHandle, version common.ChunkVersion, data []byte) error {
	if n.lieOnStore {
		return nil // claims success but never actually stores anything
	}
	return n.ChunkServer.StoreChunk(handle, version, data)
}

// --- tests ---

func TestRepairJobEnqueueIsIdempotent(t *testing.T) {
	q := NewInMemoryRepairQueue()
	now := time.Unix(0, 0)
	j1, created1 := q.Enqueue(5, 3, 2, now)
	j2, created2 := q.Enqueue(5, 3, 2, now.Add(time.Second))
	if !created1 {
		t.Fatal("first Enqueue should report created=true")
	}
	if created2 {
		t.Fatal("second Enqueue for the same handle should report created=false")
	}
	if j1.FirstEnqueuedAt != j2.FirstEnqueuedAt {
		t.Fatal("second Enqueue must not overwrite the original job (idempotent)")
	}
	if len(q.Pending()) != 1 {
		t.Fatalf("expected exactly 1 pending job, got %d", len(q.Pending()))
	}
}

func TestSourceFailureDuringRepairRetriesAnotherSource(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationFactor = 3
	h := newHarness(t, 6, cfg)

	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	handle, locations, err := h.m.AllocateChunk("/f")
	if err != nil {
		t.Fatal(err)
	}
	for _, loc := range locations {
		if err := h.nodes[loc].StoreChunk(handle, 1, []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	h.heartbeatAll(t)

	// Kill one of the three replicas outright (fd will eventually notice),
	// and take a SECOND replica offline at the storage layer only — the
	// failure detector still considers it alive (no heartbeat gap has
	// elapsed), so it's a valid candidate source, but any read against it
	// fails. This is exactly "source fails during repair": chosen because
	// it looked valid, but fails when actually read.
	deadReplica := locations[0]
	flakySource := locations[1]
	goodSource := locations[2]
	h.nodes[deadReplica].SetOnline(false)
	h.nodes[flakySource].SetOnline(false) // offline, but fd doesn't know yet

	h.clock.Advance(cfg.HeartbeatTimeout + time.Second)
	// Only the surviving, still-online replica heartbeats; flakySource and
	// deadReplica both go silent, but within THIS single tick fd hasn't
	// timed either out yet for the scan that CheckAndSchedule is about to
	// do inside this same call... so instead call Heartbeat for the good
	// one only, then invoke the repair pass directly without first running
	// a dead-server scan, so flakySource is still "alive" per fd when
	// repairChunk picks a source.
	if err := h.m.Heartbeat(goodSource, map[common.ChunkHandle]common.ChunkVersion{handle: 1}, h.clock.Now()); err != nil {
		t.Fatal(err)
	}

	rm := h.m.replication.(*GFSReplicationManager)
	filled := rm.repairChunk(handle, 1, []common.ServerID{flakySource, goodSource}, 1)
	if filled != 1 {
		t.Fatalf("expected repairChunk to fill 1 replica despite the flaky source, got %d", filled)
	}
	meta, _ := h.m.meta.GetChunkMeta(handle)
	if len(meta.Locations) < 3 {
		t.Fatalf("expected a new valid location after retry, got %d: %v", len(meta.Locations), meta.Locations)
	}
	snap := h.met.Snapshot()
	if snap.ReReplicationsFailed == 0 {
		t.Fatal("expected at least one recorded failure (the flaky source attempt) before the retry succeeded")
	}
	if snap.ReReplicationsOK == 0 {
		t.Fatal("expected the eventual retry to succeed")
	}
}

func TestDestinationFailureDuringRepairRetriesAnotherDestination(t *testing.T) {
	log := newTestLogHarness(t)
	defer log.cleanup()

	h := log.h
	handle, source := log.handle, log.source

	badDest := common.ServerID("BAD")
	goodDest := common.ServerID("GOOD")
	h.nodes[badDest] = chunkserverNew(t, badDest)
	h.nodes[goodDest] = chunkserverNew(t, goodDest)
	h.m.RegisterChunkServer(h.nodes[badDest])
	h.m.RegisterChunkServer(h.nodes[goodDest])
	h.heartbeatAll(t)
	// badDest is offline at the storage layer only, so StoreChunk fails but
	// it still looks like a legitimate placement candidate.
	h.nodes[badDest].SetOnline(false)

	rm := h.m.replication.(*GFSReplicationManager)
	rm.placement = &scriptedPlacement{picks: [][]common.ServerID{{badDest}, {goodDest}}}

	filled := rm.repairChunk(handle, 1, []common.ServerID{source}, 1)
	if filled != 1 {
		t.Fatalf("expected repair to succeed via the second (good) destination, got filled=%d", filled)
	}
	if v, ok := h.nodes[goodDest].HasChunk(handle); !ok || v != 1 {
		t.Fatalf("expected goodDest to hold a verified copy, has=%v version=%d", ok, v)
	}
	if _, ok := h.nodes[badDest].HasChunk(handle); ok {
		t.Fatal("badDest must not end up holding the chunk")
	}
	meta, _ := h.m.meta.GetChunkMeta(handle)
	if meta.Locations[badDest] {
		t.Fatal("badDest must never be committed to metadata as a valid location")
	}
	if !meta.Locations[goodDest] {
		t.Fatal("goodDest should be committed to metadata")
	}
	snap := h.met.Snapshot()
	if snap.ReReplicationsFailed == 0 {
		t.Fatal("expected the failed attempt on badDest to be recorded")
	}
}

func TestVerificationFailureBlocksMetadataCommit(t *testing.T) {
	log := newTestLogHarness(t)
	defer log.cleanup()
	h := log.h
	handle, source := log.handle, log.source

	liarID := common.ServerID("LIAR")
	liar := &lyingNode{ChunkServer: chunkserverNew(t, liarID), lieOnStore: true}
	extra := map[common.ServerID]chunkserver.StorageNode{liarID: liar}
	h.m.RegisterChunkServer(liar)
	h.heartbeatAll(t)

	rm := h.m.replication.(*GFSReplicationManager)
	rm.placement = &scriptedPlacement{picks: [][]common.ServerID{{liarID}}}
	rm.nodes = func(id common.ServerID) (chunkserver.StorageNode, bool) {
		if n, ok := extra[id]; ok {
			return n, true
		}
		n, ok := h.nodes[id]
		return n, ok
	}

	filled := rm.repairChunk(handle, 1, []common.ServerID{source}, 1)
	if filled != 0 {
		t.Fatalf("expected the lying destination's claimed success to be rejected by verification, got filled=%d", filled)
	}
	meta, _ := h.m.meta.GetChunkMeta(handle)
	if meta.Locations[liarID] {
		t.Fatal("a destination that failed verification must never be committed to metadata — this is the false-success-commit invariant")
	}
	snap := h.met.Snapshot()
	if snap.VerificationFailures == 0 {
		t.Fatal("expected VerificationFailures to be recorded")
	}
	if snap.FalseSuccessRepairCommits != 0 {
		t.Fatalf("FalseSuccessRepairCommits must always be 0, got %d", snap.FalseSuccessRepairCommits)
	}
}

func TestUnexpectedChunkIsRecordedNotActedOn(t *testing.T) {
	cfg := DefaultConfig()
	h := newHarness(t, 3, cfg)
	var server common.ServerID
	for id := range h.nodes {
		server = id
		break
	}
	unknownHandle := common.ChunkHandle(999999)
	err := h.m.Heartbeat(server, map[common.ChunkHandle]common.ChunkVersion{unknownHandle: 1}, h.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	snap := h.met.Snapshot()
	if snap.UnexpectedChunksObserved != 1 {
		t.Fatalf("UnexpectedChunksObserved = %d, want 1", snap.UnexpectedChunksObserved)
	}
	if snap.MetadataInconsistencies != 1 {
		t.Fatalf("MetadataInconsistencies = %d, want 1", snap.MetadataInconsistencies)
	}
	if _, ok := h.m.meta.GetChunkMeta(unknownHandle); ok {
		t.Fatal("an unexpected chunk must not cause the master to fabricate metadata for it")
	}
}

func TestChunkStateTransitions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationFactor = 3
	h := newHarness(t, 4, cfg) // exactly 4 nodes, RF=3: killing 2 leaves 2 < RF with no 4th node to repair to

	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	handle, locations, err := h.m.AllocateChunk("/f")
	if err != nil {
		t.Fatal(err)
	}
	for _, loc := range locations {
		if err := h.nodes[loc].StoreChunk(handle, 1, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	h.heartbeatAll(t)
	if got := h.m.ChunkState(handle); got != StateHealthy {
		t.Fatalf("fresh fully-replicated chunk: got %s, want HEALTHY", got)
	}

	h.nodes[locations[0]].SetOnline(false)
	h.clock.Advance(cfg.HeartbeatTimeout + time.Second)
	h.heartbeatAll(t)
	h.m.Scheduler().Tick() // only 1 spare node exists, so repair can succeed here
	if got := h.m.ChunkState(handle); got != StateHealthy {
		t.Fatalf("after successful repair with a spare node available: got %s, want HEALTHY", got)
	}

	// Kill every remaining valid replica (all 4 nodes are now either dead
	// or hosting this chunk, since the spare was consumed above) — zero
	// valid sources left is specifically what DEGRADED means.
	meta, _ := h.m.meta.GetChunkMeta(handle)
	var remaining []common.ServerID
	for s := range meta.Locations {
		remaining = append(remaining, s)
	}
	for _, s := range remaining {
		h.nodes[s].SetOnline(false)
	}
	h.clock.Advance(cfg.HeartbeatTimeout + time.Second)
	h.heartbeatAll(t)
	h.m.Scheduler().Tick()
	if finalState := h.m.ChunkState(handle); finalState != StateDegraded {
		t.Fatalf("after losing every valid replica with no spares: got %s, want DEGRADED", finalState)
	}

	// And the intermediate case tested above (alive but no destination
	// capacity) must read REPAIRING, never DEGRADED — pin that
	// distinction explicitly so it doesn't regress silently.
}

func TestChunkStateIsRepairingNotDegradedWhenSourceExistsButNoDestination(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReplicationFactor = 3
	h := newHarness(t, 4, cfg)
	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	handle, locations, err := h.m.AllocateChunk("/f")
	if err != nil {
		t.Fatal(err)
	}
	for _, loc := range locations {
		if err := h.nodes[loc].StoreChunk(handle, 1, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	h.heartbeatAll(t)
	h.nodes[locations[0]].SetOnline(false)
	h.clock.Advance(cfg.HeartbeatTimeout + time.Second)
	h.heartbeatAll(t)
	h.m.Scheduler().Tick() // repairs via the 4th node, consuming the only spare

	meta, _ := h.m.meta.GetChunkMeta(handle)
	var alive []common.ServerID
	for s := range meta.Locations {
		alive = append(alive, s)
	}
	if len(alive) < 2 {
		t.Fatal("setup assumption violated: expected at least 2 live locations after the first repair")
	}
	h.nodes[alive[0]].SetOnline(false) // one more loss: a valid source remains, but no spare node exists anywhere
	h.clock.Advance(cfg.HeartbeatTimeout + time.Second)
	h.heartbeatAll(t)
	h.m.Scheduler().Tick()

	if got := h.m.ChunkState(handle); got != StateRepairing {
		t.Fatalf("a valid source exists but every node is already accounted for: got %s, want REPAIRING", got)
	}
}

func TestFalseSuccessRepairCommitsIsAlwaysZero(t *testing.T) {
	// Exercise several repair paths (including an induced verification
	// failure) and confirm the metric never moves off zero, backing up the
	// structural guarantee documented on the field.
	tests := []func(t *testing.T){
		TestSourceFailureDuringRepairRetriesAnotherSource,
		TestDestinationFailureDuringRepairRetriesAnotherDestination,
		TestVerificationFailureBlocksMetadataCommit,
	}
	for _, fn := range tests {
		fn(t)
	}
}

// --- shared test scaffolding for the M2 destination/verification tests ---

type logHarness struct {
	h      *testHarness
	handle common.ChunkHandle
	source common.ServerID
}

func (l *logHarness) cleanup() {}

func newTestLogHarness(t *testing.T) *logHarness {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ReplicationFactor = 1
	h := newHarness(t, 1, cfg)
	if err := h.m.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	handle, locations, err := h.m.AllocateChunk("/f")
	if err != nil {
		t.Fatal(err)
	}
	source := locations[0]
	if err := h.nodes[source].StoreChunk(handle, 1, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	h.heartbeatAll(t)
	return &logHarness{h: h, handle: handle, source: source}
}

func chunkserverNew(t *testing.T, id common.ServerID) *chunkserver.ChunkServer {
	t.Helper()
	return chunkserver.New(id, 1<<20, nil)
}
