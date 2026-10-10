package master

import (
	"sort"
	"sync/atomic"
	"time"

	"project18/internal/chunkserver"
	"project18/internal/common"
	"project18/internal/events"
	"project18/internal/metrics"
	"project18/internal/network"
)

// ReplicationManager is the replaceable interface for spec sections 5 and 8:
// deciding which chunks are under-replicated and driving re-replication.
//
// M2 changes what happens inside CheckAndSchedule (an explicit repair
// queue, source/destination retry, verification before metadata commit —
// see repair.go and docs/experiments/M2-distributed-consistency.md) but
// deliberately keeps this interface's shape and CheckAndSchedule's return
// value unchanged from M0/M1, so Master and every M0/M1 test that depends
// on this contract keep working unmodified.
type ReplicationManager interface {
	// CheckAndSchedule scans all chunks, repairs any under-replicated one
	// it can (with retry across available sources/destinations within this
	// pass), and returns how many chunks needed repair when scanned —
	// which is NOT the same as how many are still under-replicated after
	// the pass; see Unresolved for that.
	CheckAndSchedule(now time.Time) (underReplicated int)
}

// NodeLookup resolves a server ID to its live StorageNode handle. Supplied
// by whatever owns the chunkserver registry (the Master) so ReplicationManager
// doesn't need to know how servers are registered.
type NodeLookup func(common.ServerID) (chunkserver.StorageNode, bool)

// GFSReplicationManager is the baseline: target replication factor, GFS-style
// random placement for new replicas, an explicit repair queue (M2) with
// source/destination retry and verification before any metadata commit.
type GFSReplicationManager struct {
	target    int
	meta      MetadataStore
	fd        FailureDetector
	placement ReplicaPlacementPolicy
	nodes     NodeLookup
	net       *network.InProcessNetwork
	log       *events.Log
	metrics   *metrics.Metrics
	masterID  common.ServerID
	queue     RepairQueue

	// unresolved is the number of chunks still below the replication target
	// (per the failure detector) at the END of the most recent pass. Added in
	// M1: CheckAndSchedule's return value counts chunks that needed repair
	// when scanned, whether or not the repair then succeeded, so it can't be
	// used to tell when recovery actually finished.
	unresolved atomic.Int64
}

// Unresolved reports how many chunks were still under-replicated after the
// most recent CheckAndSchedule pass.
func (rm *GFSReplicationManager) Unresolved() int { return int(rm.unresolved.Load()) }

// Pending exposes the repair queue's current backlog, for M2 experiments
// and the demo. Not part of the core ReplicationManager interface (see the
// type-assertion pattern used for Unresolved in master.go) so other
// implementations aren't forced to carry a queue concept.
func (rm *GFSReplicationManager) Pending() []common.ChunkHandle { return rm.queue.Pending() }

// JobAttempts reports the cumulative attempt count for handle's repair job,
// or 0 if none is queued (either never needed one, or already finished).
func (rm *GFSReplicationManager) JobAttempts(handle common.ChunkHandle) int {
	j, ok := rm.queue.Get(handle)
	if !ok {
		return 0
	}
	return j.Attempts
}

// State computes handle's derived ChunkState (see repair.go). Safe to call
// for any handle the metadata store knows about.
func (rm *GFSReplicationManager) State(handle common.ChunkHandle) ChunkState {
	meta, ok := rm.meta.GetChunkMeta(handle)
	if !ok {
		return StateHealthy
	}
	alive := 0
	for server := range meta.Locations {
		if rm.fd.IsAlive(server) {
			alive++
		}
	}
	if alive >= rm.target {
		return StateHealthy
	}
	if alive == 0 {
		return StateDegraded
	}
	if _, queued := rm.queue.Get(handle); queued {
		return StateRepairing
	}
	return StateUnderReplicated
}

func NewGFSReplicationManager(target int, meta MetadataStore, fd FailureDetector, placement ReplicaPlacementPolicy, nodes NodeLookup, net *network.InProcessNetwork, log *events.Log, m *metrics.Metrics, masterID common.ServerID) *GFSReplicationManager {
	return &GFSReplicationManager{
		target: target, meta: meta, fd: fd, placement: placement,
		nodes: nodes, net: net, log: log, metrics: m, masterID: masterID,
		queue: NewInMemoryRepairQueue(),
	}
}

func (rm *GFSReplicationManager) CheckAndSchedule(now time.Time) int {
	underReplicated := 0

	for _, handle := range rm.meta.AllChunkHandles() {
		meta, ok := rm.meta.GetChunkMeta(handle)
		if !ok {
			continue
		}

		var alive, dead []common.ServerID
		for server := range meta.Locations {
			if rm.fd.IsAlive(server) {
				alive = append(alive, server)
			} else {
				dead = append(dead, server)
			}
		}
		// M1: deterministic order — the source-selection loop below walks
		// `alive` in order, so unordered map iteration here would make
		// source selection non-reproducible across runs with the same seed.
		sort.Slice(alive, func(i, j int) bool { return alive[i] < alive[j] })
		sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })

		// Drop dead replicas from the metadata's location set — they no
		// longer count toward replication factor.
		for _, d := range dead {
			rm.meta.RemoveReplicaLocation(handle, d)
		}

		if len(alive) >= rm.target {
			rm.queue.Remove(handle) // fully healthy again (or recovered on its own) — no job needed
			continue
		}
		if len(alive) == 0 {
			underReplicated++ // all replicas lost; nothing to copy from (DEGRADED)
			if len(dead) > 0 {
				rm.log.Logf("MASTER", "%s has NO live replicas left (lost %v) — cannot repair (DEGRADED)", handle, dead)
			}
			continue
		}

		underReplicated++
		need := rm.target - len(alive)
		job, created := rm.queue.Enqueue(handle, rm.target, meta.Version, now)
		if created {
			rm.log.Logf("MASTER", "%s under-replicated: %d/%d live (%v) — repair job enqueued (required version %d)", handle, len(alive), rm.target, alive, job.RequiredVersion)
		}

		filled := rm.repairChunk(handle, meta.Version, alive, need)
		if len(alive)+filled >= rm.target {
			rm.queue.Remove(handle)
		}
	}

	if rm.metrics != nil {
		rm.metrics.SetUnderReplicated(uint64(underReplicated))
		rm.metrics.SetRepairQueueDepth(uint64(len(rm.queue.Pending())))
	}

	stillUnder := 0
	for _, handle := range rm.meta.AllChunkHandles() {
		cm, ok := rm.meta.GetChunkMeta(handle)
		if !ok {
			continue
		}
		live := 0
		for server := range cm.Locations {
			if rm.fd.IsAlive(server) {
				live++
			}
		}
		if live < rm.target {
			stillUnder++
		}
	}
	rm.unresolved.Store(int64(stillUnder))
	return underReplicated
}

// repairChunk tries to fill `need` additional replicas of handle (whose
// authoritative version is requiredVersion), given the currently alive
// locations in `alive`. It is the M2 repair pipeline's Source Selection ->
// Destination Selection -> Copy -> Verify -> Metadata Commit stage, with
// retry: a source that fails validity/read is blacklisted for the rest of
// this pass and a different alive location is tried; a destination that
// fails store/verify is excluded and a different destination is tried
// (same source, if it's still good). Returns how many replicas were
// actually filled and committed this pass.
func (rm *GFSReplicationManager) repairChunk(handle common.ChunkHandle, requiredVersion common.ChunkVersion, alive []common.ServerID, need int) int {
	badSources := map[common.ServerID]bool{}
	excludedDests := map[common.ServerID]bool{}
	for _, a := range alive {
		excludedDests[a] = true // never "re-place" a replica onto a node that already has one
	}

	filled := 0
	for filled < need {
		src, ok := firstGood(alive, badSources)
		if !ok {
			rm.log.Logf("MASTER", "%s: no more valid sources to try this pass (filled %d/%d)", handle, filled, need)
			break
		}

		srcNode, ok := rm.nodes(src)
		if !ok {
			badSources[src] = true
			continue
		}
		// Explicit staleness guard (M2 rule: never copy from a stale
		// replica), checked against the physical node rather than trusted
		// from stale metadata — hardens the invariant instead of relying
		// on heartbeat-reconciliation having already run this same tick.
		if v, has := srcNode.HasChunk(handle); !has || v != requiredVersion {
			rm.log.Logf("MASTER", "%s: source %s failed validity check (has=%v version=%d want=%d) — trying another source", handle, src, has, v, requiredVersion)
			rm.recordFailedAttempt(handle)
			badSources[src] = true
			continue
		}
		data, version, err := srcNode.ReadChunk(handle)
		if err != nil {
			rm.log.Logf("MASTER", "%s: read from source %s failed: %v — trying another source", handle, src, err)
			rm.recordFailedAttempt(handle)
			badSources[src] = true
			continue
		}

		candidates := rm.onlineCandidates()
		dests, derr := rm.placement.Choose(candidates, excludedDests, 1)
		if derr != nil {
			rm.log.Logf("MASTER", "%s: no destination candidates remain this pass: %v", handle, derr)
			rm.queue.RecordAttempt(handle)
			break // no point trying more sources if there's nowhere to put the data
		}
		if rm.metrics != nil {
			rm.metrics.IncPlacementDecision()
		}
		dest := dests[0]
		destNode, ok := rm.nodes(dest)
		if !ok {
			excludedDests[dest] = true
			continue
		}

		rm.queue.RecordAttempt(handle)
		if rm.metrics != nil {
			rm.metrics.IncReReplicationStart()
			rm.metrics.IncRepairAttempt()
		}
		storeErr := rm.net.Call(rm.masterID, dest, func() error {
			return destNode.StoreChunk(handle, version, data)
		})
		if storeErr != nil {
			rm.log.Logf("MASTER", "%s: copy to destination %s failed: %v — trying another destination", handle, dest, storeErr)
			if rm.metrics != nil {
				rm.metrics.IncReReplicationFailed()
			}
			excludedDests[dest] = true
			continue
		}

		// Verify before committing metadata — M2 invariants 3 & 4: a
		// successful repair results in a VERIFIED replica, and metadata
		// never claims a replica is valid before that verification.
		gotVersion, held := destNode.HasChunk(handle)
		if !held || gotVersion != version {
			rm.log.Logf("MASTER", "%s: verification FAILED on %s (held=%v version=%d want=%d) — NOT committing metadata", handle, dest, held, gotVersion, version)
			if rm.metrics != nil {
				rm.metrics.IncReReplicationFailed()
				rm.metrics.IncVerificationFailure()
			}
			excludedDests[dest] = true
			continue
		}

		rm.meta.AddReplicaLocation(handle, dest)
		rm.log.Logf(string(dest), "replica %s verified at v%d (repair)", handle, version)
		rm.log.Logf("MASTER", "%s repaired via %s (verified, attempt total so far: %d)", handle, dest, rm.JobAttempts(handle))
		if rm.metrics != nil {
			rm.metrics.IncReReplicationOK()
			rm.metrics.IncVerifiedRepairCommit()
		}
		filled++
		excludedDests[dest] = true
	}
	return filled
}

// recordFailedAttempt bumps both the repair job's attempt count and the
// ReReplicationFailed metric — used for failures that happen before a
// StoreChunk is even attempted (bad source), so those still count as real
// repair effort.
func (rm *GFSReplicationManager) recordFailedAttempt(handle common.ChunkHandle) {
	rm.queue.RecordAttempt(handle)
	if rm.metrics != nil {
		rm.metrics.IncReReplicationFailed()
		rm.metrics.IncRepairAttempt()
	}
}

// firstGood returns the first entry of `ordered` not present in `bad`.
func firstGood(ordered []common.ServerID, bad map[common.ServerID]bool) (common.ServerID, bool) {
	for _, s := range ordered {
		if !bad[s] {
			return s, true
		}
	}
	return "", false
}

func (rm *GFSReplicationManager) onlineCandidates() []common.ServerID {
	var out []common.ServerID
	for _, s := range rm.fd.KnownServers() {
		if rm.fd.IsAlive(s) {
			out = append(out, s)
		}
	}
	return out
}
