package master

import (
	"sort"
	"sync"
	"time"

	"project18/internal/common"
)

// RepairJob is a request to restore one chunk to its target replication
// factor. The M2 spec's repair pipeline (Reconciliation -> Repair Queue ->
// Scheduler -> Source Selection -> Destination Selection -> Copy -> Verify
// -> Metadata Commit) is implemented by GFSReplicationManager using this
// queue: reconciliation enqueues a job per under-replicated chunk
// (idempotent — see Enqueue), and each scheduler pass drains the queue,
// trying every currently valid source and every currently available
// destination before leaving a still-incomplete job pending for the next
// pass.
type RepairJob struct {
	Handle          common.ChunkHandle
	TargetRF        int
	RequiredVersion common.ChunkVersion
	Attempts        int // cumulative across every pass, every source/destination tried
	FirstEnqueuedAt time.Time
}

// RepairQueue is the replaceable interface for M2's repair-job bookkeeping.
// Keying by chunk handle makes Enqueue naturally idempotent: a chunk that
// already has a job queued never gets a second one (correctness invariant:
// a repair retry must not create duplicate logical repairs), so
// reconciliation can call Enqueue every tick without piling up redundant
// work.
type RepairQueue interface {
	// Enqueue adds a job for handle if one isn't already queued, and
	// reports whether it created a new one.
	Enqueue(handle common.ChunkHandle, targetRF int, version common.ChunkVersion, now time.Time) (job RepairJob, created bool)
	Get(handle common.ChunkHandle) (RepairJob, bool)
	RecordAttempt(handle common.ChunkHandle)
	Remove(handle common.ChunkHandle)
	// Pending returns queued chunk handles in deterministic (sorted) order.
	Pending() []common.ChunkHandle
}

type InMemoryRepairQueue struct {
	mu   sync.Mutex
	jobs map[common.ChunkHandle]RepairJob
}

func NewInMemoryRepairQueue() *InMemoryRepairQueue {
	return &InMemoryRepairQueue{jobs: make(map[common.ChunkHandle]RepairJob)}
}

func (q *InMemoryRepairQueue) Enqueue(handle common.ChunkHandle, targetRF int, version common.ChunkVersion, now time.Time) (RepairJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if j, ok := q.jobs[handle]; ok {
		return j, false
	}
	j := RepairJob{Handle: handle, TargetRF: targetRF, RequiredVersion: version, FirstEnqueuedAt: now}
	q.jobs[handle] = j
	return j, true
}

func (q *InMemoryRepairQueue) Get(handle common.ChunkHandle) (RepairJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, ok := q.jobs[handle]
	return j, ok
}

func (q *InMemoryRepairQueue) RecordAttempt(handle common.ChunkHandle) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if j, ok := q.jobs[handle]; ok {
		j.Attempts++
		q.jobs[handle] = j
	}
}

func (q *InMemoryRepairQueue) Remove(handle common.ChunkHandle) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.jobs, handle)
}

func (q *InMemoryRepairQueue) Pending() []common.ChunkHandle {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]common.ChunkHandle, 0, len(q.jobs))
	for h := range q.jobs {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ChunkState is a derived classification of a chunk's repair status, per
// the M2 metadata model's State field (HEALTHY / UNDER_REPLICATED /
// REPAIRING / DEGRADED). It is deliberately NOT a stored field: it's
// computed on demand from existing metadata + failure-detector + repair
// queue state, so it can never drift out of sync with the data it
// summarizes. See docs/experiments/M2-distributed-consistency.md for why
// this was chosen over persisting state transitions.
type ChunkState string

const (
	StateHealthy         ChunkState = "HEALTHY"
	StateRepairing       ChunkState = "REPAIRING"        // under-replicated, repair queued, a valid source exists
	StateDegraded        ChunkState = "DEGRADED"         // under-replicated, no valid source remains — cannot repair
	StateUnderReplicated ChunkState = "UNDER_REPLICATED" // under-replicated, not (yet) queued — should be transient within one pass
)
