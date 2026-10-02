// Package master implements the GFS baseline master: the central metadata
// authority that coordinates (but never stores) file data. Everything here
// is built against the replaceable interfaces defined alongside it
// (MetadataStore, ReplicaPlacementPolicy, FailureDetector,
// ReplicationManager, LeaseManager, Scheduler) so that "replace the GFS
// master's placement policy" or "replace the scheduler" doesn't require
// touching Master itself.
package master

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"project18/internal/chunkserver"
	"project18/internal/common"
	"project18/internal/events"
	"project18/internal/metrics"
	"project18/internal/network"
)

// Config holds the tunable parameters section 4 (chunking), 5 (replication),
// 6/7 (heartbeats/failure detection) and 9 (leases) call for.
type Config struct {
	ReplicationFactor int
	ChunkSize         int // bytes; client rolls to a new chunk past this size
	HeartbeatTimeout  time.Duration
	LeaseTTL          time.Duration
	CheckpointEvery   int // background ticks between checkpoints
	PlacementSeed     int64
}

func DefaultConfig() Config {
	return Config{
		ReplicationFactor: 3,
		ChunkSize:         64, // deliberately tiny for a simulator, not 64MB
		HeartbeatTimeout:  3 * time.Second,
		LeaseTTL:          10 * time.Second,
		CheckpointEvery:   5,
		PlacementSeed:     1,
	}
}

type Master struct {
	id  common.ServerID
	cfg Config

	mu    sync.RWMutex
	alive bool
	nodes map[common.ServerID]chunkserver.StorageNode

	meta        MetadataStore
	fd          FailureDetector
	leases      LeaseManager
	placement   ReplicaPlacementPolicy
	replication ReplicationManager
	scheduler   Scheduler

	net     *network.InProcessNetwork
	log     *events.Log
	metrics *metrics.Metrics
	clock   common.Clock

	opLog                *OperationLog
	lastCheckpoint       CheckpointRecord
	ticksSinceCheckpoint int

	// recoveryPending/recoveryStart bracket one failure-to-full-recovery
	// incident: set when a failure is first detected while no incident is
	// already open, cleared (and timed) once CheckAndSchedule reports zero
	// under-replicated chunks again. Feeds Metrics.RecordRecovery — see
	// docs/experiments/M1-replica-placement.md for why this was previously
	// declared but never wired up.
	recoveryPending bool
	recoveryStart   time.Time
}

func NewMaster(id common.ServerID, net *network.InProcessNetwork, log *events.Log, m *metrics.Metrics, cfg Config) *Master {
	meta := NewInMemoryMetadataStore()
	fd := NewHeartbeatFailureDetector(cfg.HeartbeatTimeout)
	leases := NewGFSLeaseManager()
	placement := NewGFSReplicaPlacementPolicy(cfg.PlacementSeed)

	master := &Master{
		id:        id,
		cfg:       cfg,
		alive:     true,
		nodes:     make(map[common.ServerID]chunkserver.StorageNode),
		meta:      meta,
		fd:        fd,
		leases:    leases,
		placement: placement,
		net:       net,
		log:       log,
		metrics:   m,
		clock:     common.RealClock{},
		opLog:     NewOperationLog(),
	}
	master.replication = NewGFSReplicationManager(cfg.ReplicationFactor, meta, fd, placement, master.lookupNode, net, log, m, id)
	master.scheduler = NewPeriodicScheduler(cfg.HeartbeatTimeout/2, master.backgroundTick)
	return master
}

func (m *Master) ID() common.ServerID  { return m.id }
func (m *Master) Scheduler() Scheduler { return m.scheduler }

// SetClock overrides the master's time source. Intended for deterministic
// tests (see spec: "deterministic where possible") — production/demo code
// should not need this, RealClock is the default.
func (m *Master) SetClock(c common.Clock) { m.clock = c }

func (m *Master) requireAlive() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.alive {
		return fmt.Errorf("master %s is unavailable", m.id)
	}
	return nil
}

func (m *Master) lookupNode(id common.ServerID) (chunkserver.StorageNode, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[id]
	return n, ok
}

// RegisterChunkServer adds a chunkserver to the master's registry — the
// simulation-setup equivalent of a chunkserver announcing itself for the
// first time.
func (m *Master) RegisterChunkServer(node chunkserver.StorageNode) {
	m.mu.Lock()
	m.nodes[node.ID()] = node
	m.mu.Unlock()
	m.fd.RecordHeartbeat(node.ID(), m.clock.Now())
	m.log.Logf("MASTER", "registered chunkserver %s", node.ID())
}

func (m *Master) onlineServerIDs() []common.ServerID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []common.ServerID
	for id := range m.nodes {
		if m.fd.IsAlive(id) {
			out = append(out, id)
		}
	}
	// M1: deterministic order for reproducible experiments — see
	// AllChunkHandles in metadata_store.go for the full rationale.
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// IsServerAlive reports whether the master currently believes server is
// alive (per its FailureDetector). Exposed read-only for the M1 experiment
// layer to time failure detection precisely, without giving experiments
// write access to master internals.
func (m *Master) IsServerAlive(server common.ServerID) bool {
	return m.fd.IsAlive(server)
}

// --- Client-facing operations ---

func (m *Master) CreateFile(path common.Path) error {
	if err := m.requireAlive(); err != nil {
		return err
	}
	if err := m.meta.CreateFile(path); err != nil {
		return err
	}
	m.opLog.Append(Operation{Type: OpCreateFile, Path: path})
	m.metrics.IncMasterOp()
	m.log.Logf("MASTER", "created file %s", path)
	return nil
}

// AllocateChunk allocates a brand-new chunk, appends it to path, and picks
// its initial replica placement.
func (m *Master) AllocateChunk(path common.Path) (common.ChunkHandle, []common.ServerID, error) {
	if err := m.requireAlive(); err != nil {
		return 0, nil, err
	}
	if !m.meta.FileExists(path) {
		return 0, nil, fmt.Errorf("file %s does not exist", path)
	}
	candidates := m.onlineServerIDs()
	locations, err := m.placement.Choose(candidates, nil, m.cfg.ReplicationFactor)
	if err != nil {
		return 0, nil, fmt.Errorf("chunk allocation failed: %w", err)
	}

	handle := m.meta.NewChunkHandle()
	const initialVersion = common.ChunkVersion(1)
	m.meta.InitChunk(handle, initialVersion, locations)
	m.opLog.Append(Operation{Type: OpInitChunk, Handle: handle, Version: initialVersion, Locations: locations})
	if err := m.meta.AppendChunkToFile(path, handle); err != nil {
		return 0, nil, err
	}
	m.opLog.Append(Operation{Type: OpAppendChunk, Path: path, Handle: handle})
	m.metrics.IncMasterOp()
	if m.metrics != nil {
		m.metrics.IncPlacementDecision()
	}

	m.log.Logf("MASTER", "allocated chunk %s for %s", handle, path)
	m.log.Logf("MASTER", "selected replicas %v for %s", locations, handle)
	return handle, locations, nil
}

// GetChunkLocations returns the chunk handle for the given chunk index of
// path, plus the replicas currently believed alive.
func (m *Master) GetChunkLocations(path common.Path, index int) (common.ChunkHandle, []common.ServerID, error) {
	if err := m.requireAlive(); err != nil {
		return 0, nil, err
	}
	handles, err := m.meta.ChunkHandles(path)
	if err != nil {
		return 0, nil, err
	}
	if index < 0 || index >= len(handles) {
		return 0, nil, fmt.Errorf("chunk index %d out of range for %s (%d chunks)", index, path, len(handles))
	}
	handle := handles[index]
	meta, ok := m.meta.GetChunkMeta(handle)
	if !ok {
		return 0, nil, fmt.Errorf("no metadata for %s", handle)
	}
	var alive []common.ServerID
	for server := range meta.Locations {
		if m.fd.IsAlive(server) {
			alive = append(alive, server)
		}
	}
	sort.Slice(alive, func(i, j int) bool { return alive[i] < alive[j] })
	m.metrics.IncMasterOp()
	return handle, alive, nil
}

func (m *Master) ChunkCount(path common.Path) (int, error) {
	handles, err := m.meta.ChunkHandles(path)
	if err != nil {
		return 0, err
	}
	return len(handles), nil
}

// RequestLease returns the active lease for handle, granting a new one
// (bumping the chunk's version) if none is currently active.
func (m *Master) RequestLease(handle common.ChunkHandle) (LeaseInfo, error) {
	if err := m.requireAlive(); err != nil {
		return LeaseInfo{}, err
	}
	now := m.clock.Now()
	if lease, ok := m.leases.Get(handle, now); ok {
		return lease, nil
	}
	meta, ok := m.meta.GetChunkMeta(handle)
	if !ok {
		return LeaseInfo{}, fmt.Errorf("no metadata for %s", handle)
	}
	var alive []common.ServerID
	for server := range meta.Locations {
		if m.fd.IsAlive(server) {
			alive = append(alive, server)
		}
	}
	sort.Slice(alive, func(i, j int) bool { return alive[i] < alive[j] })
	if len(alive) == 0 {
		return LeaseInfo{}, fmt.Errorf("%s has no alive replicas — cannot grant lease", handle)
	}
	version := m.meta.BumpVersion(handle)
	m.opLog.Append(Operation{Type: OpBumpVersion, Handle: handle, Version: version})
	primary := alive[0]
	secondaries := alive[1:]
	lease := m.leases.Grant(handle, primary, secondaries, version, now, m.cfg.LeaseTTL)
	m.metrics.IncMasterOp()
	m.log.Logf("MASTER", "granted lease for %s: primary=%s secondaries=%v version=%d", handle, primary, secondaries, version)
	return lease, nil
}

// Heartbeat records a chunkserver's liveness and reported chunk versions,
// detecting stale replicas per spec section 10.
func (m *Master) Heartbeat(server common.ServerID, reported map[common.ChunkHandle]common.ChunkVersion, now time.Time) error {
	if err := m.requireAlive(); err != nil {
		return err
	}
	wasAlive := m.fd.IsAlive(server)
	m.fd.RecordHeartbeat(server, now)
	if !wasAlive {
		m.log.Logf("MASTER", "%s heartbeat resumed", server)
	}

	for handle, version := range reported {
		meta, ok := m.meta.GetChunkMeta(handle)
		if !ok {
			continue
		}
		switch {
		case version < meta.Version:
			m.log.Logf("MASTER", "%s reported STALE replica of %s (has v%d, master has v%d)", server, handle, version, meta.Version)
			m.meta.RemoveReplicaLocation(handle, server)
			m.opLog.Append(Operation{Type: OpRemoveReplica, Handle: handle, Server: server})
			if m.metrics != nil {
				m.metrics.IncStale()
			}
		case meta.Locations[server]:
			// already tracked at current version, nothing to do
		default:
			m.meta.AddReplicaLocation(handle, server)
			m.opLog.Append(Operation{Type: OpAddReplica, Handle: handle, Server: server})
		}
	}
	return nil
}

// --- Background activity (driven by Scheduler) ---

func (m *Master) backgroundTick() {
	m.mu.RLock()
	alive := m.alive
	m.mu.RUnlock()
	if !alive {
		return
	}

	now := m.clock.Now()
	dead := m.fd.Scan(now)
	for _, server := range dead {
		last, _ := m.fd.LastHeartbeat(server)
		m.log.Logf("MASTER", "%s heartbeat timeout — marking FAILED", server)
		if m.metrics != nil {
			m.metrics.RecordDetection(now.Sub(last))
		}
		if !m.recoveryPending {
			m.recoveryPending = true
			m.recoveryStart = now
		}
		if affected := m.leases.RevokeForServer(server); len(affected) > 0 {
			m.log.Logf("MASTER", "revoked leases for %v (primary %s failed)", affected, server)
		}
	}

	underReplicated := m.replication.CheckAndSchedule(now)
	if underReplicated == 0 && len(dead) > 0 {
		m.log.Logf("MASTER", "replication fully restored, 0 under-replicated chunks")
	}
	resolved := underReplicated
	if r, ok := m.replication.(interface{ Unresolved() int }); ok {
		resolved = r.Unresolved()
	}
	if resolved == 0 && m.recoveryPending {
		duration := now.Sub(m.recoveryStart)
		if m.metrics != nil {
			m.metrics.RecordRecovery(duration)
		}
		m.log.Logf("MASTER", "recovery complete: %v since first failure detection", duration)
		m.recoveryPending = false
	}

	m.ticksSinceCheckpoint++
	if m.ticksSinceCheckpoint >= m.cfg.CheckpointEvery {
		m.Checkpoint()
		m.ticksSinceCheckpoint = 0
	}
}

// --- Persistence (spec section 11) ---

func (m *Master) Checkpoint() {
	snap := m.meta.Snapshot()
	afterSeq := m.opLog.CurrentSeq()
	m.lastCheckpoint = CheckpointRecord{Snapshot: snap, AfterSeq: afterSeq}
	m.log.Logf("MASTER", "checkpoint created at op %d (%d files, %d chunks)", afterSeq, len(snap.Files), len(snap.Chunks))
}

// Kill simulates a master crash: every client/chunkserver-facing operation
// starts returning an error until Recover is called.
func (m *Master) Kill() {
	m.mu.Lock()
	m.alive = false
	m.mu.Unlock()
	m.log.Logf("MASTER", "FAILURE (killed)")
}

// Recover simulates a master restart: metadata is rebuilt from the last
// checkpoint plus replay of the operation log recorded since. Leases are
// intentionally NOT restored — a restarted master, like real GFS, forgets
// in-flight lease grants and callers must re-request them. Chunkserver
// liveness (failure-detector state) is intentionally preserved: modeling a
// full "master re-polls every chunkserver on startup" handshake is left as
// a deliberate simplification, noted here rather than silently glossed
// over.
func (m *Master) Recover() {
	fresh := NewInMemoryMetadataStore()
	fresh.Restore(m.lastCheckpoint.Snapshot)
	for _, op := range m.opLog.Since(m.lastCheckpoint.AfterSeq) {
		Apply(fresh, op)
	}
	m.meta = fresh
	m.leases = NewGFSLeaseManager()
	m.replication = NewGFSReplicationManager(m.cfg.ReplicationFactor, m.meta, m.fd, m.placement, m.lookupNode, m.net, m.log, m.metrics, m.id)

	m.mu.Lock()
	m.alive = true
	m.mu.Unlock()
	m.log.Logf("MASTER", "restarted — restored checkpoint (op %d) + replayed %d newer ops; leases cleared", m.lastCheckpoint.AfterSeq, m.opLog.CurrentSeq()-m.lastCheckpoint.AfterSeq)
}

func (m *Master) IsAlive() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.alive
}
