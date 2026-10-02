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
type ReplicationManager interface {
	// CheckAndSchedule scans all chunks, re-replicates any under-replicated
	// one it can, and returns how many chunks are (still) under-replicated
	// after the pass.
	CheckAndSchedule(now time.Time) (underReplicated int)
}

// NodeLookup resolves a server ID to its live StorageNode handle. Supplied
// by whatever owns the chunkserver registry (the Master) so ReplicationManager
// doesn't need to know how servers are registered.
type NodeLookup func(common.ServerID) (chunkserver.StorageNode, bool)

// GFSReplicationManager is the baseline: target replication factor, GFS-style
// random placement for new replicas, synchronous "copy" from any alive
// replica to a newly chosen destination.
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

func NewGFSReplicationManager(target int, meta MetadataStore, fd FailureDetector, placement ReplicaPlacementPolicy, nodes NodeLookup, net *network.InProcessNetwork, log *events.Log, m *metrics.Metrics, masterID common.ServerID) *GFSReplicationManager {
	return &GFSReplicationManager{
		target: target, meta: meta, fd: fd, placement: placement,
		nodes: nodes, net: net, log: log, metrics: m, masterID: masterID,
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
		// M1: deterministic order — alive[0] is used as the re-replication
		// source below, so unordered map iteration here would make source
		// selection (and therefore which server absorbs the read load)
		// non-reproducible across runs with the same seed.
		sort.Slice(alive, func(i, j int) bool { return alive[i] < alive[j] })
		sort.Slice(dead, func(i, j int) bool { return dead[i] < dead[j] })

		// Drop dead replicas from the metadata's location set — they no
		// longer count toward replication factor.
		for _, d := range dead {
			rm.meta.RemoveReplicaLocation(handle, d)
		}

		if len(alive) >= rm.target || len(alive) == 0 {
			if len(alive) < rm.target {
				underReplicated++ // all replicas lost; nothing to copy from
				if len(dead) > 0 {
					rm.log.Logf("MASTER", "%s has NO live replicas left (lost %v) — cannot re-replicate", handle, dead)
				}
			}
			continue
		}

		underReplicated++
		need := rm.target - len(alive)
		rm.log.Logf("MASTER", "%s under-replicated: %d/%d live (%v) — scheduling re-replication", handle, len(alive), rm.target, alive)

		exclude := make(map[common.ServerID]bool, len(alive))
		for _, a := range alive {
			exclude[a] = true
		}
		candidates := rm.onlineCandidates()
		dests, err := rm.placement.Choose(candidates, exclude, need)
		if err != nil {
			rm.log.Logf("MASTER", "%s re-replication failed: %v", handle, err)
			if rm.metrics != nil {
				rm.metrics.IncReReplicationFailed()
			}
			continue
		}
		if rm.metrics != nil {
			rm.metrics.IncPlacementDecision()
		}

		source, ok := rm.nodes(alive[0])
		if !ok {
			continue
		}
		data, version, err := source.ReadChunk(handle)
		if err != nil {
			rm.log.Logf("MASTER", "%s re-replication read from %s failed: %v", handle, alive[0], err)
			if rm.metrics != nil {
				rm.metrics.IncReReplicationFailed()
			}
			continue
		}

		for _, dest := range dests {
			if rm.metrics != nil {
				rm.metrics.IncReReplicationStart()
			}
			node, ok := rm.nodes(dest)
			if !ok {
				if rm.metrics != nil {
					rm.metrics.IncReReplicationFailed()
				}
				continue
			}
			err := rm.net.Call(rm.masterID, dest, func() error {
				return node.StoreChunk(handle, version, data)
			})
			if err != nil {
				rm.log.Logf("MASTER", "%s re-replication to %s failed: %v", handle, dest, err)
				if rm.metrics != nil {
					rm.metrics.IncReReplicationFailed()
				}
				continue
			}
			rm.meta.AddReplicaLocation(handle, dest)
			rm.log.Logf(string(dest), "replica %s created (re-replication, v%d)", handle, version)
			rm.log.Logf("MASTER", "%s replication restored via %s", handle, dest)
			if rm.metrics != nil {
				rm.metrics.IncReReplicationOK()
			}
		}
	}

	if rm.metrics != nil {
		rm.metrics.SetUnderReplicated(uint64(underReplicated))
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

func (rm *GFSReplicationManager) onlineCandidates() []common.ServerID {
	var out []common.ServerID
	for _, s := range rm.fd.KnownServers() {
		if rm.fd.IsAlive(s) {
			out = append(out, s)
		}
	}
	return out
}
