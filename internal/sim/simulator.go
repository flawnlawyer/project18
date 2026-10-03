// Package sim is the experimentation harness: it wires a Master, N
// Chunkservers, and a Client together over a shared NetworkModel/EventLog/
// Metrics, drives heartbeats deterministically via Tick, and exposes the
// failure-injection controls listed in the spec's "Failure Injection"
// section as explicit, named methods rather than random chaos.
package sim

import (
	"fmt"
	"time"

	"project18/internal/chunkserver"
	"project18/internal/client"
	"project18/internal/common"
	"project18/internal/events"
	"project18/internal/master"
	"project18/internal/metrics"
	"project18/internal/network"
)

const MasterID = common.ServerID("MASTER")
const ClientID = common.ServerID("CLIENT")

type Simulator struct {
	Master  *master.Master
	Client  *client.Client
	Log     *events.Log
	Metrics *metrics.Metrics
	Net     *network.InProcessNetwork

	nodes map[common.ServerID]*chunkserver.ChunkServer
	order []common.ServerID // stable iteration order for deterministic demo output
	clock common.Clock
}

// New builds a simulator with numServers chunkservers, each with the given
// capacity (bytes), running under cfg.
func New(numServers int, capacityPerServer uint64, cfg master.Config) *Simulator {
	capacities := make([]uint64, numServers)
	for i := range capacities {
		capacities[i] = capacityPerServer
	}
	return NewWithCapacities(capacities, cfg)
}

// NewWithCapacities builds a simulator with one chunkserver per entry in
// capacities (CS1..CSn, in order), each given that entry's capacity in
// bytes. Added for M1 Experiment B (uneven cluster conditions) — New is
// unchanged and just calls this with a uniform slice, so no M0 behavior
// changes.
func NewWithCapacities(capacities []uint64, cfg master.Config) *Simulator {
	log := events.NewLog()
	met := metrics.New()
	net := network.NewInProcessNetwork(met)
	m := master.NewMaster(MasterID, net, log, met, cfg)

	s := &Simulator{
		Master: m, Log: log, Metrics: met, Net: net,
		nodes: make(map[common.ServerID]*chunkserver.ChunkServer),
		clock: common.RealClock{},
	}

	for i, capacity := range capacities {
		id := common.ServerID(fmt.Sprintf("CS%d", i+1))
		cs := chunkserver.New(id, capacity, log)
		s.nodes[id] = cs
		s.order = append(s.order, id)
		m.RegisterChunkServer(cs)
	}

	s.Client = client.New(ClientID, m, s.lookupNode, net, log, met, cfg.ChunkSize)
	return s
}

// SetClock overrides the simulator's (and its master's) time source, for
// deterministic tests that need to cross heartbeat-timeout boundaries
// without sleeping in real time.
func (s *Simulator) SetClock(c common.Clock) {
	s.clock = c
	s.Master.SetClock(c)
}

func (s *Simulator) lookupNode(id common.ServerID) (chunkserver.StorageNode, bool) {
	n, ok := s.nodes[id]
	if !ok {
		return nil, false
	}
	return n, true
}

// Tick advances the simulation by one step: every online chunkserver sends
// its heartbeat (subject to any injected drop/delay), then the master runs
// its background pass (failure detection + re-replication + checkpointing).
// Calling Tick manually gives fully deterministic, single-step control;
// StartRealtime/StopRealtime below drive the same background pass on a
// wall-clock ticker for a "live" run instead.
func (s *Simulator) Tick() {
	now := s.clock.Now()
	for _, id := range s.order {
		node := s.nodes[id]
		if !node.IsOnline() {
			continue
		}
		reported := make(map[common.ChunkHandle]common.ChunkVersion)
		for _, h := range node.ChunkHandles() {
			v, ok := node.HasChunk(h)
			if ok {
				reported[h] = v
			}
		}
		_ = s.Net.Call(id, MasterID, func() error {
			return s.Master.Heartbeat(id, reported, now)
		})
	}
	s.Master.Scheduler().Tick()
}

// --- Failure injection API (spec "Failure Injection" section) ---

// KillChunkServer takes a chunkserver offline immediately (its data becomes
// unreachable and it stops heartbeating) without the master finding out
// until its next failure-detector scan times out — the realistic sequence.
func (s *Simulator) KillChunkServer(id common.ServerID) error {
	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("no such chunkserver %s", id)
	}
	node.SetOnline(false)
	s.Log.Logf(string(id), "FAILURE")
	return nil
}

// RecoverChunkServer brings a killed chunkserver back online. It rejoins the
// cluster on its next heartbeat (Tick), at which point the master's
// stale-replica check (section 10) evaluates whatever it still holds.
func (s *Simulator) RecoverChunkServer(id common.ServerID) error {
	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("no such chunkserver %s", id)
	}
	node.SetOnline(true)
	s.Log.Logf(string(id), "recovered, resuming heartbeats")
	return nil
}

func (s *Simulator) KillMaster() {
	s.Master.Kill()
}

func (s *Simulator) RecoverMaster() {
	s.Master.Recover()
}

// DropHeartbeat drops every future heartbeat from server to the master,
// without touching the server's online state — models a partitioned link
// rather than a dead node (spec: "heartbeat lost" as distinct from
// "node failure").
func (s *Simulator) DropHeartbeat(server common.ServerID) {
	s.Net.DropLink(server, MasterID)
	s.Log.Logf("NETWORK", "dropping heartbeats from %s to master", server)
}

func (s *Simulator) RestoreHeartbeat(server common.ServerID) {
	s.Net.StopDropping(server, MasterID)
	s.Log.Logf("NETWORK", "heartbeat link %s->master restored", server)
}

// DelayHeartbeat adds latency to server's heartbeats without dropping them.
func (s *Simulator) DelayHeartbeat(server common.ServerID, d time.Duration) {
	s.Net.DelayLink(server, MasterID, d)
	s.Log.Logf("NETWORK", "delaying heartbeats from %s by %v", server, d)
}

// SimulateNetworkDelay adds latency to any link, e.g. client->chunkserver
// data paths, not just heartbeats.
func (s *Simulator) SimulateNetworkDelay(from, to common.ServerID, d time.Duration) {
	s.Net.DelayLink(from, to, d)
	s.Log.Logf("NETWORK", "delaying %s->%s by %v", from, to, d)
}

// CreateStaleReplica directly overwrites a chunk on `server` with an older
// version number, simulating a replica that missed a mutation while
// offline — useful for demonstrating section 10 (stale replica detection)
// without needing a real timing race.
func (s *Simulator) CreateStaleReplica(handle common.ChunkHandle, server common.ServerID, staleVersion common.ChunkVersion, data []byte) error {
	node, ok := s.nodes[server]
	if !ok {
		return fmt.Errorf("no such chunkserver %s", server)
	}
	if err := node.StoreChunk(handle, staleVersion, data); err != nil {
		return err
	}
	s.Log.Logf(string(server), "now holds STALE %s at v%d (injected)", handle, staleVersion)
	return nil
}

// SimulateReplicationFailure makes every future re-replication write to
// `dest` fail, by dropping the master's link to it.
func (s *Simulator) SimulateReplicationFailure(dest common.ServerID) {
	s.Net.DropLink(MasterID, dest)
	s.Log.Logf("NETWORK", "master->%s writes will fail (simulated replication failure)", dest)
}

// Node exposes a chunkserver's StorageNode handle for inspection (tests/demo
// only — normal operation code should go through the master/client).
func (s *Simulator) Node(id common.ServerID) (*chunkserver.ChunkServer, bool) {
	n, ok := s.nodes[id]
	return n, ok
}

func (s *Simulator) ServerIDs() []common.ServerID {
	out := make([]common.ServerID, len(s.order))
	copy(out, s.order)
	return out
}

// AllocateChunkWithData allocates a new chunk for path via the master (so
// placement goes through the normal policy) and immediately stores data on
// every chosen initial replica, bypassing Client's byte-splitting. Used by
// the M1 experiment layer, which needs exact control over chunk *count*
// independent of payload size, while still leaving real bytes on disk so
// later re-replication has something to read.
func (s *Simulator) AllocateChunkWithData(path common.Path, data []byte) (common.ChunkHandle, []common.ServerID, error) {
	handle, locations, err := s.Master.AllocateChunk(path)
	if err != nil {
		return 0, nil, err
	}
	for _, loc := range locations {
		node, ok := s.nodes[loc]
		if !ok {
			continue
		}
		if err := node.StoreChunk(handle, 1, data); err != nil {
			return handle, locations, fmt.Errorf("store initial replica on %s: %w", loc, err)
		}
	}
	return handle, locations, nil
}
