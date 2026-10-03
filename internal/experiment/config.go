// Package experiment is the M1 experiment layer: it runs controlled,
// reproducible scenarios against the frozen M0 baseline (client, master,
// chunkserver, replication, failure detection — none of it modified here
// except for the additive observability/determinism fixes noted in
// docs/experiments/M1-replica-placement.md) and reports what actually
// happened. Nothing in this package changes M0 semantics; it only drives
// the existing sim.Simulator and reads its public state.
package experiment

import (
	"fmt"
	"time"

	"project18/internal/common"
	"project18/internal/master"
	"project18/internal/sim"
)

// Config is the minimal set of knobs M1 needs, per the spec's "deterministic
// experiment framework" section: node count, replication factor, chunk
// count/size, failure schedule, placement policy, and seed. FailureSchedule
// is expressed as an explicit list of ticks-since-start rather than wall
// time, so a run is reproducible independent of how fast the host executes
// it.
type Config struct {
	NodeCount         int
	CapacityPerServer uint64   // used when Capacities is nil (uniform cluster)
	Capacities        []uint64 // overrides NodeCount/CapacityPerServer when set (Experiment B)
	ReplicationFactor int
	ChunkCount        int
	ChunkPayloadSize  int // bytes of dummy data stored per chunk
	Seed              int64
	HeartbeatTimeout  time.Duration
	CheckpointEvery   int
}

func DefaultConfig() Config {
	return Config{
		NodeCount:         5,
		CapacityPerServer: 1 << 20,
		ReplicationFactor: 3,
		ChunkCount:        30,
		ChunkPayloadSize:  16,
		Seed:              1,
		HeartbeatTimeout:  3 * time.Second,
		CheckpointEvery:   1000, // effectively off unless an experiment wants it
	}
}

// Build constructs a fresh simulator from cfg: a fake clock started at a
// fixed instant (so every run starts from identical time, not time.Now()),
// and cfg.Seed threaded through to the placement policy. This is the one
// entry point every experiment function uses, so "same Config -> same
// starting state" holds by construction.
func Build(cfg Config) (*sim.Simulator, *common.FakeClock) {
	mcfg := master.DefaultConfig()
	mcfg.ReplicationFactor = cfg.ReplicationFactor
	mcfg.ChunkSize = cfg.ChunkPayloadSize
	mcfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	mcfg.CheckpointEvery = cfg.CheckpointEvery
	mcfg.PlacementSeed = cfg.Seed

	var s *sim.Simulator
	if cfg.Capacities != nil {
		s = sim.NewWithCapacities(cfg.Capacities, mcfg)
	} else {
		s = sim.New(cfg.NodeCount, cfg.CapacityPerServer, mcfg)
	}

	clock := common.NewFakeClock(time.Unix(0, 0))
	s.SetClock(clock)
	return s, clock
}

// populateChunks creates path and allocates n chunks on it, each with
// cfg.ChunkPayloadSize bytes of real (dummy) data on every initial replica,
// via Simulator.AllocateChunkWithData — so placement decisions are made by
// the normal master/policy path, not hand-constructed.
func populateChunks(s *sim.Simulator, path common.Path, n int, payloadSize int) ([]common.ChunkHandle, error) {
	if err := s.Client.CreateFile(path); err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	handles := make([]common.ChunkHandle, 0, n)
	for i := 0; i < n; i++ {
		handle, _, err := s.AllocateChunkWithData(path, payload)
		if err != nil {
			return handles, fmt.Errorf("allocate chunk %d: %w", i, err)
		}
		handles = append(handles, handle)
	}
	return handles, nil
}

// tickUntil advances the simulator one heartbeat-interval at a time (via
// the fake clock, no real sleeping) until cond() is true or maxTicks is
// reached. Returns the number of ticks actually taken and whether cond was
// satisfied — callers use the bool to distinguish "recovered" from "gave
// up waiting."
func tickUntil(s *sim.Simulator, clock *common.FakeClock, step time.Duration, maxTicks int, cond func() bool) (ticks int, satisfied bool) {
	for i := 0; i < maxTicks; i++ {
		if cond() {
			return i, true
		}
		clock.Advance(step)
		s.Tick()
	}
	return maxTicks, cond()
}
