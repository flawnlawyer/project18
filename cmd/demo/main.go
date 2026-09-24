// Command demo runs a scripted walkthrough of the M0 milestone:
// client -> master -> chunk allocation -> replica placement -> chunkservers
// -> read/write -> heartbeats -> chunkserver failure -> failure detection ->
// re-replication -> node recovery — plus a master restart, to exercise
// persistence (section 11) as well.
package main

import (
	"fmt"
	"strings"
	"time"

	"project18/internal/common"
	"project18/internal/master"
	"project18/internal/sim"
)

func section(title string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", 70))
}

func main() {
	cfg := master.DefaultConfig()
	cfg.ReplicationFactor = 3
	cfg.ChunkSize = 16 // tiny, so a short message spans multiple chunks
	cfg.HeartbeatTimeout = 500 * time.Millisecond
	cfg.CheckpointEvery = 3

	s := sim.New(5, 1<<20, cfg) // 5 chunkservers, 1MB capacity each

	section("1. Create file, write data (chunk allocation + replica placement)")
	path := common.Path("/data/test.txt")
	must(s.Client.CreateFile(path))
	payload := []byte("The quick brown fox jumps over the lazy dog. GFS baseline demo.")
	must(s.Client.Write(path, payload))
	n, _ := s.Master.ChunkCount(path)
	fmt.Printf("\n%s split into %d chunks (chunk size = %d bytes)\n", path, n, cfg.ChunkSize)

	section("2. Read it back")
	data, err := s.Client.Read(path)
	must(err)
	fmt.Printf("read %d bytes: %q\nmatches original: %v\n", len(data), string(data), string(data) == string(payload))

	section("3. Heartbeats (steady state)")
	for i := 0; i < 2; i++ {
		s.Tick()
		time.Sleep(cfg.HeartbeatTimeout / 2)
	}
	fmt.Println("2 ticks completed, no failures yet")

	section("4. Kill a chunkserver, watch failure detection + re-replication")
	victim := common.ServerID("CS2")
	must(s.KillChunkServer(victim))
	fmt.Printf("%s killed. Ticking until the master notices (heartbeat timeout = %v)...\n", victim, cfg.HeartbeatTimeout)
	// Tick past the heartbeat timeout so the failure detector's scan fires.
	deadline := time.Now().Add(cfg.HeartbeatTimeout*2 + 200*time.Millisecond)
	for time.Now().Before(deadline) {
		s.Tick()
		time.Sleep(cfg.HeartbeatTimeout / 3)
	}

	section("5. Recover the node")
	must(s.RecoverChunkServer(victim))
	s.Tick()
	fmt.Printf("%s back online and heartbeating\n", victim)

	section("6. Stale replica detection")
	// Grab a real handle from the file and show what happens when a replica
	// reports an older version than the master expects.
	handle, locs, _ := s.Master.GetChunkLocations(path, 0)
	if len(locs) > 0 {
		must(s.CreateStaleReplica(handle, victim, 0, []byte("old data")))
		s.Tick()
		fmt.Printf("%s reported a stale copy of %s — see event log for the master's response\n", victim, handle)
	}

	section("7. Master persistence: checkpoint, kill, restart")
	filesBefore, chunksBefore := s.Master.ChunkCount(path)
	fmt.Printf("chunks in %s before restart: %d (err=%v)\n", path, filesBefore, chunksBefore)
	s.Master.Checkpoint()
	s.KillMaster()
	fmt.Println("master killed — client ops now fail:")
	if err := s.Client.CreateFile("/data/after-crash.txt"); err != nil {
		fmt.Printf("  -> as expected: %v\n", err)
	}
	s.RecoverMaster()
	filesAfter, chunksAfterErr := s.Master.ChunkCount(path)
	fmt.Printf("chunks in %s after restart: %d (err=%v) — metadata survived\n", path, filesAfter, chunksAfterErr)
	dataAfter, err := s.Client.Read(path)
	fmt.Printf("re-read after restart: %q (err=%v)\n", string(dataAfter), err)

	section("Final metrics")
	fmt.Println(s.Metrics.Snapshot())

	section("Full event log")
	s.Log.Dump()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
