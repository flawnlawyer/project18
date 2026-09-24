package sim

import (
	"testing"
	"time"

	"project18/internal/common"
	"project18/internal/master"
)

func TestWriteReadRoundTrip(t *testing.T) {
	cfg := master.DefaultConfig()
	cfg.ChunkSize = 8
	s := New(4, 1<<20, cfg)
	s.SetClock(common.NewFakeClock(time.Unix(0, 0)))

	path := common.Path("/f")
	if err := s.Client.CreateFile(path); err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello distributed world")
	if err := s.Client.Write(path, payload); err != nil {
		t.Fatal(err)
	}
	got, err := s.Client.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestFullFailureAndRecoveryLifecycle(t *testing.T) {
	cfg := master.DefaultConfig()
	cfg.ReplicationFactor = 3
	cfg.ChunkSize = 8
	cfg.HeartbeatTimeout = 10 * time.Second
	s := New(5, 1<<20, cfg)
	clock := common.NewFakeClock(time.Unix(0, 0))
	s.SetClock(clock)

	path := common.Path("/f")
	if err := s.Client.CreateFile(path); err != nil {
		t.Fatal(err)
	}
	if err := s.Client.Write(path, []byte("some data to replicate")); err != nil {
		t.Fatal(err)
	}
	s.Tick() // steady-state heartbeat

	victim := common.ServerID("CS1")
	if err := s.KillChunkServer(victim); err != nil {
		t.Fatal(err)
	}

	clock.Advance(cfg.HeartbeatTimeout + time.Second)
	s.Tick() // survivors heartbeat, failure detector times out victim, re-replication runs

	handle, locs, err := s.Master.GetChunkLocations(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(locs) != cfg.ReplicationFactor {
		t.Fatalf("after failure+re-replication, %s has %d live locations, want %d", handle, len(locs), cfg.ReplicationFactor)
	}
	for _, l := range locs {
		if l == victim {
			t.Fatalf("killed server %s still listed as a live location", victim)
		}
	}

	// Read must still succeed — data survived the failure via re-replication.
	got, err := s.Client.Read(path)
	if err != nil {
		t.Fatalf("read after failure should still succeed: %v", err)
	}
	if string(got) != "some data to replicate" {
		t.Fatalf("data corrupted after failure/recovery: %q", got)
	}

	if err := s.RecoverChunkServer(victim); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	node, _ := s.Node(victim)
	if !node.IsOnline() {
		t.Fatal("recovered chunkserver should be online")
	}

	snap := s.Metrics.Snapshot()
	if snap.FailureDetections == 0 {
		t.Fatal("expected a recorded failure detection")
	}
	if snap.ReReplicationsOK == 0 {
		t.Fatal("expected at least one successful re-replication")
	}
}

func TestMasterKillBlocksThenRecoverRestores(t *testing.T) {
	cfg := master.DefaultConfig()
	s := New(3, 1<<20, cfg)

	if err := s.Client.CreateFile("/f"); err != nil {
		t.Fatal(err)
	}
	s.KillMaster()
	if err := s.Client.CreateFile("/g"); err == nil {
		t.Fatal("expected create to fail while master is down")
	}
	s.RecoverMaster()
	if err := s.Client.CreateFile("/g"); err != nil {
		t.Fatalf("create should succeed after master recovers: %v", err)
	}
}
