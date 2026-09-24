// Package common holds the small set of types shared by every subsystem
// (master, chunkserver, client, network). Keeping these in one place is what
// lets each subsystem be replaced independently later without touching the
// others' code.
package common

import (
	"fmt"
	"time"
)

// ChunkHandle uniquely identifies a chunk for the lifetime of the simulation.
type ChunkHandle uint64

// ServerID identifies a chunkserver (its logical name, e.g. "CS1").
type ServerID string

// Path identifies a file in the master's namespace. Flat namespace for M0 —
// no directories, matching how far the GFS baseline needs to go.
type Path string

// ChunkVersion is bumped by the master every time a new lease is granted for
// a chunk. Replicas that report a lower version than the master's record are
// stale.
type ChunkVersion uint64

// ReplicaLocation pairs a chunkserver with the version it is believed to
// hold. Used when the master reports chunk placement to a client.
type ReplicaLocation struct {
	Server  ServerID
	Version ChunkVersion
}

func (h ChunkHandle) String() string { return fmt.Sprintf("C%d", uint64(h)) }

// Clock is a small seam over time.Now so tests (or a future replay mode) can
// inject deterministic timing without every component calling time.Now()
// directly. RealClock is the only implementation for M0.
type Clock interface {
	Now() time.Time
}

// RealClock implements Clock using the system clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a manually-advanced Clock for deterministic tests — no
// sleeping past real heartbeat timeouts just to exercise failure-detection
// logic.
type FakeClock struct {
	t time.Time
}

func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{t: start} }
func (c *FakeClock) Now() time.Time           { return c.t }
func (c *FakeClock) Advance(d time.Duration)  { c.t = c.t.Add(d) }
