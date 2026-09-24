package master

import (
	"sync"
	"time"

	"project18/internal/common"
)

// FailureDetector is the replaceable interface for deciding whether a
// chunkserver is alive. HeartbeatFailureDetector is the GFS-baseline
// implementation: a server is alive iff it heartbeated within `timeout`.
// Kept separate from the master's RPC handling per spec section 7 ("Do not
// make failure detection an inseparable part of the Master implementation").
type FailureDetector interface {
	RecordHeartbeat(server common.ServerID, at time.Time)
	IsAlive(server common.ServerID) bool
	LastHeartbeat(server common.ServerID) (time.Time, bool)
	// Scan re-evaluates every known server against `now` and returns the set
	// newly detected as dead since the previous Scan (edge-triggered, so
	// callers don't re-process the same failure every tick).
	Scan(now time.Time) []common.ServerID
	// MarkAliveAgain clears the failed record for a server whose heartbeat
	// has resumed without waiting for the next natural heartbeat interval —
	// used when the failure injector explicitly recovers a node.
	MarkAliveAgain(server common.ServerID, at time.Time)
	Forget(server common.ServerID)
	KnownServers() []common.ServerID
}

type HeartbeatFailureDetector struct {
	mu      sync.Mutex
	timeout time.Duration
	last    map[common.ServerID]time.Time
	alive   map[common.ServerID]bool
}

func NewHeartbeatFailureDetector(timeout time.Duration) *HeartbeatFailureDetector {
	return &HeartbeatFailureDetector{
		timeout: timeout,
		last:    make(map[common.ServerID]time.Time),
		alive:   make(map[common.ServerID]bool),
	}
}

func (d *HeartbeatFailureDetector) RecordHeartbeat(server common.ServerID, at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.last[server] = at
	d.alive[server] = true
}

func (d *HeartbeatFailureDetector) IsAlive(server common.ServerID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.alive[server]
}

func (d *HeartbeatFailureDetector) LastHeartbeat(server common.ServerID) (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.last[server]
	return t, ok
}

func (d *HeartbeatFailureDetector) Scan(now time.Time) []common.ServerID {
	d.mu.Lock()
	defer d.mu.Unlock()
	var newlyDead []common.ServerID
	for server, last := range d.last {
		if d.alive[server] && now.Sub(last) > d.timeout {
			d.alive[server] = false
			newlyDead = append(newlyDead, server)
		}
	}
	return newlyDead
}

func (d *HeartbeatFailureDetector) MarkAliveAgain(server common.ServerID, at time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.last[server] = at
	d.alive[server] = true
}

func (d *HeartbeatFailureDetector) Forget(server common.ServerID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.last, server)
	delete(d.alive, server)
}

func (d *HeartbeatFailureDetector) KnownServers() []common.ServerID {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]common.ServerID, 0, len(d.last))
	for s := range d.last {
		out = append(out, s)
	}
	return out
}
