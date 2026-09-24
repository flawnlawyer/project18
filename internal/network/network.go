// Package network models the links between components. Per the spec, this is
// a simulation — we do not want real sockets/RPC plumbing getting in the way
// of experimentation. Every "call" between client/master/chunkserver goes
// through a NetworkModel so that delay, drops, and partitions are controlled
// experiments rather than actual network code. A later experiment can swap
// InProcessNetwork for something that models real link latency distributions
// without touching any caller.
package network

import (
	"errors"
	"sync"
	"time"

	"project18/internal/common"
	"project18/internal/metrics"
)

var ErrDropped = errors.New("network: message dropped")

// NetworkModel is the replaceable interface. Callers route every RPC-like
// interaction through Call so failure injection has one place to act.
type NetworkModel interface {
	// Call executes fn as if it were a network round-trip from `from` to
	// `to`. It applies any configured delay/drop rule for that specific
	// link before running fn, and returns ErrDropped instead of calling fn
	// if the message is dropped.
	Call(from, to common.ServerID, fn func() error) error
}

type link struct {
	from, to common.ServerID
}

type linkRule struct {
	delay    time.Duration
	dropAll  bool
	dropOnce bool // drop exactly the next message on this link, then clear
}

// InProcessNetwork is the M0 NetworkModel: everything runs in one process,
// calls are plain function invocations, but delay/drop can be injected on a
// specific (from,to) link to simulate an unreliable network without real
// sockets. Keying by the full pair (rather than just the destination) is
// what lets "drop CS3's heartbeats" avoid also dropping every other
// server's heartbeats to the same master.
type InProcessNetwork struct {
	mu      sync.Mutex
	rules   map[link]*linkRule
	metrics *metrics.Metrics
}

func NewInProcessNetwork(m *metrics.Metrics) *InProcessNetwork {
	return &InProcessNetwork{
		rules:   make(map[link]*linkRule),
		metrics: m,
	}
}

func (n *InProcessNetwork) Call(from, to common.ServerID, fn func() error) error {
	n.mu.Lock()
	rule, ok := n.rules[link{from, to}]
	var delay time.Duration
	dropped := false
	if ok {
		if rule.dropAll {
			dropped = true
		} else if rule.dropOnce {
			dropped = true
			rule.dropOnce = false
		}
		delay = rule.delay
	}
	n.mu.Unlock()

	if n.metrics != nil {
		n.metrics.IncMessage()
	}
	if dropped {
		return ErrDropped
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return fn()
}

// --- Failure-injection controls (see spec "Failure Injection" section) ---

// DelayLink makes every future message on from->to incur the given delay.
func (n *InProcessNetwork) DelayLink(from, to common.ServerID, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rule(from, to).delay = d
}

// DropLink makes every future message on from->to fail with ErrDropped,
// until StopDropping is called. Used e.g. to simulate a lost heartbeat
// stream from one specific server without marking the node itself offline.
func (n *InProcessNetwork) DropLink(from, to common.ServerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rule(from, to).dropAll = true
}

// DropNext drops exactly the next message on from->to.
func (n *InProcessNetwork) DropNext(from, to common.ServerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rule(from, to).dropOnce = true
}

// StopDropping clears any drop/delay rule on from->to.
func (n *InProcessNetwork) StopDropping(from, to common.ServerID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.rules, link{from, to})
}

func (n *InProcessNetwork) rule(from, to common.ServerID) *linkRule {
	k := link{from, to}
	r, ok := n.rules[k]
	if !ok {
		r = &linkRule{}
		n.rules[k] = r
	}
	return r
}
