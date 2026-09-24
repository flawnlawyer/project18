// Package events implements the simulation-wide event log. Every subsystem
// (master, chunkserver, client, failure injector) appends here so that the
// full causal story of a run — why the master made a given decision — can be
// inspected after the fact, per the "Observability" section of the spec.
package events

import (
	"fmt"
	"sync"
	"time"
)

// Event is one line of the simulation's story.
type Event struct {
	Seq       uint64
	Time      time.Time
	Component string // e.g. "MASTER", "CS3", "CLIENT"
	Message   string
}

func (e Event) String() string {
	return fmt.Sprintf("[%s] %s", e.Component, e.Message)
}

// Log is an append-only, thread-safe event log.
type Log struct {
	mu     sync.Mutex
	events []Event
	seq    uint64
}

func NewLog() *Log {
	return &Log{}
}

// Logf appends a formatted event attributed to component.
func (l *Log) Logf(component, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	l.events = append(l.events, Event{
		Seq:       l.seq,
		Time:      time.Now(),
		Component: component,
		Message:   fmt.Sprintf(format, args...),
	})
}

// All returns a snapshot copy of every event recorded so far.
func (l *Log) All() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	return out
}

// Since returns every event with Seq > seq, so a caller can poll for new
// activity without re-scanning the whole log.
func (l *Log) Since(seq uint64) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Event
	for _, e := range l.events {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out
}

// Dump prints every event to stdout in order — the simplest way to inspect
// "why" the system made a decision.
func (l *Log) Dump() {
	for _, e := range l.All() {
		fmt.Printf("%6d  %s  %s\n", e.Seq, e.Time.Format("15:04:05.000"), e)
	}
}
