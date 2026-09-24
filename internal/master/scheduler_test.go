package master

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPeriodicSchedulerStartStop(t *testing.T) {
	var calls int32
	s := NewPeriodicScheduler(10*time.Millisecond, func() {
		atomic.AddInt32(&calls, 1)
	})
	s.Start()
	time.Sleep(55 * time.Millisecond)
	s.Stop()
	got := atomic.LoadInt32(&calls)
	if got < 2 {
		t.Fatalf("expected several ticks in 55ms at 10ms period, got %d", got)
	}

	// Calling Stop twice, or Start after Stop, must not panic or deadlock.
	s.Stop()
	s.Start()
	s.Stop()
}

func TestPeriodicSchedulerManualTick(t *testing.T) {
	var calls int32
	s := NewPeriodicScheduler(time.Hour, func() {
		atomic.AddInt32(&calls, 1)
	})
	s.Tick()
	s.Tick()
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 manual ticks, got %d", got)
	}
}
