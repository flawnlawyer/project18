package master

import (
	"sync"
	"time"
)

// Scheduler is the replaceable interface driving the master's background
// activity (failure-detector scans, replication checks, checkpoints). Kept
// separate so an experiment can replace "poll every N ms" with something
// event-driven later. Tick() is exposed for deterministic, single-step
// operation in tests and the demo — the spec's "Deterministic where
// possible" principle.
type Scheduler interface {
	Start()
	Stop()
	Tick()
}

// PeriodicScheduler runs backgroundFn on a fixed interval in its own
// goroutine, and also allows manual single steps via Tick().
type PeriodicScheduler struct {
	period       time.Duration
	backgroundFn func()

	mu      sync.Mutex
	ticker  *time.Ticker
	done    chan struct{}
	running bool
}

func NewPeriodicScheduler(period time.Duration, backgroundFn func()) *PeriodicScheduler {
	return &PeriodicScheduler{period: period, backgroundFn: backgroundFn}
}

func (s *PeriodicScheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}
	s.ticker = time.NewTicker(s.period)
	s.done = make(chan struct{})
	s.running = true
	ticker := s.ticker
	done := s.done
	go func() {
		for {
			select {
			case <-ticker.C:
				s.backgroundFn()
			case <-done:
				return
			}
		}
	}()
}

func (s *PeriodicScheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	s.ticker.Stop()
	close(s.done)
	s.running = false
}

// Tick runs one background pass immediately, regardless of whether the
// automatic ticker is running. Used for deterministic demos/tests.
func (s *PeriodicScheduler) Tick() {
	s.backgroundFn()
}
