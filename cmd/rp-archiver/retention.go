package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nyaruka/rp-archiver/archives"
	"github.com/sirupsen/logrus"
)

// retentionScheduler runs the call recordings retention step at most once per archiving cycle,
// in its own goroutine, so it neither waits for org archival nor delays it.
type retentionScheduler struct {
	step func(ctx context.Context, cycleStart time.Time)

	// read and written only by the main loop goroutine
	startedThisCycle bool

	running atomic.Bool
	wg      sync.WaitGroup
}

// newRetentionScheduler returns a scheduler for step; a nil step (retention disabled) never runs.
func newRetentionScheduler(step func(ctx context.Context, cycleStart time.Time)) *retentionScheduler {
	return &retentionScheduler{step: step}
}

// StartForCycle starts the step unless it already started in this cycle or a previous run is
// still going. The step context is bounded by archives.CallRecordingsRetentionTimeout.
func (s *retentionScheduler) StartForCycle(cycleStart time.Time) {
	if s.step == nil || s.startedThisCycle {
		return
	}
	if !s.running.CompareAndSwap(false, true) {
		logrus.WithField("component", "call_recordings_retention").Warn("previous call recordings retention run still in progress, skipping this cycle")
		return
	}
	s.startedThisCycle = true

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.running.Store(false)

		ctx, cancel := context.WithTimeout(context.Background(), archives.CallRecordingsRetentionTimeout)
		defer cancel()
		s.step(ctx, cycleStart)
	}()
}

// NextCycle allows the step to start again; called before the loop sleeps until the next cycle.
func (s *retentionScheduler) NextCycle() {
	s.startedThisCycle = false
}

// Wait blocks until a running step ends; used before exiting when ExitOnCompletion is set.
func (s *retentionScheduler) Wait() {
	s.wg.Wait()
}
