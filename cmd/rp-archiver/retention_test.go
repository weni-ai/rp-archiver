package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nyaruka/rp-archiver/archives"
	"github.com/stretchr/testify/assert"
)

func TestRetentionScheduler_OncePerCycle(t *testing.T) {
	var runs int32
	scheduler := newRetentionScheduler(func(ctx context.Context, cycleStart time.Time) {
		atomic.AddInt32(&runs, 1)
	})

	cycle := time.Date(2026, 11, 18, 0, 1, 0, 0, time.UTC)
	scheduler.StartForCycle(cycle)
	scheduler.Wait()
	// the org-fetch retry path goes back to the top of the loop within the same cycle
	scheduler.StartForCycle(cycle.Add(5 * time.Minute))
	scheduler.Wait()
	assert.Equal(t, int32(1), atomic.LoadInt32(&runs))

	scheduler.NextCycle()
	scheduler.StartForCycle(cycle.AddDate(0, 0, 1))
	scheduler.Wait()
	assert.Equal(t, int32(2), atomic.LoadInt32(&runs))
}

func TestRetentionScheduler_IndependentOfOrgs(t *testing.T) {
	release := make(chan struct{})
	var finished int32
	scheduler := newRetentionScheduler(func(ctx context.Context, cycleStart time.Time) {
		<-release
		atomic.StoreInt32(&finished, 1)
	})

	scheduler.StartForCycle(time.Now())
	// StartForCycle returned while the step is still running, so org dispatch is not delayed
	assert.Equal(t, int32(0), atomic.LoadInt32(&finished))

	close(release)
	scheduler.Wait()
	assert.Equal(t, int32(1), atomic.LoadInt32(&finished), "Wait returns only after the step ends")
}

func TestRetentionScheduler_DisabledNeverRuns(t *testing.T) {
	scheduler := newRetentionScheduler(nil)

	scheduler.StartForCycle(time.Now())
	scheduler.Wait()
	scheduler.NextCycle()
	scheduler.StartForCycle(time.Now())
	scheduler.Wait()
}

func TestRetentionScheduler_Timeout(t *testing.T) {
	var deadline time.Time
	var hasDeadline bool
	var cycleStart time.Time
	scheduler := newRetentionScheduler(func(ctx context.Context, start time.Time) {
		deadline, hasDeadline = ctx.Deadline()
		cycleStart = start
	})

	cycle := time.Date(2026, 11, 18, 0, 1, 0, 0, time.UTC)
	before := time.Now()
	scheduler.StartForCycle(cycle)
	scheduler.Wait()

	assert.True(t, hasDeadline)
	assert.WithinDuration(t, before.Add(archives.CallRecordingsRetentionTimeout), deadline, 5*time.Second)
	assert.Equal(t, cycle, cycleStart)
}

func TestRetentionScheduler_NoOverlap(t *testing.T) {
	release := make(chan struct{})
	var runs int32
	scheduler := newRetentionScheduler(func(ctx context.Context, cycleStart time.Time) {
		atomic.AddInt32(&runs, 1)
		<-release
	})

	scheduler.StartForCycle(time.Now())
	scheduler.NextCycle()
	// a next cycle that starts while the previous run is still going does not start a second run
	scheduler.StartForCycle(time.Now())
	close(release)
	scheduler.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&runs))
}
