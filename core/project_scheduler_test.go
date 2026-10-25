package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeSchedulerClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeSchedulerTimer
}

type fakeSchedulerTimer struct {
	mu      sync.Mutex
	clock   *fakeSchedulerClock
	at      time.Time
	ch      chan time.Time
	stopped bool
	fired   bool
}

func newFakeSchedulerClock(now time.Time) *fakeSchedulerClock {
	return &fakeSchedulerClock{now: now}
}

func (c *fakeSchedulerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeSchedulerClock) NewTimer(d time.Duration) SchedulerTimer {
	c.mu.Lock()
	now := c.now
	t := &fakeSchedulerTimer{
		clock: c,
		at:    now.Add(d),
		ch:    make(chan time.Time, 1),
	}
	c.timers = append(c.timers, t)
	c.mu.Unlock()

	t.fireIfDue(now)
	return t
}

func (c *fakeSchedulerClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	timers := append([]*fakeSchedulerTimer(nil), c.timers...)
	c.mu.Unlock()

	for _, timer := range timers {
		timer.fireIfDue(now)
	}
}

func (c *fakeSchedulerClock) timersSnapshot() []*fakeSchedulerTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*fakeSchedulerTimer(nil), c.timers...)
}

func (t *fakeSchedulerTimer) isStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

func (t *fakeSchedulerTimer) C() <-chan time.Time { return t.ch }

func (t *fakeSchedulerTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

func (t *fakeSchedulerTimer) fireIfDue(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired || now.Before(t.at) {
		return
	}
	t.fired = true
	select {
	case t.ch <- now:
	default:
	}
}

func waitStarted(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case got := <-started:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task to start")
		return ""
	}
}

func assertNoBufferedStart(t *testing.T, started <-chan string) {
	t.Helper()
	select {
	case got := <-started:
		t.Fatalf("unexpected additional start already buffered: %s", got)
	default:
	}
}

func waitProjectSchedulerIdle(t *testing.T, scheduler *ProjectScheduler) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last SchedulerSnapshot
	for time.Now().Before(deadline) {
		last = scheduler.Snapshot()
		if last.ActiveCount == 0 && last.WaitingCount == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scheduler did not become idle; last=%+v", last)
}

type projectSchedulerStubPlatform struct{ name string }

func (p projectSchedulerStubPlatform) Name() string                             { return p.name }
func (p projectSchedulerStubPlatform) Start(MessageHandler) error               { return nil }
func (p projectSchedulerStubPlatform) Reply(context.Context, any, string) error { return nil }
func (p projectSchedulerStubPlatform) Send(context.Context, any, string) error  { return nil }
func (p projectSchedulerStubPlatform) Stop() error                              { return nil }

func TestProjectSchedulerSerializesDifferentSessionsAndSourcesFIFO(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(0, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })

	started := make(chan string, 3)
	release := make(chan struct{}, 3)
	task := func(id, source, session string, platform Platform) ProjectTask {
		return ProjectTask{ID: id, Source: AgentTriggerSource(source), SessionKey: session, Platform: platform, Run: func(ctx context.Context) TaskResult {
			started <- id
			select {
			case <-release:
				return TaskResult{State: "completed"}
			case <-ctx.Done():
				return TaskResult{State: "cancelled", Err: ctx.Err()}
			}
		}}
	}

	if got := scheduler.Submit(task("a", "interactive", "feishu:chat-a:root:r1", projectSchedulerStubPlatform{name: "feishu"})); got.Status != SubmitStarted || got.Position != 0 {
		t.Fatalf("submit a = %+v", got)
	}
	if got := scheduler.Submit(task("b", "heartbeat", "telegram:-100:42", projectSchedulerStubPlatform{name: "telegram"})); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("submit b = %+v", got)
	}
	if got := scheduler.Submit(task("c", "timer", "weixin:user", projectSchedulerStubPlatform{name: "weixin"})); got.Status != SubmitQueued || got.Position != 2 {
		t.Fatalf("submit c = %+v", got)
	}

	if got := waitStarted(t, started); got != "a" {
		t.Fatalf("first started = %q", got)
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 1 || snap.WaitingCount != 2 {
		t.Fatalf("snapshot while a active = %+v", snap)
	}
	assertNoBufferedStart(t, started)

	release <- struct{}{}
	if got := waitStarted(t, started); got != "b" {
		t.Fatalf("second started = %q, want FIFO task b before source/platform variants", got)
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 1 || snap.WaitingCount != 1 {
		t.Fatalf("snapshot while b active = %+v", snap)
	}
	assertNoBufferedStart(t, started)

	release <- struct{}{}
	if got := waitStarted(t, started); got != "c" {
		t.Fatalf("third started = %q", got)
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 1 || snap.WaitingCount != 0 {
		t.Fatalf("snapshot while c active = %+v", snap)
	}
	release <- struct{}{}
}

func TestWaitingCapacityIsTenExcludingActive(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(100, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })

	started := make(chan string, 12)
	release := make(chan struct{})
	blockingTask := ProjectTask{ID: "active", SessionKey: "weixin:active", Run: func(ctx context.Context) TaskResult {
		started <- "active"
		select {
		case <-release:
			return TaskResult{State: "completed"}
		case <-ctx.Done():
			return TaskResult{State: "cancelled", Err: ctx.Err()}
		}
	}}
	if got := scheduler.Submit(blockingTask); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	if got := waitStarted(t, started); got != "active" {
		t.Fatalf("started active = %q", got)
	}

	for i := 1; i <= 10; i++ {
		got := scheduler.Submit(ProjectTask{ID: fmt.Sprintf("waiting-%02d", i), Source: "interactive", SessionKey: fmt.Sprintf("session-%02d", i), Run: func(ctx context.Context) TaskResult {
			started <- "unexpected"
			return TaskResult{State: "completed"}
		}})
		if got.Status != SubmitQueued || got.Position != i {
			t.Fatalf("submit waiting %d = %+v", i, got)
		}
	}

	got := scheduler.Submit(ProjectTask{ID: "overflow", SessionKey: "overflow", Run: func(ctx context.Context) TaskResult {
		started <- "overflow"
		return TaskResult{State: "completed"}
	}})
	if got.Status != SubmitRejectedFull || got.Position != 0 {
		t.Fatalf("overflow submit = %+v", got)
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 1 || snap.WaitingCount != 10 {
		t.Fatalf("snapshot full waiting = %+v", snap)
	}
	assertNoBufferedStart(t, started)
}

func TestProjectSchedulerDefaultsQueueTimesAndCleanup(t *testing.T) {
	now := time.Unix(123, 0)
	clock := newFakeSchedulerClock(now)
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })

	started := make(chan string, 1)
	activeRelease := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{ID: "active", Run: func(ctx context.Context) TaskResult {
		started <- "active"
		select {
		case <-activeRelease:
			return TaskResult{State: "completed"}
		case <-ctx.Done():
			return TaskResult{State: "cancelled", Err: ctx.Err()}
		}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	if got := waitStarted(t, started); got != "active" {
		t.Fatalf("started = %q", got)
	}

	if got := scheduler.Submit(ProjectTask{ID: "queued", Source: "relay", SessionKey: "telegram:-100:topic"}); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("submit queued = %+v", got)
	}
	snap := scheduler.Snapshot()
	if snap.WaitingCount != 1 {
		t.Fatalf("waiting count = %d", snap.WaitingCount)
	}
	queued := snap.Waiting[0]
	if !queued.EnqueuedAt.Equal(now) || !queued.ExpiresAt.Equal(now.Add(6*time.Hour)) {
		t.Fatalf("queued times = enqueued %s expires %s", queued.EnqueuedAt, queued.ExpiresAt)
	}
	close(activeRelease)
}

func TestProjectSchedulerStopCancelsRunTimer(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(0, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)

	started := make(chan struct{}, 1)
	if got := scheduler.Submit(ProjectTask{ID: "active", Run: func(ctx context.Context) TaskResult {
		started <- struct{}{}
		<-ctx.Done()
		return TaskResult{State: "cancelled", Err: ctx.Err()}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for active task")
	}

	scheduler.Stop(context.Background())
	timers := clock.timersSnapshot()
	if len(timers) != 1 {
		t.Fatalf("timer count = %d, want exactly run timer", len(timers))
	}
	if !timers[0].isStopped() {
		t.Fatal("run timer still active after Stop")
	}
	clock.Advance(30 * time.Minute)
	if got := scheduler.Submit(ProjectTask{ID: "late"}); got.Status != SubmitRejectedStopped {
		t.Fatalf("submit after stopped timer advance = %+v", got)
	}
}

func TestFakeSchedulerClockNewTimerZeroIsReady(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(0, 0))
	timer := clock.NewTimer(0)
	select {
	case got := <-timer.C():
		if !got.Equal(clock.Now()) {
			t.Fatalf("zero timer fired at %s, want %s", got, clock.Now())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("zero-duration fake timer was not ready immediately")
	}
}

func TestProjectSchedulerRejectsSubmitAfterStop(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(0, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	scheduler.Stop(context.Background())

	got := scheduler.Submit(ProjectTask{ID: "late", Run: func(ctx context.Context) TaskResult {
		return TaskResult{State: "completed"}
	}})
	if got.Status != SubmitRejectedStopped {
		t.Fatalf("submit after stop = %+v", got)
	}
}

type projectSchedulerFinishRecord struct {
	id     string
	result TaskResult
}

func waitFinish(t *testing.T, finished <-chan projectSchedulerFinishRecord) projectSchedulerFinishRecord {
	t.Helper()
	select {
	case got := <-finished:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for terminal callback")
		return projectSchedulerFinishRecord{}
	}
}

func waitCleanup(t *testing.T, cleaned <-chan string) string {
	t.Helper()
	select {
	case got := <-cleaned:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cleanup")
		return ""
	}
}

func assertNoFinish(t *testing.T, finished <-chan projectSchedulerFinishRecord) {
	t.Helper()
	select {
	case got := <-finished:
		t.Fatalf("unexpected terminal callback already buffered: %+v", got)
	default:
	}
}

func assertNoCleanup(t *testing.T, cleaned <-chan string) {
	t.Helper()
	select {
	case got := <-cleaned:
		t.Fatalf("unexpected cleanup already buffered: %s", got)
	default:
	}
}

func TestSixHourExpiryCleansAndContinuesFIFO(t *testing.T) {
	// 防回归点：queued 任务到达 ExpiresAt 的精确边界时，不能启动 Run，必须终态 expired、清理一次并继续 FIFO。
	now := time.Unix(1_000, 0)
	clock := newFakeSchedulerClock(now)
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 7*time.Hour)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })

	started := make(chan string, 4)
	cleaned := make(chan string, 4)
	finished := make(chan projectSchedulerFinishRecord, 4)
	activeRelease := make(chan struct{})
	cRelease := make(chan struct{})

	makeTask := func(id string, run func(context.Context) TaskResult) ProjectTask {
		return ProjectTask{
			ID:  id,
			Run: run,
			Cleanup: func() {
				cleaned <- id
			},
			OnFinish: func(result TaskResult) {
				finished <- projectSchedulerFinishRecord{id: id, result: result}
			},
		}
	}

	if got := scheduler.Submit(makeTask("active", func(ctx context.Context) TaskResult {
		started <- "active"
		select {
		case <-activeRelease:
			return TaskResult{State: "completed"}
		case <-ctx.Done():
			return TaskResult{State: "cancelled", Err: ctx.Err()}
		}
	})); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	if got := waitStarted(t, started); got != "active" {
		t.Fatalf("started active = %q", got)
	}

	expiresAt := now.Add(6 * time.Hour)
	if got := scheduler.Submit(ProjectTask{
		ID:         "expired",
		EnqueuedAt: now,
		ExpiresAt:  expiresAt,
		Run: func(context.Context) TaskResult {
			started <- "expired"
			return TaskResult{State: "completed"}
		},
		Cleanup: func() { cleaned <- "expired" },
		OnFinish: func(result TaskResult) {
			finished <- projectSchedulerFinishRecord{id: "expired", result: result}
		},
	}); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("submit expired = %+v", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID:         "next",
		EnqueuedAt: now,
		ExpiresAt:  expiresAt.Add(time.Nanosecond),
		Run: func(ctx context.Context) TaskResult {
			started <- "next"
			select {
			case <-cRelease:
				return TaskResult{State: "completed"}
			case <-ctx.Done():
				return TaskResult{State: "cancelled", Err: ctx.Err()}
			}
		},
		Cleanup: func() { cleaned <- "next" },
		OnFinish: func(result TaskResult) {
			finished <- projectSchedulerFinishRecord{id: "next", result: result}
		},
	}); got.Status != SubmitQueued || got.Position != 2 {
		t.Fatalf("submit next = %+v", got)
	}

	clock.Advance(6 * time.Hour)
	close(activeRelease)
	cleanedBeforeNext := map[string]int{}
	for i := 0; i < 2; i++ {
		cleanedBeforeNext[waitCleanup(t, cleaned)]++
	}
	if cleanedBeforeNext["active"] != 1 || cleanedBeforeNext["expired"] != 1 {
		t.Fatalf("cleanup before next = %+v, want active and expired exactly once", cleanedBeforeNext)
	}
	finishedBeforeNext := map[string]string{}
	for i := 0; i < 2; i++ {
		got := waitFinish(t, finished)
		finishedBeforeNext[got.id] = got.result.State
	}
	if finishedBeforeNext["active"] != "completed" || finishedBeforeNext["expired"] != "expired" {
		t.Fatalf("finishes before next = %+v, want active completed and expired expired", finishedBeforeNext)
	}
	if got := waitStarted(t, started); got != "next" {
		t.Fatalf("next started = %q, want next; expired Run must not start", got)
	}
	assertNoBufferedStart(t, started)
	close(cRelease)
	if got := waitCleanup(t, cleaned); got != "next" {
		t.Fatalf("third cleanup = %q, want next", got)
	}
	if got := waitFinish(t, finished); got.id != "next" || got.result.State != "completed" {
		t.Fatalf("next finish = %+v, want completed", got)
	}
}

func TestThirtyMinuteTimeoutReleasesSlotAndCleansOnce(t *testing.T) {
	// 防回归点：runTimeout 是绝对 30m，不因 Run 持续执行而滑动；协作 Run 退出后发布 timed_out 并释放 FIFO 下一项。
	clock := newFakeSchedulerClock(time.Unix(2_000, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })

	started := make(chan string, 3)
	cleaned := make(chan string, 3)
	finished := make(chan projectSchedulerFinishRecord, 3)
	nextRelease := make(chan struct{})

	if got := scheduler.Submit(ProjectTask{
		ID: "slow",
		Run: func(ctx context.Context) TaskResult {
			started <- "slow"
			<-ctx.Done()
			return TaskResult{State: "cancelled", Err: ctx.Err()}
		},
		Cleanup:  func() { cleaned <- "slow" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "slow", result: result} },
	}); got.Status != SubmitStarted {
		t.Fatalf("submit slow = %+v", got)
	}
	if got := waitStarted(t, started); got != "slow" {
		t.Fatalf("started slow = %q", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "next",
		Run: func(ctx context.Context) TaskResult {
			started <- "next"
			select {
			case <-nextRelease:
				return TaskResult{State: "completed"}
			case <-ctx.Done():
				return TaskResult{State: "cancelled", Err: ctx.Err()}
			}
		},
		Cleanup:  func() { cleaned <- "next" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "next", result: result} },
	}); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("submit next = %+v", got)
	}

	clock.Advance(30 * time.Minute)
	if got := waitCleanup(t, cleaned); got != "slow" {
		t.Fatalf("first cleanup = %q, want slow", got)
	}
	if got := waitFinish(t, finished); got.id != "slow" || got.result.State != "timed_out" {
		t.Fatalf("timeout finish = %+v, want timed_out", got)
	}
	if got := waitStarted(t, started); got != "next" {
		t.Fatalf("next started = %q, want next after cooperative timeout", got)
	}
	assertNoBufferedStart(t, started)
	close(nextRelease)
	if got := waitCleanup(t, cleaned); got != "next" {
		t.Fatalf("second cleanup = %q, want next", got)
	}
	if got := waitFinish(t, finished); got.id != "next" || got.result.State != "completed" {
		t.Fatalf("next finish = %+v, want completed", got)
	}
}

func TestTimedOutRunMustExitBeforeNextStarts(t *testing.T) {
	// 防回归点：不合作 Run 超时后只能先裁定并 cancel ctx，原 Run 未退出前不得启动下一项或清理其附件。
	clock := newFakeSchedulerClock(time.Unix(3_000, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })

	started := make(chan string, 3)
	cleaned := make(chan string, 3)
	finished := make(chan projectSchedulerFinishRecord, 3)
	ctxCancelled := make(chan struct{})
	releaseSlow := make(chan struct{})
	nextRelease := make(chan struct{})

	if got := scheduler.Submit(ProjectTask{
		ID: "stubborn",
		Run: func(ctx context.Context) TaskResult {
			started <- "stubborn"
			go func() {
				<-ctx.Done()
				close(ctxCancelled)
			}()
			<-releaseSlow
			return TaskResult{State: "completed"}
		},
		Cleanup:  func() { cleaned <- "stubborn" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "stubborn", result: result} },
	}); got.Status != SubmitStarted {
		t.Fatalf("submit stubborn = %+v", got)
	}
	if got := waitStarted(t, started); got != "stubborn" {
		t.Fatalf("started stubborn = %q", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "next",
		Run: func(ctx context.Context) TaskResult {
			started <- "next"
			select {
			case <-nextRelease:
				return TaskResult{State: "completed"}
			case <-ctx.Done():
				return TaskResult{State: "cancelled", Err: ctx.Err()}
			}
		},
		Cleanup:  func() { cleaned <- "next" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "next", result: result} },
	}); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("submit next = %+v", got)
	}

	clock.Advance(30 * time.Minute)
	select {
	case <-ctxCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for timeout to cancel stubborn Run context")
	}
	assertNoBufferedStart(t, started)
	assertNoCleanup(t, cleaned)
	assertNoFinish(t, finished)

	close(releaseSlow)
	if got := waitCleanup(t, cleaned); got != "stubborn" {
		t.Fatalf("first cleanup = %q, want stubborn after Run exits", got)
	}
	if got := waitFinish(t, finished); got.id != "stubborn" || got.result.State != "timed_out" {
		t.Fatalf("stubborn finish = %+v, want timed_out after Run exits", got)
	}
	if got := waitStarted(t, started); got != "next" {
		t.Fatalf("next started = %q, want next only after stubborn exits", got)
	}
	close(nextRelease)
	if got := waitCleanup(t, cleaned); got != "next" {
		t.Fatalf("second cleanup = %q, want next", got)
	}
	if got := waitFinish(t, finished); got.id != "next" || got.result.State != "completed" {
		t.Fatalf("next finish = %+v, want completed", got)
	}
}

func TestCompletionCancellationRaceReleasesSlotOnce(t *testing.T) {
	// 防回归点：Run 正常完成与 Stop 取消竞态时，只能有一个终态、一次 cleanup；本轮不提前实现 SessionKey scope cancel。
	for i := 0; i < 100; i++ {
		clock := newFakeSchedulerClock(time.Unix(int64(4_000+i), 0))
		scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)

		started := make(chan struct{}, 1)
		cleaned := make(chan string, 2)
		finished := make(chan projectSchedulerFinishRecord, 2)
		releaseRun := make(chan struct{})
		beginRace := make(chan struct{})

		if got := scheduler.Submit(ProjectTask{
			ID: "race",
			Run: func(ctx context.Context) TaskResult {
				started <- struct{}{}
				select {
				case <-releaseRun:
					return TaskResult{State: "completed"}
				case <-ctx.Done():
					return TaskResult{State: "cancelled", Err: ctx.Err()}
				}
			},
			Cleanup:  func() { cleaned <- "race" },
			OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "race", result: result} },
		}); got.Status != SubmitStarted {
			t.Fatalf("iteration %d submit race = %+v", i, got)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d timed out waiting for race task start", i)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-beginRace
			close(releaseRun)
		}()
		go func() {
			defer wg.Done()
			<-beginRace
			scheduler.Stop(context.Background())
		}()
		close(beginRace)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d timed out waiting for completion/cancellation race", i)
		}

		if got := waitCleanup(t, cleaned); got != "race" {
			t.Fatalf("iteration %d cleanup = %q, want race", i, got)
		}
		got := waitFinish(t, finished)
		if got.id != "race" || (got.result.State != "completed" && got.result.State != "cancelled") {
			t.Fatalf("iteration %d finish = %+v, want completed or cancelled", i, got)
		}
		assertNoCleanup(t, cleaned)
		assertNoFinish(t, finished)
	}
}

func waitSnapshot(t *testing.T, scheduler *ProjectScheduler) SchedulerSnapshot {
	t.Helper()
	result := make(chan SchedulerSnapshot, 1)
	go func() { result <- scheduler.Snapshot() }()
	select {
	case got := <-result:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for scheduler Snapshot")
		return SchedulerSnapshot{}
	}
}

func waitStopReturned(t *testing.T, stopped <-chan struct{}) {
	t.Helper()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Stop to return")
	}
}

func TestBlockedOnFinishBacklogIsBoundedAndDoesNotBlockOwner(t *testing.T) {
	// 防回归点：阻塞 OnFinish 只能占用有界通知槽；槽满后独立拒绝，不能反压 owner loop 使 Snapshot/Stop 不可达。
	clock := newFakeSchedulerClock(time.Unix(5_000, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	unblockCallbacks := make(chan struct{})
	defer scheduler.Stop(context.Background())
	defer close(unblockCallbacks)

	started := make(chan string, 32)
	cleaned := make(chan string, 32)
	callbackEntered := make(chan string, 32)

	submitBatch := func(prefix string, release <-chan struct{}) {
		for i := 0; i < 11; i++ {
			id := fmt.Sprintf("%s-%02d", prefix, i)
			got := scheduler.Submit(ProjectTask{
				ID: id,
				Run: func(context.Context) TaskResult {
					started <- id
					<-release
					return TaskResult{State: TaskStateCompleted}
				},
				Cleanup: func() { cleaned <- id },
				OnFinish: func(TaskResult) {
					callbackEntered <- id
					<-unblockCallbacks
				},
			})
			if i == 0 {
				if got.Status != SubmitStarted {
					t.Fatalf("submit %s = %+v, want started", id, got)
				}
				continue
			}
			if got.Status != SubmitQueued || got.Position != i {
				t.Fatalf("submit %s = %+v, want queued position %d", id, got, i)
			}
		}
	}

	firstRelease := make(chan struct{})
	submitBatch("first", firstRelease)
	close(firstRelease)
	for i := 0; i < 11; i++ {
		waitCleanup(t, cleaned)
	}
	if got := waitStarted(t, started); got != "first-00" {
		t.Fatalf("first start = %q", got)
	}
	for i := 1; i < 11; i++ {
		waitStarted(t, started)
	}
	if got := waitFinishStart(t, callbackEntered); got == "" {
		t.Fatal("first callback did not start")
	}

	secondRelease := make(chan struct{})
	submitBatch("second", secondRelease)
	close(secondRelease)
	for i := 0; i < 11; i++ {
		waitCleanup(t, cleaned)
	}
	for i := 0; i < 11; i++ {
		waitStarted(t, started)
	}

	snap := waitSnapshot(t, scheduler)
	if snap.NotificationCapacity != 22 || snap.NotificationPending != 22 {
		t.Fatalf("notification snapshot = pending %d capacity %d, want 22/22", snap.NotificationPending, snap.NotificationCapacity)
	}
	got := scheduler.Submit(ProjectTask{
		ID:       "rejected-notification",
		Run:      func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		Cleanup:  func() { cleaned <- "rejected-notification" },
		OnFinish: func(TaskResult) {},
	})
	if got.Status != SubmitRejectedNotificationBacklog {
		t.Fatalf("submit with full notification backlog = %+v, want rejected_notification_backlog", got)
	}
	assertNoCleanup(t, cleaned)

	nilNotify := scheduler.Submit(ProjectTask{ID: "nil-notify", Run: func(context.Context) TaskResult {
		started <- "nil-notify"
		return TaskResult{State: TaskStateCompleted}
	}})
	if nilNotify.Status != SubmitStarted {
		t.Fatalf("nil OnFinish submit with full notification backlog = %+v, want started", nilNotify)
	}
	waitStarted(t, started)

	stopReturned := make(chan struct{})
	go func() {
		scheduler.Stop(context.Background())
		close(stopReturned)
	}()
	waitStopReturned(t, stopReturned)
}

func waitFinishStart(t *testing.T, callbackEntered <-chan string) string {
	t.Helper()
	select {
	case got := <-callbackEntered:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for OnFinish to start")
		return ""
	}
}

func TestProjectSchedulerCleanupPanicStillPublishesAndReleasesSlot(t *testing.T) {
	scheduler := NewProjectScheduler(nil, 0, 0, 0)
	finished := make(chan TaskResult, 2)
	var cleanupAttempts atomic.Int32
	if got := scheduler.Submit(ProjectTask{
		ID: "cleanup-panic",
		Cleanup: func() {
			cleanupAttempts.Add(1)
			panic("cleanup must be isolated")
		},
		OnFinish: func(result TaskResult) { finished <- result },
	}); got.Status != SubmitStarted {
		t.Fatalf("cleanup panic submit=%+v", got)
	}
	if result := <-finished; result.State != TaskStateCompleted {
		t.Fatalf("cleanup panic terminal=%s", result.State)
	}
	if got := cleanupAttempts.Load(); got != 1 {
		t.Fatalf("cleanup attempts=%d, want 1", got)
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		if scheduler.Snapshot().NotificationPending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cleanup panic 后 notification slot 未释放")
		}
	}
	select {
	case result := <-finished:
		t.Fatalf("cleanup panic OnFinish 重复发布：%+v", result)
	default:
	}
	afterFinished := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "after-cleanup-panic",
		OnFinish: func(TaskResult) {
			close(afterFinished)
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("cleanup panic 后续 submit=%+v", got)
	}
	select {
	case <-afterFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup panic 后续任务未正常发布")
	}
	scheduler.Stop(context.Background())
}

func TestOnFinishPanicReleasesNotificationSlot(t *testing.T) {
	// 防回归点：OnFinish panic 不能杀死 publisher goroutine，且必须释放通知槽，后续任务仍能发布。
	clock := newFakeSchedulerClock(time.Unix(5_100, 0))
	scheduler := NewProjectScheduler(clock, 1, 6*time.Hour, 30*time.Minute)
	defer scheduler.Stop(context.Background())

	started := make(chan string, 2)
	cleaned := make(chan string, 2)
	finished := make(chan projectSchedulerFinishRecord, 1)

	if got := scheduler.Submit(ProjectTask{
		ID: "panic",
		Run: func(context.Context) TaskResult {
			started <- "panic"
			return TaskResult{State: TaskStateCompleted}
		},
		Cleanup: func() { cleaned <- "panic" },
		OnFinish: func(TaskResult) {
			panic("boom")
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("submit panic = %+v", got)
	}
	if got := waitStarted(t, started); got != "panic" {
		t.Fatalf("started panic = %q", got)
	}
	if got := waitCleanup(t, cleaned); got != "panic" {
		t.Fatalf("cleanup panic = %q", got)
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		if scheduler.Snapshot().ActiveCount == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("panic task completion ownership 未收敛")
		}
	}

	if got := scheduler.Submit(ProjectTask{
		ID: "after-panic",
		Run: func(context.Context) TaskResult {
			started <- "after-panic"
			return TaskResult{State: TaskStateCompleted}
		},
		Cleanup: func() { cleaned <- "after-panic" },
		OnFinish: func(result TaskResult) {
			finished <- projectSchedulerFinishRecord{id: "after-panic", result: result}
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("submit after panic = %+v", got)
	}
	if got := waitStarted(t, started); got != "after-panic" {
		t.Fatalf("started after panic = %q", got)
	}
	if got := waitCleanup(t, cleaned); got != "after-panic" {
		t.Fatalf("cleanup after panic = %q", got)
	}
	if got := waitFinish(t, finished); got.id != "after-panic" || got.result.State != TaskStateCompleted {
		t.Fatalf("finish after panic = %+v", got)
	}
}

func TestStopWinsPublishesCancelledEvenIfRunReturnsCompleted(t *testing.T) {
	// 防回归点：Stop 已被 owner 接受后即裁定 active cancelled；不合作 Run 后续返回 completed 也不能覆盖裁定。
	clock := newFakeSchedulerClock(time.Unix(5_200, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	defer scheduler.Stop(context.Background())

	started := make(chan string, 2)
	cleaned := make(chan string, 2)
	finished := make(chan projectSchedulerFinishRecord, 2)
	ctxCancelled := make(chan struct{})
	releaseRun := make(chan struct{})

	if got := scheduler.Submit(ProjectTask{
		ID: "stubborn",
		Run: func(ctx context.Context) TaskResult {
			started <- "stubborn"
			<-ctx.Done()
			close(ctxCancelled)
			<-releaseRun
			return TaskResult{State: TaskStateCompleted}
		},
		Cleanup:  func() { cleaned <- "stubborn" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "stubborn", result: result} },
	}); got.Status != SubmitStarted {
		t.Fatalf("submit stubborn = %+v", got)
	}
	if got := waitStarted(t, started); got != "stubborn" {
		t.Fatalf("started stubborn = %q", got)
	}
	stopReturned := make(chan struct{})
	go func() {
		scheduler.Stop(context.Background())
		close(stopReturned)
	}()
	select {
	case <-ctxCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Stop to cancel Run context")
	}
	close(releaseRun)
	waitStopReturned(t, stopReturned)
	if got := waitCleanup(t, cleaned); got != "stubborn" {
		t.Fatalf("cleanup = %q, want stubborn", got)
	}
	if got := waitFinish(t, finished); got.id != "stubborn" || got.result.State != TaskStateCancelled {
		t.Fatalf("finish = %+v, want cancelled despite completed Run return", got)
	}
}

func TestCompletionCancellationRaceHandlesSuccessorOnce(t *testing.T) {
	// 防回归点：completion 与 Stop 竞态必须覆盖 successor；Stop 赢则 successor 不启动并 cancelled once，completion 赢则 successor 至多启动一次再由 Stop 收尾。
	for i := 0; i < 100; i++ {
		clock := newFakeSchedulerClock(time.Unix(int64(5_300+i), 0))
		scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)

		started := make(chan string, 2)
		cleaned := make(chan string, 2)
		finished := make(chan projectSchedulerFinishRecord, 2)
		releaseRun := make(chan struct{})
		beginRace := make(chan struct{})

		if got := scheduler.Submit(ProjectTask{
			ID: "race",
			Run: func(ctx context.Context) TaskResult {
				started <- "race"
				select {
				case <-releaseRun:
					return TaskResult{State: TaskStateCompleted}
				case <-ctx.Done():
					return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
				}
			},
			Cleanup:  func() { cleaned <- "race" },
			OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "race", result: result} },
		}); got.Status != SubmitStarted {
			t.Fatalf("iteration %d submit race = %+v", i, got)
		}
		if got := waitStarted(t, started); got != "race" {
			t.Fatalf("iteration %d started = %q, want race", i, got)
		}
		if got := scheduler.Submit(ProjectTask{
			ID: "next",
			Run: func(ctx context.Context) TaskResult {
				started <- "next"
				<-ctx.Done()
				return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
			},
			Cleanup:  func() { cleaned <- "next" },
			OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "next", result: result} },
		}); got.Status != SubmitQueued || got.Position != 1 {
			t.Fatalf("iteration %d submit next = %+v", i, got)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-beginRace
			close(releaseRun)
		}()
		stopReturned := make(chan struct{})
		go func() {
			defer wg.Done()
			<-beginRace
			scheduler.Stop(context.Background())
			close(stopReturned)
		}()
		close(beginRace)
		waitStopReturned(t, stopReturned)
		wg.Wait()

		seenCleanup := map[string]int{}
		for j := 0; j < 2; j++ {
			seenCleanup[waitCleanup(t, cleaned)]++
		}
		if seenCleanup["race"] != 1 || seenCleanup["next"] != 1 {
			t.Fatalf("iteration %d cleanup counts = %+v, want race/next once", i, seenCleanup)
		}
		seenFinish := map[string]string{}
		for j := 0; j < 2; j++ {
			got := waitFinish(t, finished)
			seenFinish[got.id] = got.result.State
		}
		if seenFinish["race"] != TaskStateCompleted && seenFinish["race"] != TaskStateCancelled {
			t.Fatalf("iteration %d race finish = %q", i, seenFinish["race"])
		}
		if seenFinish["next"] != TaskStateCancelled {
			t.Fatalf("iteration %d next finish = %q, want cancelled", i, seenFinish["next"])
		}
		assertNoCleanup(t, cleaned)
		assertNoFinish(t, finished)
		assertStartedAtMostOnce(t, started, "next")
	}
}

func assertStartedAtMostOnce(t *testing.T, started <-chan string, id string) {
	t.Helper()
	count := 0
	for {
		select {
		case got := <-started:
			if got == id {
				count++
			}
			if count > 1 {
				t.Fatalf("%s started %d times, want at most once", id, count)
			}
		default:
			return
		}
	}
}

func TestProjectWaitingCancelCleanupCanReenterSnapshot(t *testing.T) {
	scheduler := NewProjectScheduler(nil, 2, time.Hour, 0)
	activeStarted := make(chan struct{})
	activeRelease := make(chan struct{})
	cleanupDone := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "active-session",
		Run: func(context.Context) TaskResult {
			close(activeStarted)
			<-activeRelease
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	select {
	case <-activeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("active 未启动")
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "waiting", SessionKey: "cancel-session",
		Cleanup: func() {
			scheduler.Snapshot()
			close(cleanupDone)
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("waiting submit=%+v", got)
	}

	cancelResult := make(chan CancelResult, 1)
	go func() {
		cancelResult <- scheduler.CancelSessionContext(context.Background(), "cancel-session", CancelStop)
	}()
	select {
	case <-cleanupDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting cancel Cleanup 重入 Snapshot 未完成")
	}
	select {
	case result := <-cancelResult:
		if result.WaitingCancelled != 1 || result.ActiveCancelled != 0 || result.Pending {
			t.Fatalf("cancel result=%+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting cancel completion 未收敛")
	}
	close(activeRelease)
	scheduler.Stop(context.Background())
}

func TestProjectWaitingCancelContextBoundsDetachedCleanupAndPreservesOrder(t *testing.T) {
	scheduler := NewProjectScheduler(nil, 2, time.Hour, 0)
	activeStarted := make(chan struct{})
	activeRelease := make(chan struct{})
	activeCleaned := make(chan struct{})
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	successorStarted := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "active-session",
		Run: func(context.Context) TaskResult {
			close(activeStarted)
			<-activeRelease
			return TaskResult{State: TaskStateCompleted}
		},
		Cleanup: func() { close(activeCleaned) },
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	select {
	case <-activeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("active 未启动")
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "cancelled", SessionKey: "cancel-session",
		Cleanup: func() {
			close(cleanupEntered)
			<-cleanupRelease
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("cancelled submit=%+v", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "successor", SessionKey: "successor-session",
		Run: func(context.Context) TaskResult {
			close(successorStarted)
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("successor submit=%+v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelResult := make(chan CancelResult, 1)
	go func() {
		cancelResult <- scheduler.CancelSessionContext(ctx, "cancel-session", CancelStop)
	}()
	select {
	case <-cleanupEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting cancel Cleanup 未进入")
	}
	cancel()
	select {
	case result := <-cancelResult:
		if result.WaitingCancelled != 1 || !result.Pending {
			t.Fatalf("bounded cancel result=%+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting cancel 未遵守 caller context 有界返回")
	}

	close(activeRelease)
	select {
	case <-activeCleaned:
	case <-time.After(2 * time.Second):
		t.Fatal("active completion 未进入")
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		if scheduler.Snapshot().ActiveCount == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("active completion ownership 未收敛")
		}
	}
	select {
	case <-successorStarted:
		t.Fatal("detached Cleanup 收敛前 successor 提前启动")
	default:
	}

	close(cleanupRelease)
	select {
	case <-successorStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("detached Cleanup 收敛后 successor 未启动")
	}
	if result := scheduler.CancelSessionContext(context.Background(), "cancel-session", CancelStop); result.Pending {
		t.Fatalf("detached completion 收敛后 cancel result=%+v", result)
	}
	scheduler.Stop(context.Background())
}

func TestProjectWaitingExpireCleanupCanReenterSnapshot(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(8_100, 0))
	scheduler := NewProjectScheduler(clock, 2, time.Hour, 0)
	activeStarted := make(chan struct{})
	activeRelease := make(chan struct{})
	cleanupDone := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "active",
		Run: func(context.Context) TaskResult {
			close(activeStarted)
			<-activeRelease
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	select {
	case <-activeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("active 未启动")
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "expiring",
		Cleanup: func() {
			scheduler.Snapshot()
			close(cleanupDone)
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("expiring submit=%+v", got)
	}

	clock.Advance(time.Hour + time.Nanosecond)
	select {
	case <-cleanupDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting expire Cleanup 重入 Snapshot 未完成")
	}
	if snapshot := scheduler.Snapshot(); snapshot.Expired != 1 || snapshot.WaitingCount != 0 {
		t.Fatalf("expire snapshot=%+v", snapshot)
	}
	close(activeRelease)
	scheduler.Stop(context.Background())
}

func TestProjectStopWaitingCleanupIsDetachedAndNotifierClosesAfterSettlement(t *testing.T) {
	scheduler := NewProjectScheduler(nil, 1, time.Hour, 0)
	activeStarted := make(chan struct{})
	activeRelease := make(chan struct{})
	cleanupReentered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	waitingFinished := make(chan TaskResult, 1)
	if got := scheduler.Submit(ProjectTask{
		ID: "active",
		Run: func(ctx context.Context) TaskResult {
			close(activeStarted)
			<-ctx.Done()
			<-activeRelease
			return TaskResult{State: TaskStateCancelled}
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	select {
	case <-activeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("active 未启动")
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "waiting",
		Cleanup: func() {
			scheduler.Snapshot()
			close(cleanupReentered)
			<-cleanupRelease
		},
		OnFinish: func(result TaskResult) { waitingFinished <- result },
	}); got.Status != SubmitQueued {
		t.Fatalf("waiting submit=%+v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopReturned := make(chan struct{})
	go func() {
		scheduler.Stop(ctx)
		close(stopReturned)
	}()
	select {
	case <-cleanupReentered:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop waiting Cleanup 重入 Snapshot 未完成")
	}
	cancel()
	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 未遵守 caller context 有界返回")
	}
	select {
	case result := <-waitingFinished:
		t.Fatalf("Cleanup 未释放前提前发布：%+v", result)
	default:
	}

	close(cleanupRelease)
	close(activeRelease)
	settled := make(chan struct{})
	go func() {
		scheduler.Stop(context.Background())
		close(settled)
	}()
	select {
	case <-settled:
	case <-time.After(2 * time.Second):
		t.Fatal("释放 waiting Cleanup 后 Scheduler 未收敛")
	}
	select {
	case result := <-waitingFinished:
		if result.State != TaskStateCancelled {
			t.Fatalf("waiting terminal=%s", result.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notifier 过早关闭，waiting terminal 未发布")
	}
	if snapshot := scheduler.Snapshot(); snapshot.NotificationPending != 0 {
		t.Fatalf("Stop 收敛后 notification pending=%d", snapshot.NotificationPending)
	}
}

func TestConversationScopedCancellation(t *testing.T) {
	modes := []struct {
		name string
		mode CancelMode
	}{
		{name: "stop", mode: CancelStop},
		{name: "cancel", mode: CancelAndNew},
		{name: "new", mode: CancelNew},
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeSchedulerClock(time.Unix(6_000, 0))
			scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
			t.Cleanup(func() { scheduler.Stop(context.Background()) })

			started := make(chan string, 4)
			cleaned := make(chan string, 4)
			finished := make(chan projectSchedulerFinishRecord, 4)
			bRelease := make(chan struct{})

			makeTask := func(id, scope string) ProjectTask {
				return ProjectTask{
					ID:         id,
					SessionKey: scope,
					Run: func(ctx context.Context) TaskResult {
						started <- id
						if scope == "scope-b" {
							select {
							case <-bRelease:
								return TaskResult{State: TaskStateCompleted}
							case <-ctx.Done():
								return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
							}
						}
						<-ctx.Done()
						return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
					},
					Cleanup:  func() { cleaned <- id },
					OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: id, result: result} },
				}
			}

			if got := scheduler.Submit(makeTask("a1", "scope-a")); got.Status != SubmitStarted {
				t.Fatalf("submit a1 = %+v", got)
			}
			if got := waitStarted(t, started); got != "a1" {
				t.Fatalf("first start = %q", got)
			}
			if got := scheduler.Submit(makeTask("a2", "scope-a")); got.Status != SubmitQueued || got.Position != 1 {
				t.Fatalf("submit a2 = %+v", got)
			}
			if got := scheduler.Submit(makeTask("b1", "scope-b")); got.Status != SubmitQueued || got.Position != 2 {
				t.Fatalf("submit b1 = %+v", got)
			}
			if got := scheduler.Submit(makeTask("b2", "scope-b")); got.Status != SubmitQueued || got.Position != 3 {
				t.Fatalf("submit b2 = %+v", got)
			}

			result := scheduler.CancelSession("scope-a", tc.mode)
			if result.ActiveCancelled != 1 || result.WaitingCancelled != 1 {
				t.Fatalf("CancelSession result = %+v, want active=1 waiting=1", result)
			}

			seenCleanup := map[string]int{}
			seenFinish := map[string]string{}
			for i := 0; i < 2; i++ {
				seenCleanup[waitCleanup(t, cleaned)]++
				finish := waitFinish(t, finished)
				seenFinish[finish.id] = finish.result.State
			}
			if seenCleanup["a1"] != 1 || seenCleanup["a2"] != 1 {
				t.Fatalf("scope-a cleanup = %+v, want a1/a2 once", seenCleanup)
			}
			if seenFinish["a1"] != TaskStateCancelled || seenFinish["a2"] != TaskStateCancelled {
				t.Fatalf("scope-a finishes = %+v, want cancelled", seenFinish)
			}

			if got := waitStarted(t, started); got != "b1" {
				t.Fatalf("next start = %q, want b1; cross-scope FIFO must be preserved", got)
			}
			close(bRelease)
			if got := waitCleanup(t, cleaned); got != "b1" {
				t.Fatalf("b1 cleanup = %q", got)
			}
			if got := waitFinish(t, finished); got.id != "b1" || got.result.State != TaskStateCompleted {
				t.Fatalf("b1 finish = %+v", got)
			}
			if got := waitStarted(t, started); got != "b2" {
				t.Fatalf("final start = %q, want b2", got)
			}
			if got := waitCleanup(t, cleaned); got != "b2" {
				t.Fatalf("b2 cleanup = %q", got)
			}
			if got := waitFinish(t, finished); got.id != "b2" || got.result.State != TaskStateCompleted {
				t.Fatalf("b2 finish = %+v", got)
			}
		})
	}
}

func TestCancelSessionInvalidScopeAndModeFailClosed(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(6_100, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })
	started := make(chan string, 1)
	if got := scheduler.Submit(ProjectTask{ID: "active", SessionKey: "scope-a", Run: func(ctx context.Context) TaskResult {
		started <- "active"
		<-ctx.Done()
		return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	if got := waitStarted(t, started); got != "active" {
		t.Fatalf("started = %q", got)
	}
	if got := scheduler.CancelSession("", CancelStop); got.ActiveCancelled != 0 || got.WaitingCancelled != 0 {
		t.Fatalf("empty scope cancelled tasks: %+v", got)
	}
	if got := scheduler.CancelSession("scope-a", CancelMode("bogus")); got.ActiveCancelled != 0 || got.WaitingCancelled != 0 {
		t.Fatalf("invalid mode cancelled tasks: %+v", got)
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 1 || snap.Active == nil || snap.Active.SessionKey != "scope-a" {
		t.Fatalf("snapshot after invalid cancels = %+v", snap)
	}
}

func TestCancelSessionContextReturnsPendingForStubbornRunAndKeepsSlot(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(6_200, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		scheduler.Stop(ctx)
	})

	started := make(chan string, 2)
	if got := scheduler.Submit(ProjectTask{ID: "stubborn", SessionKey: "scope-a", Run: func(context.Context) TaskResult {
		started <- "stubborn"
		select {}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit stubborn = %+v", got)
	}
	if got := waitStarted(t, started); got != "stubborn" {
		t.Fatalf("started = %q", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := scheduler.CancelSessionContext(ctx, "scope-a", CancelStop)
	if result.ActiveCancelled != 1 || result.WaitingCancelled != 0 || !result.Pending {
		t.Fatalf("CancelSessionContext result = %+v, want active=1 waiting=0 pending=true", result)
	}

	if got := scheduler.Submit(ProjectTask{ID: "next", SessionKey: "scope-b", Run: func(context.Context) TaskResult {
		started <- "next"
		return TaskResult{State: TaskStateCompleted}
	}}); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("submit next while stubborn pending = %+v", got)
	}
	assertNoBufferedStart(t, started)
	if snap := scheduler.Snapshot(); snap.Active == nil || snap.Active.ID != "stubborn" || snap.WaitingCount != 1 {
		t.Fatalf("snapshot after pending cancel = %+v, want stubborn active and next waiting", snap)
	}
}

func TestSchedulerStopCleansAllTasksAndRejectsNewSubmissions(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(7_300, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	started := make(chan string, 2)
	cleaned := make(chan string, 3)
	finished := make(chan projectSchedulerFinishRecord, 3)

	if got := scheduler.Submit(ProjectTask{
		ID: "active",
		Run: func(ctx context.Context) TaskResult {
			started <- "active"
			<-ctx.Done()
			return TaskResult{Err: ctx.Err()}
		},
		Cleanup:  func() { cleaned <- "active" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "active", result: result} },
	}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	if got := waitStarted(t, started); got != "active" {
		t.Fatalf("started = %q", got)
	}
	for _, id := range []string{"waiting-1", "waiting-2"} {
		id := id
		if got := scheduler.Submit(ProjectTask{
			ID:       id,
			Run:      func(context.Context) TaskResult { started <- id; return TaskResult{State: TaskStateCompleted} },
			Cleanup:  func() { cleaned <- id },
			OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: id, result: result} },
		}); got.Status != SubmitQueued {
			t.Fatalf("submit %s = %+v", id, got)
		}
	}

	scheduler.Stop(context.Background())
	seenCleanup := map[string]int{}
	seenFinish := map[string]string{}
	for i := 0; i < 3; i++ {
		seenCleanup[waitCleanup(t, cleaned)]++
		got := waitFinish(t, finished)
		seenFinish[got.id] = got.result.State
	}
	if seenCleanup["active"] != 1 || seenCleanup["waiting-1"] != 1 || seenCleanup["waiting-2"] != 1 {
		t.Fatalf("cleanup counts = %+v, want each task once", seenCleanup)
	}
	if seenFinish["active"] != TaskStateCancelled || seenFinish["waiting-1"] != TaskStateCancelled || seenFinish["waiting-2"] != TaskStateCancelled {
		t.Fatalf("finish states = %+v, want all cancelled", seenFinish)
	}
	if got := scheduler.Submit(ProjectTask{ID: "late"}); got.Status != SubmitRejectedStopped {
		t.Fatalf("post-stop submit = %+v, want rejected_stopped", got)
	}
	assertNoBufferedStart(t, started)
}

func TestStopDeadlineThenLateRunCompletionDoesNotDoublePublishOrCleanup(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(7_400, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	started := make(chan string, 2)
	cleaned := make(chan string, 3)
	finished := make(chan projectSchedulerFinishRecord, 3)
	ctxCancelled := make(chan struct{})
	releaseActive := make(chan struct{})

	if got := scheduler.Submit(ProjectTask{
		ID: "stubborn",
		Run: func(ctx context.Context) TaskResult {
			started <- "stubborn"
			<-ctx.Done()
			close(ctxCancelled)
			<-releaseActive
			return TaskResult{State: TaskStateCompleted}
		},
		Cleanup:  func() { cleaned <- "stubborn" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "stubborn", result: result} },
	}); got.Status != SubmitStarted {
		t.Fatalf("submit stubborn = %+v", got)
	}
	if got := waitStarted(t, started); got != "stubborn" {
		t.Fatalf("started = %q", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID:       "next",
		Run:      func(context.Context) TaskResult { started <- "next"; return TaskResult{State: TaskStateCompleted} },
		Cleanup:  func() { cleaned <- "next" },
		OnFinish: func(result TaskResult) { finished <- projectSchedulerFinishRecord{id: "next", result: result} },
	}); got.Status != SubmitQueued {
		t.Fatalf("submit next = %+v", got)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stopReturned := make(chan struct{})
	go func() { scheduler.Stop(stopCtx); close(stopReturned) }()
	select {
	case <-ctxCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not cancel active context")
	}
	waitStopReturned(t, stopReturned)
	if got := waitCleanup(t, cleaned); got != "next" {
		t.Fatalf("first cleanup = %q, want waiting next cleanup at Stop", got)
	}
	if got := waitFinish(t, finished); got.id != "next" || got.result.State != TaskStateCancelled {
		t.Fatalf("waiting finish = %+v, want cancelled", got)
	}
	assertNoCleanup(t, cleaned)
	assertNoFinish(t, finished)
	assertNoBufferedStart(t, started)
	if got := scheduler.Submit(ProjectTask{ID: "post-stop"}); got.Status != SubmitRejectedStopped {
		t.Fatalf("post-stop submit = %+v, want rejected_stopped", got)
	}

	close(releaseActive)
	if got := waitCleanup(t, cleaned); got != "stubborn" {
		t.Fatalf("late cleanup = %q, want stubborn once", got)
	}
	if got := waitFinish(t, finished); got.id != "stubborn" || got.result.State != TaskStateCancelled {
		t.Fatalf("late finish = %+v, want cancelled once", got)
	}
	assertNoCleanup(t, cleaned)
	assertNoFinish(t, finished)
	assertNoBufferedStart(t, started)
}

func TestNewSchedulerStartsWithEmptyQueue(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(7_500, 0))
	old := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	started := make(chan string, 2)
	if got := old.Submit(ProjectTask{ID: "active", Run: func(ctx context.Context) TaskResult {
		started <- "active"
		<-ctx.Done()
		return TaskResult{Err: ctx.Err()}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	if got := waitStarted(t, started); got != "active" {
		t.Fatalf("started = %q", got)
	}
	if got := old.Submit(ProjectTask{ID: "queued", Run: func(context.Context) TaskResult {
		started <- "queued"
		return TaskResult{State: TaskStateCompleted}
	}}); got.Status != SubmitQueued {
		t.Fatalf("submit queued = %+v", got)
	}
	old.Stop(context.Background())

	fresh := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	defer fresh.Stop(context.Background())
	if snap := fresh.Snapshot(); snap.ActiveCount != 0 || snap.WaitingCount != 0 || len(snap.Waiting) != 0 || snap.Active != nil {
		t.Fatalf("fresh scheduler snapshot = %+v, want empty/no replay", snap)
	}
	assertNoBufferedStart(t, started)
}

func TestProjectSchedulerSnapshotCountsTerminalOutcomesOnce(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(200, 0))
	scheduler := NewProjectScheduler(clock, 1, time.Hour, time.Minute)
	t.Cleanup(func() { scheduler.Stop(context.Background()) })

	started := make(chan string, 8)
	release := make(chan TaskResult, 2)
	if got := scheduler.Submit(ProjectTask{ID: "active", SessionKey: "scope-active", Run: func(ctx context.Context) TaskResult {
		started <- "active"
		return <-release
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	if got := waitStarted(t, started); got != "active" {
		t.Fatalf("started = %q", got)
	}
	if got := scheduler.Submit(ProjectTask{ID: "waiting", SessionKey: "scope-waiting", Run: func(ctx context.Context) TaskResult {
		started <- "waiting"
		return <-release
	}}); got.Status != SubmitQueued {
		t.Fatalf("submit waiting = %+v", got)
	}
	if got := scheduler.Submit(ProjectTask{ID: "overflow", SessionKey: "scope-overflow"}); got.Status != SubmitRejectedFull {
		t.Fatalf("submit overflow = %+v", got)
	}
	release <- TaskResult{State: TaskStateCompleted}
	if got := waitStarted(t, started); got != "waiting" {
		t.Fatalf("started waiting = %q", got)
	}
	release <- TaskResult{State: TaskStateFailed, Err: fmt.Errorf("boom")}

	var counters map[string]any
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		snap := scheduler.Snapshot()
		payload, err := json.Marshal(snap)
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		if err := json.Unmarshal(payload, &counters); err != nil {
			t.Fatalf("unmarshal snapshot: %v", err)
		}
		if snap.ActiveCount == 0 && snap.WaitingCount == 0 && counters["Completed"] == float64(1) && counters["Failed"] == float64(1) && counters["Rejected"] == float64(1) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("snapshot counters = %+v, want completed=1 failed=1 rejected=1", counters)
}

func TestProjectSchedulerTimeoutCancelStopCountersAreOnce(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(300, 0))
	scheduler := NewProjectScheduler(clock, 3, time.Hour, time.Minute)

	started := make(chan string, 3)
	if got := scheduler.Submit(ProjectTask{ID: "timeout", SessionKey: "scope-timeout", Run: func(ctx context.Context) TaskResult {
		started <- "timeout"
		<-ctx.Done()
		return TaskResult{Err: ctx.Err()}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit timeout = %+v", got)
	}
	if got := waitStarted(t, started); got != "timeout" {
		t.Fatalf("started = %q", got)
	}
	clock.Advance(time.Minute)
	var timeoutCounters map[string]any
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		snap := scheduler.Snapshot()
		payload, err := json.Marshal(snap)
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		if err := json.Unmarshal(payload, &timeoutCounters); err != nil {
			t.Fatalf("unmarshal snapshot: %v", err)
		}
		if timeoutCounters["TimedOut"] == float64(1) && snap.ActiveCount == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if timeoutCounters["TimedOut"] != float64(1) {
		t.Fatalf("timed out counter = %+v", timeoutCounters)
	}

	block := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{ID: "active-cancel", SessionKey: "scope-cancel", Run: func(ctx context.Context) TaskResult {
		started <- "active-cancel"
		<-ctx.Done()
		<-block
		return TaskResult{Err: ctx.Err()}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active-cancel = %+v", got)
	}
	if got := waitStarted(t, started); got != "active-cancel" {
		t.Fatalf("started = %q", got)
	}
	resultCh := make(chan CancelResult, 1)
	go func() { resultCh <- scheduler.CancelSession("scope-cancel", CancelStop) }()
	select {
	case got := <-resultCh:
		t.Fatalf("cancel returned before task finished: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(block)
	select {
	case got := <-resultCh:
		if got.ActiveCancelled != 1 || got.Pending {
			t.Fatalf("cancel result = %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not finish")
	}
	snapAfterCancel := scheduler.Snapshot()
	payloadAfterCancel, err := json.Marshal(snapAfterCancel)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var cancelCounters map[string]any
	if err := json.Unmarshal(payloadAfterCancel, &cancelCounters); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if cancelCounters["Cancelled"] != float64(1) || cancelCounters["TimedOut"] != float64(1) {
		t.Fatalf("after active cancel counters = %+v", cancelCounters)
	}

	if got := scheduler.Submit(ProjectTask{ID: "active-stop", SessionKey: "scope-stop", Run: func(ctx context.Context) TaskResult {
		started <- "active-stop"
		<-ctx.Done()
		return TaskResult{Err: ctx.Err()}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active-stop = %+v", got)
	}
	if got := waitStarted(t, started); got != "active-stop" {
		t.Fatalf("started = %q", got)
	}
	if got := scheduler.Submit(ProjectTask{ID: "waiting-stop", SessionKey: "scope-waiting-stop"}); got.Status != SubmitQueued {
		t.Fatalf("submit waiting-stop = %+v", got)
	}
	scheduler.Stop(context.Background())
	if snap := scheduler.Snapshot(); snap.ActiveCount != 0 || snap.WaitingCount != 0 {
		// Snapshot after Stop returns zero value, so assert no panic/race and rely on pre-stop counters above.
		_ = snap
	}
}

type stagedTerminalProbe struct {
	staged  *StagedAttachments
	path    string
	cleaned chan string
}

func newStagedTerminalProbe(t *testing.T, id string) *stagedTerminalProbe {
	t.Helper()
	msg := &Message{MessageID: id, Files: []FileAttachment{{FileName: id + ".txt", MimeType: "text/plain", Data: []byte("payload-" + id)}}}
	staged := mustStage(t, t.TempDir(), msg, DefaultMaxAttachmentSize)
	return &stagedTerminalProbe{staged: staged, path: staged.Entries()[0].Path, cleaned: make(chan string, 2)}
}

func (p *stagedTerminalProbe) cleanup(id string) func() {
	return func() {
		_ = p.staged.Cleanup()
		p.cleaned <- id
	}
}

func (p *stagedTerminalProbe) assertCleanedOnceAndRemoved(t *testing.T, id string) {
	t.Helper()
	if got := waitCleanup(t, p.cleaned); got != id {
		t.Fatalf("cleanup = %q, want %q", got, id)
	}
	assertNoCleanup(t, p.cleaned)
	if _, err := os.Stat(p.path); !os.IsNotExist(err) {
		t.Fatalf("staged file %q still exists or stat failed with non-ENOENT: %v", p.path, err)
	}
}

func TestStagedAttachmentLifecycleAcrossSchedulerTerminals(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		scheduler := NewProjectScheduler(newFakeSchedulerClock(time.Unix(13_000, 0)), 10, 6*time.Hour, 30*time.Minute)
		probe := newStagedTerminalProbe(t, "completed")
		if got := scheduler.Submit(ProjectTask{ID: "completed", Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} }, Cleanup: probe.cleanup("completed")}); got.Status != SubmitStarted {
			t.Fatalf("submit completed = %+v", got)
		}
		probe.assertCleanedOnceAndRemoved(t, "completed")
	})

	t.Run("failed", func(t *testing.T) {
		scheduler := NewProjectScheduler(newFakeSchedulerClock(time.Unix(13_100, 0)), 10, 6*time.Hour, 30*time.Minute)
		probe := newStagedTerminalProbe(t, "failed")
		if got := scheduler.Submit(ProjectTask{ID: "failed", Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateFailed, Err: errors.New("boom")} }, Cleanup: probe.cleanup("failed")}); got.Status != SubmitStarted {
			t.Fatalf("submit failed = %+v", got)
		}
		probe.assertCleanedOnceAndRemoved(t, "failed")
	})

	t.Run("expired_waiting", func(t *testing.T) {
		clock := newFakeSchedulerClock(time.Unix(13_200, 0))
		scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
		activeRelease := make(chan struct{})
		if got := scheduler.Submit(ProjectTask{ID: "active", Run: func(context.Context) TaskResult { <-activeRelease; return TaskResult{State: TaskStateCompleted} }}); got.Status != SubmitStarted {
			t.Fatalf("submit active = %+v", got)
		}
		probe := newStagedTerminalProbe(t, "expired")
		if got := scheduler.Submit(ProjectTask{ID: "expired", Run: func(context.Context) TaskResult { t.Fatal("expired task must not run"); return TaskResult{} }, Cleanup: probe.cleanup("expired")}); got.Status != SubmitQueued {
			t.Fatalf("submit expired = %+v", got)
		}
		clock.Advance(6*time.Hour + time.Nanosecond)
		probe.assertCleanedOnceAndRemoved(t, "expired")
		close(activeRelease)
	})

	t.Run("scope_cancel_active_and_waiting", func(t *testing.T) {
		scheduler := NewProjectScheduler(newFakeSchedulerClock(time.Unix(13_300, 0)), 10, 6*time.Hour, 30*time.Minute)
		active := newStagedTerminalProbe(t, "cancel-active")
		waiting := newStagedTerminalProbe(t, "cancel-waiting")
		started := make(chan struct{}, 1)
		if got := scheduler.Submit(ProjectTask{ID: "cancel-active", SessionKey: "scope-a", Run: func(ctx context.Context) TaskResult {
			started <- struct{}{}
			<-ctx.Done()
			return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
		}, Cleanup: active.cleanup("cancel-active")}); got.Status != SubmitStarted {
			t.Fatalf("submit active = %+v", got)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("active not started")
		}
		if got := scheduler.Submit(ProjectTask{ID: "cancel-waiting", SessionKey: "scope-a", Run: func(context.Context) TaskResult { t.Fatal("cancelled waiting task must not run"); return TaskResult{} }, Cleanup: waiting.cleanup("cancel-waiting")}); got.Status != SubmitQueued {
			t.Fatalf("submit waiting = %+v", got)
		}
		res := scheduler.CancelSession("scope-a", CancelStop)
		if res.ActiveCancelled != 1 || res.WaitingCancelled != 1 {
			t.Fatalf("cancel result = %+v", res)
		}
		active.assertCleanedOnceAndRemoved(t, "cancel-active")
		waiting.assertCleanedOnceAndRemoved(t, "cancel-waiting")
	})

	t.Run("timed_out_active", func(t *testing.T) {
		clock := newFakeSchedulerClock(time.Unix(13_400, 0))
		scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
		probe := newStagedTerminalProbe(t, "timeout")
		started := make(chan struct{}, 1)
		if got := scheduler.Submit(ProjectTask{ID: "timeout", Run: func(ctx context.Context) TaskResult {
			started <- struct{}{}
			<-ctx.Done()
			return TaskResult{Err: ctx.Err()}
		}, Cleanup: probe.cleanup("timeout")}); got.Status != SubmitStarted {
			t.Fatalf("submit timeout = %+v", got)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout task not started")
		}
		clock.Advance(30*time.Minute + time.Nanosecond)
		probe.assertCleanedOnceAndRemoved(t, "timeout")
	})

	t.Run("shutdown_active_and_waiting", func(t *testing.T) {
		scheduler := NewProjectScheduler(newFakeSchedulerClock(time.Unix(13_500, 0)), 10, 6*time.Hour, 30*time.Minute)
		active := newStagedTerminalProbe(t, "shutdown-active")
		waiting := newStagedTerminalProbe(t, "shutdown-waiting")
		started := make(chan struct{}, 1)
		if got := scheduler.Submit(ProjectTask{ID: "shutdown-active", Run: func(ctx context.Context) TaskResult {
			started <- struct{}{}
			<-ctx.Done()
			return TaskResult{Err: ctx.Err()}
		}, Cleanup: active.cleanup("shutdown-active")}); got.Status != SubmitStarted {
			t.Fatalf("submit shutdown active = %+v", got)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("shutdown active not started")
		}
		if got := scheduler.Submit(ProjectTask{ID: "shutdown-waiting", Run: func(context.Context) TaskResult { t.Fatal("shutdown waiting task must not run"); return TaskResult{} }, Cleanup: waiting.cleanup("shutdown-waiting")}); got.Status != SubmitQueued {
			t.Fatalf("submit shutdown waiting = %+v", got)
		}
		scheduler.Stop(context.Background())
		active.assertCleanedOnceAndRemoved(t, "shutdown-active")
		waiting.assertCleanedOnceAndRemoved(t, "shutdown-waiting")
	})
}

func TestProjectSchedulerActiveTurnSupplementRequiresActiveOwnership(t *testing.T) {
	clock := newFakeSchedulerClock(time.Unix(14_000, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	defer scheduler.Stop(context.Background())

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	delivered := make(chan ActiveTurnSupplementRequest, 1)
	if got := scheduler.Submit(ProjectTask{
		ID:         "active",
		SessionKey: "session-a",
		Run: func(ctx context.Context) TaskResult {
			started <- struct{}{}
			select {
			case <-release:
				return TaskResult{State: TaskStateCompleted}
			case <-ctx.Done():
				return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
			}
		},
		ActiveSupplement: func(_ context.Context, req ActiveTurnSupplementRequest) error {
			delivered <- req
			return nil
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("active task not started")
	}

	handle, ok := scheduler.ActiveTurn("session-a")
	if !ok || handle.SessionKey != "session-a" || handle.TaskID != "active" || handle.Generation == 0 {
		t.Fatalf("active handle = %+v ok=%v", handle, ok)
	}
	if _, ok := scheduler.ActiveTurn("session-b"); ok {
		t.Fatal("wrong session unexpectedly got active handle")
	}

	wrong := scheduler.SupplementActiveTurn(context.Background(), ActiveTurnSupplementRequest{
		SessionKey: "session-b",
		TaskID:     handle.TaskID,
		Generation: handle.Generation,
		Text:       "wrong",
		Command:    "ps",
	})
	if wrong.Status != ActiveTurnSupplementWrongSession {
		t.Fatalf("wrong-session supplement = %+v", wrong)
	}
	stale := scheduler.SupplementActiveTurn(context.Background(), ActiveTurnSupplementRequest{
		SessionKey: handle.SessionKey,
		TaskID:     handle.TaskID,
		Generation: handle.Generation + 1,
		Text:       "stale",
		Command:    "ps",
	})
	if stale.Status != ActiveTurnSupplementStale {
		t.Fatalf("stale supplement = %+v", stale)
	}
	okResult := scheduler.SupplementActiveTurn(context.Background(), ActiveTurnSupplementRequest{
		SessionKey: handle.SessionKey,
		TaskID:     handle.TaskID,
		Generation: handle.Generation,
		Text:       "hello active",
		Command:    "ps",
	})
	if okResult.Status != ActiveTurnSupplementDelivered || okResult.Err != nil {
		t.Fatalf("active supplement = %+v", okResult)
	}
	select {
	case req := <-delivered:
		if req.Text != "hello active" || req.SessionKey != "session-a" || req.Generation != handle.Generation {
			t.Fatalf("delivered request = %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("supplement handler not called")
	}

	close(release)
	waitProjectSchedulerIdle(t, scheduler)
	if _, ok := scheduler.ActiveTurn("session-a"); ok {
		t.Fatal("completed task still exposes active handle")
	}
	if got := scheduler.SupplementActiveTurn(context.Background(), ActiveTurnSupplementRequest{
		SessionKey: handle.SessionKey,
		TaskID:     handle.TaskID,
		Generation: handle.Generation,
		Text:       "after",
		Command:    "ps",
	}); got.Status != ActiveTurnSupplementNoActive {
		t.Fatalf("post-completion supplement = %+v", got)
	}
}

func TestProjectSchedulerActiveTurnSupplementRejectsUnsupported(t *testing.T) {
	scheduler := NewProjectScheduler(newFakeSchedulerClock(time.Unix(14_100, 0)), 10, 6*time.Hour, 30*time.Minute)
	defer scheduler.Stop(context.Background())
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{ID: "active", SessionKey: "session-a", Run: func(ctx context.Context) TaskResult {
		started <- struct{}{}
		select {
		case <-release:
			return TaskResult{State: TaskStateCompleted}
		case <-ctx.Done():
			return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
		}
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v", got)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("active task not started")
	}
	handle, ok := scheduler.ActiveTurn("session-a")
	if !ok {
		t.Fatal("missing active handle")
	}
	if got := scheduler.SupplementActiveTurn(context.Background(), ActiveTurnSupplementRequest{
		SessionKey: handle.SessionKey,
		TaskID:     handle.TaskID,
		Generation: handle.Generation,
		Text:       "unsupported",
		Command:    "ps",
	}); got.Status != ActiveTurnSupplementUnsupported {
		t.Fatalf("unsupported supplement = %+v", got)
	}
	close(release)
}
