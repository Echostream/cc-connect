package core

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func waitLegacyFixSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("等待%s超时", what)
	}
}

func TestLegacySchedulerActiveCleanupCanReenterSnapshot(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	cleanupDone := make(chan struct{})
	finished := make(chan TaskResult, 1)

	got := scheduler.Submit(ProjectTask{
		ID: "active-reentry", SessionKey: "session-a",
		Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		Cleanup: func() {
			scheduler.Snapshot()
			close(cleanupDone)
		},
		OnFinish: func(result TaskResult) { finished <- result },
	})
	if got.Status != SubmitStarted {
		t.Fatalf("submit=%+v, want started", got)
	}
	waitLegacyFixSignal(t, cleanupDone, "active Cleanup 重入 Snapshot")
	if result := <-finished; result.State != TaskStateCompleted {
		t.Fatalf("terminal=%s, want completed", result.State)
	}
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerWaitingCancelCleanupCanReenterSnapshot(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	activeRelease := make(chan struct{})
	activeStarted := make(chan struct{})
	cleanupDone := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			close(activeStarted)
			<-activeRelease
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	waitLegacyFixSignal(t, activeStarted, "active 启动")
	if got := scheduler.Submit(ProjectTask{
		ID: "waiting", SessionKey: "session-a",
		Cleanup: func() {
			scheduler.Snapshot()
			close(cleanupDone)
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("waiting submit=%+v", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	result := scheduler.CancelSessionContext(ctx, "session-a", CancelStop)
	cancel()
	if result.WaitingCancelled != 1 || !result.Pending {
		t.Fatalf("cancel=%+v, want waiting=1 pending", result)
	}
	waitLegacyFixSignal(t, cleanupDone, "waiting Cleanup 重入 Snapshot")
	close(activeRelease)
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerBlockedCleanupDoesNotBlockOtherLane(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "a", SessionKey: "session-a",
		Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		Cleanup: func() {
			close(cleanupEntered)
			<-cleanupRelease
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("A submit=%+v", got)
	}
	waitLegacyFixSignal(t, cleanupEntered, "A Cleanup 阻塞")

	bStarted := make(chan struct{})
	bRelease := make(chan struct{})
	submitDone := make(chan SubmitResult, 1)
	go func() {
		submitDone <- scheduler.Submit(ProjectTask{
			ID: "b", SessionKey: "session-b",
			Run: func(context.Context) TaskResult {
				close(bStarted)
				<-bRelease
				return TaskResult{State: TaskStateCompleted}
			},
		})
	}()
	select {
	case result := <-submitDone:
		if result.Status != SubmitStarted {
			t.Fatalf("B submit=%+v, want started", result)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("A Cleanup 阻塞了 B Submit")
	}
	waitLegacyFixSignal(t, bStarted, "B 启动")
	if snapshot := scheduler.Snapshot(); snapshot.ActiveCount != 2 {
		t.Fatalf("snapshot active=%d, want 2（A completing + B running）", snapshot.ActiveCount)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	cancelResult := scheduler.CancelSessionContext(ctx, "session-b", CancelStop)
	cancel()
	if cancelResult.ActiveCancelled != 1 || !cancelResult.Pending {
		t.Fatalf("cancel B=%+v", cancelResult)
	}

	close(bRelease)
	close(cleanupRelease)
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerActiveCleanupRetainsLaneUntilCompletion(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	successorStarted := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "session-a",
		Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		Cleanup: func() {
			close(cleanupEntered)
			<-cleanupRelease
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "successor", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			close(successorStarted)
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("successor submit=%+v", got)
	}
	waitLegacyFixSignal(t, cleanupEntered, "active Cleanup")
	if snapshot := scheduler.Snapshot(); snapshot.ActiveCount != 1 || snapshot.WaitingCount != 1 {
		t.Fatalf("Cleanup 期间 snapshot=%+v, want active=1 waiting=1", snapshot)
	}
	select {
	case <-successorStarted:
		t.Fatal("同 lane successor 在 Cleanup 返回前启动")
	default:
	}
	close(cleanupRelease)
	waitLegacyFixSignal(t, successorStarted, "Cleanup 后 successor 启动")
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerWaitingCleanupOwnerPreventsSameKeyBypass(t *testing.T) {
	scheduler := NewLegacyScheduler(2)
	activeRelease := make(chan struct{})
	activeStarted := make(chan struct{})
	waitingCleanupEntered := make(chan struct{})
	waitingCleanupRelease := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			close(activeStarted)
			<-activeRelease
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	waitLegacyFixSignal(t, activeStarted, "active 启动")
	if got := scheduler.Submit(ProjectTask{
		ID: "cancelled-waiting", SessionKey: "session-a",
		Cleanup: func() {
			close(waitingCleanupEntered)
			<-waitingCleanupRelease
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("waiting submit=%+v", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	cancelResult := scheduler.CancelSessionContext(ctx, "session-a", CancelStop)
	cancel()
	if cancelResult.WaitingCancelled != 1 || !cancelResult.Pending {
		t.Fatalf("cancel=%+v", cancelResult)
	}
	waitLegacyFixSignal(t, waitingCleanupEntered, "waiting Cleanup")
	close(activeRelease)
	waitLegacyCondition(t, "active ownership 在 Run 返回后释放", func() bool {
		return scheduler.Snapshot().ActiveCount == 0
	})

	newStarted := make(chan struct{})
	submitDone := make(chan SubmitResult, 1)
	go func() {
		submitDone <- scheduler.Submit(ProjectTask{
			ID: "new", SessionKey: "session-a",
			Run: func(context.Context) TaskResult {
				close(newStarted)
				return TaskResult{State: TaskStateCompleted}
			},
		})
	}()
	select {
	case result := <-submitDone:
		if result.Status != SubmitQueued {
			t.Fatalf("new submit=%+v, want queued behind completion owner", result)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("waiting Cleanup 阻塞了同 key Submit")
	}
	if snapshot := scheduler.Snapshot(); snapshot.ActiveCount != 0 || snapshot.WaitingCount != 1 {
		t.Fatalf("waiting Cleanup owner 期间 snapshot=%+v, want active=0 waiting=1", snapshot)
	}
	select {
	case <-newStarted:
		t.Fatal("waiting Cleanup owner 后的新任务提前启动")
	default:
	}
	close(waitingCleanupRelease)
	waitLegacyFixSignal(t, newStarted, "waiting Cleanup 后的新任务")
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerStopExpiredDuringBlockedCleanupReturnsAndEventuallySettles(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	finished := make(chan TaskResult, 1)
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "session-a",
		Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		Cleanup: func() {
			close(cleanupEntered)
			<-cleanupRelease
		},
		OnFinish: func(result TaskResult) { finished <- result },
	}); got.Status != SubmitStarted {
		t.Fatalf("submit=%+v", got)
	}
	waitLegacyFixSignal(t, cleanupEntered, "Cleanup 阻塞")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopReturned := make(chan struct{})
	go func() {
		scheduler.Stop(ctx)
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Stop(expiredCtx) 被不合作 Cleanup 阻塞")
	}
	close(cleanupRelease)
	if result := <-finished; result.State != TaskStateCompleted {
		t.Fatalf("terminal=%s, want completed", result.State)
	}
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerCleanupFreezesCompletedDecisionAgainstCancelAndStop(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	finished := make(chan TaskResult, 1)
	if got := scheduler.Submit(ProjectTask{
		ID: "completed", SessionKey: "session-a",
		Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		Cleanup: func() {
			close(cleanupEntered)
			<-cleanupRelease
		},
		OnFinish: func(result TaskResult) { finished <- result },
	}); got.Status != SubmitStarted {
		t.Fatalf("submit=%+v", got)
	}
	waitLegacyFixSignal(t, cleanupEntered, "Cleanup 阻塞")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	cancelResult := scheduler.CancelSessionContext(ctx, "session-a", CancelStop)
	cancel()
	if cancelResult.ActiveCancelled != 0 || !cancelResult.Pending {
		t.Fatalf("completing active cancel=%+v, want activeCancelled=0 pending", cancelResult)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	scheduler.Stop(stopCtx)
	stopCancel()
	close(cleanupRelease)
	if result := <-finished; result.State != TaskStateCompleted {
		t.Fatalf("Cancel/Stop 改写 terminal=%s, want completed", result.State)
	}
	scheduler.Stop(context.Background())
	if snapshot := scheduler.Snapshot(); snapshot.Completed != 1 || snapshot.Cancelled != 0 {
		t.Fatalf("terminal counter=%+v, want completed=1 cancelled=0", snapshot)
	}
}

func TestLegacySchedulerStopWaitsForCompletionSendersBeforeClosingEvents(t *testing.T) {
	scheduler := NewLegacyScheduler(2)
	activeRelease := make(chan struct{})
	activeStarted := make(chan struct{})
	waitingCleanupEntered := make(chan struct{})
	waitingCleanupRelease := make(chan struct{})
	waitingFinished := make(chan TaskResult, 1)
	activeFinished := make(chan TaskResult, 1)
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			close(activeStarted)
			<-activeRelease
			return TaskResult{State: TaskStateCompleted}
		},
		OnFinish: func(result TaskResult) { activeFinished <- result },
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	waitLegacyFixSignal(t, activeStarted, "active 启动")
	if got := scheduler.Submit(ProjectTask{
		ID: "waiting", SessionKey: "session-a",
		Cleanup: func() {
			close(waitingCleanupEntered)
			<-waitingCleanupRelease
		},
		OnFinish: func(result TaskResult) { waitingFinished <- result },
	}); got.Status != SubmitQueued {
		t.Fatalf("waiting submit=%+v", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	scheduler.Stop(ctx)
	cancel()
	waitLegacyFixSignal(t, waitingCleanupEntered, "Stop 摘除 waiting 后 Cleanup")
	close(activeRelease)
	close(waitingCleanupRelease)
	if result := <-waitingFinished; result.State != TaskStateCancelled {
		t.Fatalf("waiting terminal=%s", result.State)
	}
	if result := <-activeFinished; result.State != TaskStateCancelled {
		t.Fatalf("active terminal=%s", result.State)
	}
	scheduler.Stop(context.Background())
}

func TestSchedulerConstructorsDoNotPanicOnOverflowingNotificationCapacity(t *testing.T) {
	constructors := []struct {
		name string
		new  func()
	}{
		{name: "legacy-max-int", new: func() { NewLegacyScheduler(math.MaxInt).Stop(context.Background()) }},
		{name: "legacy-overflow-boundary", new: func() { NewLegacyScheduler(math.MaxInt / 2).Stop(context.Background()) }},
		{name: "project-max-int", new: func() { NewProjectScheduler(nil, math.MaxInt, 0, 0).Stop(context.Background()) }},
		{name: "project-overflow-boundary", new: func() { NewProjectScheduler(nil, math.MaxInt/2, 0, 0).Stop(context.Background()) }},
	}
	for _, tc := range constructors {
		t.Run(tc.name, func(t *testing.T) { tc.new() })
	}
}

func TestLegacySchedulerTryBeginLocalMutationRechecksExpiredContextAfterLock(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	scheduler.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan bool, 1)
	go func() {
		_, ok := scheduler.TryBeginLocalMutation(ctx)
		result <- ok
	}()
	cancel()
	scheduler.mu.Unlock()
	if ok := <-result; ok {
		t.Fatal("context 等锁期间过期后仍授予 mutation lease")
	}
	if got := scheduler.Submit(ProjectTask{ID: "after", SessionKey: "session-a"}); got.Status != SubmitStarted {
		t.Fatalf("过期 lease 泄漏导致 submit=%+v", got)
	}
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerFailedTerminalCompletesExactlyOnceAndStartsSuccessorOnce(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	failedRelease := make(chan struct{})
	failedStarted := make(chan struct{})
	failedFinished := make(chan TaskResult, 2)
	successorStarted := make(chan struct{}, 2)
	sentinelFinished := make(chan struct{})
	var failedCleanup atomic.Int32
	var failedFinishes atomic.Int32
	var successorRuns atomic.Int32
	if got := scheduler.Submit(ProjectTask{
		ID: "failed", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			close(failedStarted)
			<-failedRelease
			return TaskResult{Err: fmt.Errorf("boom")}
		},
		Cleanup: func() { failedCleanup.Add(1) },
		OnFinish: func(result TaskResult) {
			failedFinishes.Add(1)
			failedFinished <- result
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("failed submit=%+v", got)
	}
	waitLegacyFixSignal(t, failedStarted, "failed task 启动")
	if got := scheduler.Submit(ProjectTask{
		ID: "successor", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			successorRuns.Add(1)
			successorStarted <- struct{}{}
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("successor submit=%+v", got)
	}
	close(failedRelease)
	if result := <-failedFinished; result.State != TaskStateFailed {
		t.Fatalf("failed terminal=%s", result.State)
	}
	waitLegacyFixSignal(t, successorStarted, "successor 启动")
	waitLegacyCondition(t, "failed successor 收敛", func() bool {
		return scheduler.Snapshot().ActiveCount == 0
	})
	if got := scheduler.Submit(ProjectTask{
		ID: "notification-sentinel", SessionKey: "session-sentinel",
		OnFinish: func(TaskResult) { close(sentinelFinished) },
	}); got.Status != SubmitStarted {
		t.Fatalf("sentinel submit=%+v", got)
	}
	waitLegacyFixSignal(t, sentinelFinished, "notifier FIFO sentinel 发布")
	if got := failedCleanup.Load(); got != 1 {
		t.Fatalf("failed cleanup=%d, want 1", got)
	}
	if got := failedFinishes.Load(); got != 1 {
		t.Fatalf("failed OnFinish=%d, want 1", got)
	}
	if got := successorRuns.Load(); got != 1 {
		t.Fatalf("successor runs=%d, want 1", got)
	}
	if snapshot := scheduler.Snapshot(); snapshot.Failed != 1 {
		t.Fatalf("failed counter=%d, want 1", snapshot.Failed)
	}
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerActiveCleanupPanicStillPublishesAndReleasesSlot(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	finished := make(chan TaskResult, 2)
	successorStarted := make(chan struct{}, 2)
	var cleanupAttempts atomic.Int32
	if got := scheduler.Submit(ProjectTask{
		ID: "panic-active", SessionKey: "session-a",
		Run: func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		Cleanup: func() {
			cleanupAttempts.Add(1)
			panic("cleanup must be isolated")
		},
		OnFinish: func(result TaskResult) { finished <- result },
	}); got.Status != SubmitStarted {
		t.Fatalf("panic active submit=%+v", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "successor", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			successorStarted <- struct{}{}
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("successor submit=%+v", got)
	}

	if result := <-finished; result.State != TaskStateCompleted {
		t.Fatalf("panic active terminal=%s", result.State)
	}
	waitLegacyFixSignal(t, successorStarted, "Cleanup panic 后 successor 启动")
	waitLegacyCondition(t, "Cleanup panic 后 lane 和 slot 收敛", func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.ActiveCount == 0 && snapshot.WaitingCount == 0 && snapshot.NotificationPending == 0
	})
	if got := cleanupAttempts.Load(); got != 1 {
		t.Fatalf("cleanup attempts=%d, want 1", got)
	}
	select {
	case <-finished:
		t.Fatal("Cleanup panic 后重复发布 OnFinish")
	default:
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "after-panic", SessionKey: "session-a",
		OnFinish: func(TaskResult) {},
	}); got.Status != SubmitStarted {
		t.Fatalf("Cleanup panic 泄漏 slot，后续 submit=%+v", got)
	}
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerDetachedCleanupPanicStillPublishesAndSettles(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "cancel"
		if stop {
			name = "stop"
		}
		t.Run(name, func(t *testing.T) {
			scheduler := NewLegacyScheduler(1)
			activeStarted := make(chan struct{})
			activeRelease := make(chan struct{})
			finished := make(chan TaskResult, 2)
			if got := scheduler.Submit(ProjectTask{
				ID: "active", SessionKey: "session-a",
				Run: func(context.Context) TaskResult {
					close(activeStarted)
					<-activeRelease
					return TaskResult{State: TaskStateCompleted}
				},
			}); got.Status != SubmitStarted {
				t.Fatalf("active submit=%+v", got)
			}
			waitLegacyFixSignal(t, activeStarted, "active 启动")
			if got := scheduler.Submit(ProjectTask{
				ID: "waiting-panic", SessionKey: "session-a",
				Cleanup:  func() { panic("detached cleanup must be isolated") },
				OnFinish: func(result TaskResult) { finished <- result },
			}); got.Status != SubmitQueued {
				t.Fatalf("waiting submit=%+v", got)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if stop {
				scheduler.Stop(ctx)
			} else if result := scheduler.CancelSessionContext(ctx, "session-a", CancelStop); result.WaitingCancelled != 1 || !result.Pending {
				t.Fatalf("cancel result=%+v", result)
			}
			if result := <-finished; result.State != TaskStateCancelled {
				t.Fatalf("detached terminal=%s", result.State)
			}
			close(activeRelease)
			scheduler.Stop(context.Background())
			if snapshot := scheduler.Snapshot(); snapshot.NotificationPending != 0 {
				t.Fatalf("detached Cleanup panic 后 pending=%d", snapshot.NotificationPending)
			}
			select {
			case result := <-finished:
				t.Fatalf("detached Cleanup panic 后重复发布 OnFinish：%+v", result)
			default:
			}
		})
	}
}

func TestLegacySchedulerOnFinishPanicReleasesSlotAndPublisherContinues(t *testing.T) {
	scheduler := NewLegacyScheduler(0)
	panicPublished := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "panic-finish", SessionKey: "session-a",
		OnFinish: func(TaskResult) {
			close(panicPublished)
			panic("finish must be isolated")
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("panic finish submit=%+v", got)
	}
	waitLegacyFixSignal(t, panicPublished, "panic OnFinish 进入")
	waitLegacyCondition(t, "panic OnFinish slot 释放", func() bool {
		return scheduler.Snapshot().NotificationPending == 0
	})

	finished := make(chan TaskResult, 1)
	if got := scheduler.Submit(ProjectTask{
		ID: "after-finish-panic", SessionKey: "session-a",
		OnFinish: func(result TaskResult) { finished <- result },
	}); got.Status != SubmitStarted {
		t.Fatalf("after panic submit=%+v", got)
	}
	if result := <-finished; result.State != TaskStateCompleted {
		t.Fatalf("after panic terminal=%s", result.State)
	}
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerCancelWaitsForDetachedCleanupAfterActiveExits(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	activeStarted := make(chan struct{})
	waitingCleanupEntered := make(chan struct{})
	waitingCleanupRelease := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "active", SessionKey: "session-a",
		Run: func(ctx context.Context) TaskResult {
			close(activeStarted)
			<-ctx.Done()
			return TaskResult{State: TaskStateCancelled}
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	waitLegacyFixSignal(t, activeStarted, "active 启动")
	if got := scheduler.Submit(ProjectTask{
		ID: "waiting", SessionKey: "session-a",
		Cleanup: func() {
			close(waitingCleanupEntered)
			<-waitingCleanupRelease
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("waiting submit=%+v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelResult := make(chan CancelResult, 1)
	go func() { cancelResult <- scheduler.CancelSessionContext(ctx, "session-a", CancelStop) }()
	waitLegacyFixSignal(t, waitingCleanupEntered, "waiting detached Cleanup")
	waitLegacyCondition(t, "active 快速退出", func() bool { return scheduler.Snapshot().ActiveCount == 0 })
	select {
	case result := <-cancelResult:
		t.Fatalf("detached Cleanup 阻塞时 Cancel 提前返回：%+v", result)
	default:
	}
	cancel()
	result := <-cancelResult
	if result.ActiveCancelled != 1 || result.WaitingCancelled != 1 || !result.Pending {
		t.Fatalf("cancel=%+v, want active=1 waiting=1 pending", result)
	}
	close(waitingCleanupRelease)
	if result := scheduler.CancelSessionContext(context.Background(), "session-a", CancelStop); result.Pending {
		t.Fatalf("Cleanup 释放后 cancel=%+v, want settled", result)
	}
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerCancelObservesDetachedOwnerWithoutActive(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	task := normalizeLegacyTask(ProjectTask{
		ID: "detached-only", SessionKey: "session-a",
		Cleanup: func() {
			close(cleanupEntered)
			<-cleanupRelease
		},
	})

	scheduler.mu.Lock()
	lane := scheduler.newLaneLocked(task.SessionKey)
	lane.waiting = []ProjectTask{task}
	scheduler.waitingCount = 1
	completions := scheduler.detachWaitingLocked(task.SessionKey, lane, TaskStateCancelled)
	scheduler.mu.Unlock()
	scheduler.runDetachedCompletions(completions)
	waitLegacyFixSignal(t, cleanupEntered, "detached-only Cleanup")

	ctx, cancel := context.WithCancel(context.Background())
	cancelResult := make(chan CancelResult, 1)
	go func() { cancelResult <- scheduler.CancelSessionContext(ctx, task.SessionKey, CancelStop) }()
	select {
	case result := <-cancelResult:
		t.Fatalf("detached-only Cleanup 阻塞时 Cancel 提前返回：%+v", result)
	default:
	}
	cancel()
	if result := <-cancelResult; !result.Pending {
		t.Fatalf("detached-only cancel=%+v, want pending", result)
	}
	close(cleanupRelease)
	waitLegacyCondition(t, "detached-only completion 收敛", func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return scheduler.completionCount == 0
	})
	if result := scheduler.CancelSessionContext(context.Background(), task.SessionKey, CancelStop); result.Pending {
		t.Fatalf("detached-only settled cancel=%+v", result)
	}
	scheduler.Stop(context.Background())
}

func TestSchedulerConstructorsKeepLogicalCapacityWithoutHugeAllocation(t *testing.T) {
	cases := []struct {
		name        string
		maxWaiting  int
		wantLogical int
	}{
		{name: "largest-non-overflow", maxWaiting: math.MaxInt/2 - 1, wantLogical: math.MaxInt - 1},
		{name: "first-overflow", maxWaiting: math.MaxInt / 2, wantLogical: 0},
		{name: "max-int", maxWaiting: math.MaxInt, wantLogical: 0},
	}
	for _, tc := range cases {
		t.Run("legacy-"+tc.name, func(t *testing.T) {
			scheduler := NewLegacyScheduler(tc.maxWaiting)
			if got := scheduler.Snapshot().NotificationCapacity; got != tc.wantLogical {
				t.Fatalf("logical capacity=%d, want %d", got, tc.wantLogical)
			}
			scheduler.Stop(context.Background())
		})
		t.Run("project-"+tc.name, func(t *testing.T) {
			scheduler := NewProjectScheduler(nil, tc.maxWaiting, 0, 0)
			if got := scheduler.Snapshot().NotificationCapacity; got != tc.wantLogical {
				t.Fatalf("logical capacity=%d, want %d", got, tc.wantLogical)
			}
			scheduler.Stop(context.Background())
		})
	}
}

func TestLegacySchedulerSlowOnFinishDoesNotBlockLaneRelease(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	successorStarted := make(chan struct{})
	if got := scheduler.Submit(ProjectTask{
		ID: "slow-finish", SessionKey: "session-a",
		OnFinish: func(TaskResult) {
			close(callbackEntered)
			<-callbackRelease
		},
	}); got.Status != SubmitStarted {
		t.Fatalf("slow finish submit=%+v", got)
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "successor", SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			close(successorStarted)
			return TaskResult{State: TaskStateCompleted}
		},
	}); got.Status != SubmitQueued {
		t.Fatalf("successor submit=%+v", got)
	}
	waitLegacyFixSignal(t, callbackEntered, "慢 OnFinish 进入")
	waitLegacyFixSignal(t, successorStarted, "慢 OnFinish 期间 successor 启动")
	if snapshot := scheduler.Snapshot(); snapshot.NotificationPending != 1 {
		t.Fatalf("慢 OnFinish pending=%d, want 1", snapshot.NotificationPending)
	}
	close(callbackRelease)
	waitLegacyCondition(t, "慢 OnFinish slot 释放", func() bool {
		return scheduler.Snapshot().NotificationPending == 0
	})
	scheduler.Stop(context.Background())
}

func TestLegacySchedulerStaleNotificationReleaseCannotDeleteReplacementLane(t *testing.T) {
	scheduler := NewLegacyScheduler(0)
	defer scheduler.Stop(context.Background())

	scheduler.mu.Lock()
	staleLane := scheduler.newLaneLocked("session-a")
	if !staleLane.notificationSlots.reserve() {
		scheduler.mu.Unlock()
		t.Fatal("stale lane notification slot reserve failed")
	}
	staleLane.notificationSlots.release()
	delete(scheduler.lanes, "session-a")
	replacement := scheduler.newLaneLocked("session-a")
	scheduler.deleteLaneIfIdleLocked("session-a", staleLane)
	if got := scheduler.lanes["session-a"]; got != replacement {
		scheduler.mu.Unlock()
		t.Fatalf("stale notification release 删除了 replacement lane：got=%p want=%p", got, replacement)
	}
	scheduler.deleteLaneIfIdleLocked("session-a", replacement)
	scheduler.mu.Unlock()
}

func TestLegacySchedulerTryBeginLocalMutationDeterministicallyRechecksAfterLock(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	scheduler.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	enteredPostPrecheckHelper := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		close(enteredPostPrecheckHelper)
		_, ok := scheduler.tryBeginLocalMutationAfterPrecheck(ctx)
		result <- ok
	}()
	waitLegacyFixSignal(t, enteredPostPrecheckHelper, "mutation 锁前检查后的 helper 入口")
	cancel()
	scheduler.mu.Unlock()
	if ok := <-result; ok {
		t.Fatal("已越过锁前检查的过期 context 获得 mutation lease")
	}
	scheduler.Stop(context.Background())
}

func waitLegacyFeedback(t *testing.T, platform *replyCtxRecordingPlatform, replyCtx string, contains string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		for _, event := range platform.recordedEvents() {
			if event.replyCtx == replyCtx && strings.Contains(event.content, contains) {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("未收到 replyCtx=%v containing %q，events=%+v", replyCtx, contains, platform.recordedEvents())
		case <-time.After(time.Millisecond):
		}
	}
}

func TestEngineLegacySchedulerQueuedRepliesMessageQueued(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	agent := newLegacyEngineAgent()
	agent.workDir = t.TempDir()
	platform := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish, WithScheduler(scheduler))
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:feedback-queued"
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a1", UserID: "u", Content: "a1", ReplyCtx: "r1"})
	session := waitLegacyEngineSession(t, agent, key)
	waitLegacyEngineSend(t, session, "A1 Send")
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a2", UserID: "u", Content: "a2", ReplyCtx: "r2"})
	waitLegacyFeedback(t, platform, "r2", e.i18n.T(MsgMessageQueued))

	session.releases <- struct{}{}
	waitLegacyEngineSend(t, session, "A2 Send")
	session.releases <- struct{}{}
	waitLegacyCondition(t, "feedback 任务收敛", func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.ActiveCount == 0 && snapshot.WaitingCount == 0
	})
}

func TestEngineLegacySchedulerFullRepliesQueueFullWithLaneDepth(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	agent := newLegacyEngineAgent()
	agent.workDir = t.TempDir()
	platform := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish, WithScheduler(scheduler))
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:feedback-full"
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a1", UserID: "u", Content: "a1", ReplyCtx: "r1"})
	session := waitLegacyEngineSession(t, agent, key)
	waitLegacyEngineSend(t, session, "A1 Send")
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a2", UserID: "u", Content: "a2", ReplyCtx: "r2"})
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a3", UserID: "u", Content: "a3", ReplyCtx: "r3"})
	want := fmt.Sprintf(e.i18n.T(MsgQueueFull), 1)
	waitLegacyFeedback(t, platform, "r3", want)

	session.releases <- struct{}{}
	waitLegacyEngineSend(t, session, "A2 Send")
	session.releases <- struct{}{}
	waitLegacyCondition(t, "feedback 任务收敛", func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.ActiveCount == 0 && snapshot.WaitingCount == 0
	})
}

func TestEngineProjectSchedulerFullStillRepliesPreviousProcessing(t *testing.T) {
	scheduler := NewProjectScheduler(nil, 1, time.Hour, 0)
	agent := newLegacyEngineAgent()
	agent.workDir = t.TempDir()
	platform := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish, WithScheduler(scheduler))
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = e.Stop() })

	key := "test:project-feedback-full"
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a1", UserID: "u", Content: "a1", ReplyCtx: "r1"})
	session := waitLegacyEngineSession(t, agent, key)
	waitLegacyEngineSend(t, session, "A1 Send")
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a2", UserID: "u", Content: "a2", ReplyCtx: "r2"})
	e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "a3", UserID: "u", Content: "a3", ReplyCtx: "r3"})
	waitLegacyFeedback(t, platform, "r3", e.i18n.T(MsgPreviousProcessing))
	for _, event := range platform.recordedEvents() {
		if event.replyCtx == "r3" && strings.Contains(event.content, strings.Split(e.i18n.T(MsgQueueFull), "%")[0]) {
			t.Fatalf("Project full 错误回复 MsgQueueFull: %+v", event)
		}
	}

	session.releases <- struct{}{}
	waitLegacyEngineSend(t, session, "A2 Send")
	session.releases <- struct{}{}
	waitLegacyCondition(t, "feedback 任务收敛", func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.ActiveCount == 0 && snapshot.WaitingCount == 0
	})
}
