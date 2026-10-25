package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newLegacySchedulerUnderTest(maxWaiting int) EngineScheduler {
	return NewLegacyScheduler(maxWaiting)
}

type legacyBlockingTask struct {
	task       ProjectTask
	started    chan struct{}
	release    chan struct{}
	cleanup    atomic.Int32
	finish     atomic.Int32
	finishSeen chan TaskResult
}

func newLegacyBlockingTask(id, sessionKey string) *legacyBlockingTask {
	b := &legacyBlockingTask{
		started:    make(chan struct{}),
		release:    make(chan struct{}),
		finishSeen: make(chan TaskResult, 1),
	}
	b.task = ProjectTask{
		ID:         id,
		Source:     TaskSourceInteractive,
		SessionKey: sessionKey,
		Run: func(context.Context) TaskResult {
			close(b.started)
			<-b.release
			return TaskResult{State: TaskStateCompleted}
		},
		Cleanup: func() { b.cleanup.Add(1) },
		OnFinish: func(result TaskResult) {
			b.finish.Add(1)
			b.finishSeen <- result
		},
	}
	return b
}

func waitLegacySignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("等待%s超时", what)
	}
}

func waitLegacyCondition(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待%s超时", what)
}

func stopLegacyScheduler(t *testing.T, scheduler EngineScheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	scheduler.Stop(ctx)
}

func TestLegacySchedulerCrossSessionParallelAndSameSessionFIFO(t *testing.T) {
	scheduler := newLegacySchedulerUnderTest(2)
	defer stopLegacyScheduler(t, scheduler)

	a1 := newLegacyBlockingTask("a1", "session-a")
	a2 := newLegacyBlockingTask("a2", "session-a")
	b1 := newLegacyBlockingTask("b1", "session-b")

	if got := scheduler.Submit(a1.task); got.Status != SubmitStarted {
		t.Fatalf("a1 status=%s, want %s", got.Status, SubmitStarted)
	}
	waitLegacySignal(t, a1.started, "a1 启动")
	if got := scheduler.Submit(a2.task); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("a2 result=%+v, want queued position 1", got)
	}
	if got := scheduler.Submit(b1.task); got.Status != SubmitStarted {
		t.Fatalf("b1 status=%s, want %s（不同 SessionKey 必须并行）", got.Status, SubmitStarted)
	}
	waitLegacySignal(t, b1.started, "b1 并行启动")

	snap := scheduler.Snapshot()
	if snap.ActiveCount != 2 || snap.WaitingCount != 1 {
		t.Fatalf("snapshot active=%d waiting=%d, want 2/1", snap.ActiveCount, snap.WaitingCount)
	}
	if len(snap.ActiveTasks) != 2 || snap.ActiveTasks[0].SessionKey != "session-a" || snap.ActiveTasks[1].SessionKey != "session-b" {
		t.Fatalf("active tasks 未稳定按会话排序: %+v", snap.ActiveTasks)
	}

	close(a1.release)
	waitLegacySignal(t, a2.started, "a2 在 a1 真实返回后启动")
	close(a2.release)
	close(b1.release)
	waitLegacyCondition(t, "三个任务清理和终态通知", func() bool {
		return a1.cleanup.Load() == 1 && a2.cleanup.Load() == 1 && b1.cleanup.Load() == 1 &&
			a1.finish.Load() == 1 && a2.finish.Load() == 1 && b1.finish.Load() == 1
	})
}

func TestProjectSchedulerSnapshotPopulatesCompatibleActiveTasks(t *testing.T) {
	scheduler := NewProjectScheduler(nil, 1, 0, 0)
	task := newLegacyBlockingTask("project-active", "project-session")
	defer scheduler.Stop(context.Background())
	defer close(task.release)
	if got := scheduler.Submit(task.task); got.Status != SubmitStarted {
		t.Fatalf("submit=%+v, want started", got)
	}
	waitLegacySignal(t, task.started, "Project active 启动")

	snapshot := scheduler.Snapshot()
	if snapshot.Active == nil || snapshot.Active.ID != "project-active" {
		t.Fatalf("旧 Active 字段=%+v", snapshot.Active)
	}
	if len(snapshot.ActiveTasks) != 1 || snapshot.ActiveTasks[0].ID != "project-active" {
		t.Fatalf("兼容 ActiveTasks=%+v, want singleton project-active", snapshot.ActiveTasks)
	}
}

func TestLegacySchedulerCapacityIsPerLaneAndExcludesActive(t *testing.T) {
	scheduler := newLegacySchedulerUnderTest(2)
	defer stopLegacyScheduler(t, scheduler)

	a1 := newLegacyBlockingTask("a1", "session-a")
	a2 := newLegacyBlockingTask("a2", "session-a")
	a3 := newLegacyBlockingTask("a3", "session-a")
	a4 := newLegacyBlockingTask("a4", "session-a")
	b1 := newLegacyBlockingTask("b1", "session-b")

	if got := scheduler.Submit(a1.task); got.Status != SubmitStarted {
		t.Fatal(got.Status)
	}
	waitLegacySignal(t, a1.started, "a1 启动")
	if got := scheduler.Submit(a2.task); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("a2=%+v", got)
	}
	if got := scheduler.Submit(a3.task); got.Status != SubmitQueued || got.Position != 2 {
		t.Fatalf("a3=%+v", got)
	}
	if got := scheduler.Submit(a4.task); got.Status != SubmitRejectedFull {
		t.Fatalf("a4=%+v, want rejected_full", got)
	}
	if a4.cleanup.Load() != 0 || a4.finish.Load() != 0 {
		t.Fatalf("拒绝任务被 scheduler 接管: cleanup=%d finish=%d", a4.cleanup.Load(), a4.finish.Load())
	}
	if got := scheduler.Submit(b1.task); got.Status != SubmitStarted {
		t.Fatalf("A lane 满时 B 应启动，got=%+v", got)
	}
	waitLegacySignal(t, b1.started, "b1 启动")

	close(a1.release)
	waitLegacySignal(t, a2.started, "a2 启动")
	close(a2.release)
	waitLegacySignal(t, a3.started, "a3 启动")
	close(a3.release)
	close(b1.release)
	waitLegacyCondition(t, "accepted task cleanup", func() bool {
		return a1.cleanup.Load()+a2.cleanup.Load()+a3.cleanup.Load()+b1.cleanup.Load() == 4
	})
}

func TestLegacySchedulerCancelWaitsForPhysicalRunExitAndDoesNotAffectOtherLane(t *testing.T) {
	scheduler := newLegacySchedulerUnderTest(2)
	defer stopLegacyScheduler(t, scheduler)

	a1 := newLegacyBlockingTask("a1", "session-a")
	a2 := newLegacyBlockingTask("a2", "session-a")
	b1 := newLegacyBlockingTask("b1", "session-b")
	if got := scheduler.Submit(a1.task); got.Status != SubmitStarted {
		t.Fatal(got.Status)
	}
	waitLegacySignal(t, a1.started, "a1 启动")
	if got := scheduler.Submit(a2.task); got.Status != SubmitQueued {
		t.Fatal(got.Status)
	}
	if got := scheduler.Submit(b1.task); got.Status != SubmitStarted {
		t.Fatalf("b1=%+v", got)
	}
	waitLegacySignal(t, b1.started, "b1 启动")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	result := scheduler.CancelSessionContext(ctx, "session-a", CancelStop)
	cancel()
	if result.ActiveCancelled != 1 || result.WaitingCancelled != 1 || !result.Pending {
		t.Fatalf("cancel result=%+v, want active=1 waiting=1 pending", result)
	}
	waitLegacyCondition(t, "a2 waiting cleanup", func() bool { return a2.cleanup.Load() == 1 })
	if a1.cleanup.Load() != 0 {
		t.Fatalf("a1 Run 未退出却提前 cleanup=%d", a1.cleanup.Load())
	}
	select {
	case <-a2.started:
		t.Fatal("cancelled successor 不得启动")
	default:
	}
	snap := scheduler.Snapshot()
	if snap.ActiveCount != 2 || snap.WaitingCount != 0 {
		t.Fatalf("取消 A 未退出时 snapshot=%+v", snap)
	}

	close(a1.release)
	waitLegacyCondition(t, "a1 真实退出后 cleanup", func() bool { return a1.cleanup.Load() == 1 })
	if got := (<-a1.finishSeen).State; got != TaskStateCancelled {
		t.Fatalf("a1 terminal=%s, want cancelled", got)
	}
	if got := (<-a2.finishSeen).State; got != TaskStateCancelled {
		t.Fatalf("a2 terminal=%s, want cancelled", got)
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 1 || snap.Active == nil || snap.Active.SessionKey != "session-b" {
		t.Fatalf("B lane 应继续独立运行: %+v", snap)
	}
	close(b1.release)
}

func TestLegacySchedulerNotificationBacklogIsPerLane(t *testing.T) {
	scheduler := NewLegacyScheduler(0)
	defer stopLegacyScheduler(t, scheduler)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	blockingFinish := func(TaskResult) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	for _, key := range []string{"session-a", "session-b"} {
		got := scheduler.Submit(ProjectTask{
			ID: key, SessionKey: key,
			Run:      func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
			OnFinish: blockingFinish,
		})
		if got.Status != SubmitStarted {
			t.Fatalf("submit %s=%+v, want started", key, got)
		}
	}
	waitLegacySignal(t, entered, "首个 OnFinish 进入")
	waitLegacyCondition(t, "两个 active Run 均已退出", func() bool {
		return scheduler.Snapshot().ActiveCount == 0
	})

	got := scheduler.Submit(ProjectTask{
		ID: "session-c", SessionKey: "session-c",
		Run:      func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} },
		OnFinish: func(TaskResult) {},
	})
	if got.Status != SubmitStarted {
		t.Fatalf("其他 lane 的 notification backlog 不得拒绝空闲新 key：submit=%+v", got)
	}
}

func TestLegacySchedulerNotificationBacklogStillBoundsSameLane(t *testing.T) {
	scheduler := NewLegacyScheduler(0)
	defer stopLegacyScheduler(t, scheduler)

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	blockingFinish := func(TaskResult) {
		close(entered)
		<-release
	}
	if got := scheduler.Submit(ProjectTask{
		ID: "first", SessionKey: "session-a",
		OnFinish: blockingFinish,
	}); got.Status != SubmitStarted {
		t.Fatalf("first submit=%+v", got)
	}
	waitLegacySignal(t, entered, "首个 OnFinish 进入")
	waitLegacyCondition(t, "首个 Run 退出", func() bool {
		return scheduler.Snapshot().ActiveCount == 0
	})
	if got := scheduler.Submit(ProjectTask{
		ID: "second", SessionKey: "session-a",
		OnFinish: func(TaskResult) {},
	}); got.Status != SubmitStarted {
		t.Fatalf("second submit=%+v", got)
	}
	waitLegacyCondition(t, "同 lane 两个 notification ownership", func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.ActiveCount == 0 && snapshot.NotificationPending == 2
	})
	if got := scheduler.Submit(ProjectTask{
		ID: "third", SessionKey: "session-a",
		OnFinish: func(TaskResult) {},
	}); got.Status != SubmitRejectedNotificationBacklog {
		t.Fatalf("same-lane notification backlog submit=%+v, want rejected_notification_backlog", got)
	}
}

func TestLegacySchedulerFinishAndSameSessionSubmitNeverOverlapRuns(t *testing.T) {
	const transitions = 100

	scheduler := newLegacySchedulerUnderTest(1)
	defer stopLegacyScheduler(t, scheduler)

	var running atomic.Int32
	var maxRunning atomic.Int32
	newTrackedTask := func(id string) *legacyBlockingTask {
		task := newLegacyBlockingTask(id, "session-a")
		// 本用例只隔离验证 lane 回收；禁用异步通知，避免把有界通知背压
		// 混入高频 Submit 竞态。通知 exactly-once 由独立 Cancel/Stop 用例覆盖。
		task.task.OnFinish = nil
		task.task.Run = func(context.Context) TaskResult {
			current := running.Add(1)
			for {
				observed := maxRunning.Load()
				if current <= observed || maxRunning.CompareAndSwap(observed, current) {
					break
				}
			}
			close(task.started)
			<-task.release
			running.Add(-1)
			return TaskResult{State: TaskStateCompleted}
		}
		return task
	}

	tasks := make([]*legacyBlockingTask, 0, transitions+1)
	current := newTrackedTask("task-0")
	tasks = append(tasks, current)
	if got := scheduler.Submit(current.task); got.Status != SubmitStarted {
		t.Fatalf("initial submit=%+v, want started", got)
	}
	waitLegacySignal(t, current.started, "initial task 启动")

	for i := 1; i <= transitions; i++ {
		next := newTrackedTask(fmt.Sprintf("task-%d", i))
		tasks = append(tasks, next)
		startRace := make(chan struct{})
		submitResult := make(chan SubmitResult, 1)
		go func(previous *legacyBlockingTask) {
			<-startRace
			close(previous.release)
		}(current)
		go func() {
			<-startRace
			submitResult <- scheduler.Submit(next.task)
		}()
		close(startRace)

		got := <-submitResult
		if got.Status != SubmitStarted && got.Status != SubmitQueued {
			t.Fatalf("transition %d submit=%+v, want started or queued", i, got)
		}
		waitLegacySignal(t, next.started, fmt.Sprintf("task-%d 启动", i))
		current = next
	}
	close(current.release)

	waitLegacyCondition(t, "竞态任务全部 exactly-once cleanup", func() bool {
		for _, task := range tasks {
			if task.cleanup.Load() != 1 {
				return false
			}
		}
		return true
	})
	if got := maxRunning.Load(); got != 1 {
		t.Fatalf("同 session 最大物理 Run 并发=%d, want 1", got)
	}
}

func TestLegacySchedulerRepeatedCancelKeepsAcceptedTasksExactlyOnce(t *testing.T) {
	scheduler := newLegacySchedulerUnderTest(1)
	defer stopLegacyScheduler(t, scheduler)

	active := newLegacyBlockingTask("active", "session-a")
	waiting := newLegacyBlockingTask("waiting", "session-a")
	if got := scheduler.Submit(active.task); got.Status != SubmitStarted {
		t.Fatalf("active submit=%+v", got)
	}
	waitLegacySignal(t, active.started, "active 启动")
	if got := scheduler.Submit(waiting.task); got.Status != SubmitQueued {
		t.Fatalf("waiting submit=%+v", got)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		result := scheduler.CancelSessionContext(ctx, "session-a", CancelStop)
		cancel()
		if result.ActiveCancelled != 1 || result.Pending != true {
			t.Fatalf("cancel %d result=%+v, want active=1 pending", attempt, result)
		}
		wantWaiting := 0
		if attempt == 1 {
			wantWaiting = 1
		}
		if result.WaitingCancelled != wantWaiting {
			t.Fatalf("cancel %d waiting=%d, want %d", attempt, result.WaitingCancelled, wantWaiting)
		}
	}
	if waiting.cleanup.Load() != 1 {
		t.Fatalf("重复 Cancel 后 waiting cleanup=%d, want 1", waiting.cleanup.Load())
	}
	if active.cleanup.Load() != 0 {
		t.Fatalf("active 物理退出前 cleanup=%d, want 0", active.cleanup.Load())
	}

	close(active.release)
	waitLegacyCondition(t, "重复 Cancel 后 exactly-once 终态", func() bool {
		return active.cleanup.Load() == 1 && active.finish.Load() == 1 &&
			waiting.cleanup.Load() == 1 && waiting.finish.Load() == 1
	})
	if got := (<-active.finishSeen).State; got != TaskStateCancelled {
		t.Fatalf("active terminal=%s, want cancelled", got)
	}
	if got := (<-waiting.finishSeen).State; got != TaskStateCancelled {
		t.Fatalf("waiting terminal=%s, want cancelled", got)
	}
}

func TestLegacySchedulerFinishCancelAndStopRaceSettlesExactlyOnce(t *testing.T) {
	const rounds = 50

	for round := 0; round < rounds; round++ {
		scheduler := newLegacySchedulerUnderTest(1)
		active := newLegacyBlockingTask(fmt.Sprintf("active-%d", round), "session-a")
		successor := newLegacyBlockingTask(fmt.Sprintf("successor-%d", round), "session-a")
		successor.task.Run = func(context.Context) TaskResult {
			close(successor.started)
			return TaskResult{State: TaskStateCompleted}
		}

		if got := scheduler.Submit(active.task); got.Status != SubmitStarted {
			t.Fatalf("round %d active submit=%+v", round, got)
		}
		waitLegacySignal(t, active.started, fmt.Sprintf("round %d active 启动", round))
		if got := scheduler.Submit(successor.task); got.Status != SubmitQueued {
			t.Fatalf("round %d successor submit=%+v", round, got)
		}

		startRace := make(chan struct{})
		var racers sync.WaitGroup
		racers.Add(3)
		go func() {
			defer racers.Done()
			<-startRace
			close(active.release)
		}()
		go func() {
			defer racers.Done()
			<-startRace
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			scheduler.CancelSessionContext(ctx, "session-a", CancelStop)
		}()
		go func() {
			defer racers.Done()
			<-startRace
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			scheduler.Stop(ctx)
		}()
		close(startRace)
		racers.Wait()

		waitLegacyCondition(t, fmt.Sprintf("round %d exactly-once 终态", round), func() bool {
			return active.cleanup.Load() == 1 && active.finish.Load() == 1 &&
				successor.cleanup.Load() == 1 && successor.finish.Load() == 1
		})
		if active.cleanup.Load() != 1 || active.finish.Load() != 1 ||
			successor.cleanup.Load() != 1 || successor.finish.Load() != 1 {
			t.Fatalf("round %d exactly-once 破坏: active=%d/%d successor=%d/%d", round,
				active.cleanup.Load(), active.finish.Load(), successor.cleanup.Load(), successor.finish.Load())
		}
		if snap := scheduler.Snapshot(); snap.ActiveCount != 0 || snap.WaitingCount != 0 {
			t.Fatalf("round %d 未收敛: %+v", round, snap)
		}
		scheduler.Stop(context.Background())
	}
}

func TestLegacySchedulerStopIsBoundedButRetainsActiveOwnership(t *testing.T) {
	scheduler := newLegacySchedulerUnderTest(1)
	a1 := newLegacyBlockingTask("a1", "session-a")
	a2 := newLegacyBlockingTask("a2", "session-a")
	if got := scheduler.Submit(a1.task); got.Status != SubmitStarted {
		t.Fatal(got.Status)
	}
	waitLegacySignal(t, a1.started, "a1 启动")
	if got := scheduler.Submit(a2.task); got.Status != SubmitQueued {
		t.Fatal(got.Status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	scheduler.Stop(ctx)
	cancel()
	if got := scheduler.Submit(ProjectTask{ID: "late", SessionKey: "session-b"}); got.Status != SubmitRejectedStopped {
		t.Fatalf("Stop 后 submit=%+v", got)
	}
	waitLegacyCondition(t, "stop 清理 waiting", func() bool { return a2.cleanup.Load() == 1 })
	if a1.cleanup.Load() != 0 {
		t.Fatalf("active Run 未退出却清理: %d", a1.cleanup.Load())
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 1 || snap.WaitingCount != 0 {
		t.Fatalf("有界 Stop 后 active ownership 丢失: %+v", snap)
	}

	close(a1.release)
	waitLegacyCondition(t, "active settle", func() bool {
		return a1.cleanup.Load() == 1 && a1.finish.Load() == 1 && a2.finish.Load() == 1
	})
	if got := (<-a1.finishSeen).State; got != TaskStateCancelled {
		t.Fatalf("active terminal=%s, want cancelled", got)
	}
	if got := (<-a2.finishSeen).State; got != TaskStateCancelled {
		t.Fatalf("waiting terminal=%s, want cancelled", got)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	scheduler.Stop(ctx2)
	if a1.cleanup.Load() != 1 || a2.cleanup.Load() != 1 || a1.finish.Load() != 1 || a2.finish.Load() != 1 {
		t.Fatalf("重复 Stop 破坏 exactly-once: cleanup=%d/%d finish=%d/%d", a1.cleanup.Load(), a2.cleanup.Load(), a1.finish.Load(), a2.finish.Load())
	}
}

func TestLegacySchedulerActiveTurnSupplementTargetsOnlyRequestedLane(t *testing.T) {
	scheduler := NewLegacyScheduler(2)
	defer stopLegacyScheduler(t, scheduler)

	startedA := make(chan struct{})
	startedB := make(chan struct{})
	release := make(chan struct{})
	deliveredA := make(chan ActiveTurnSupplementRequest, 1)
	deliveredB := make(chan ActiveTurnSupplementRequest, 1)
	if got := scheduler.Submit(ProjectTask{ID: "active-a", SessionKey: "session-a", Run: func(ctx context.Context) TaskResult {
		close(startedA)
		select {
		case <-release:
			return TaskResult{State: TaskStateCompleted}
		case <-ctx.Done():
			return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
		}
	}, ActiveSupplement: func(_ context.Context, req ActiveTurnSupplementRequest) error {
		deliveredA <- req
		return nil
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit A = %+v", got)
	}
	if got := scheduler.Submit(ProjectTask{ID: "active-b", SessionKey: "session-b", Run: func(ctx context.Context) TaskResult {
		close(startedB)
		select {
		case <-release:
			return TaskResult{State: TaskStateCompleted}
		case <-ctx.Done():
			return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
		}
	}, ActiveSupplement: func(_ context.Context, req ActiveTurnSupplementRequest) error {
		deliveredB <- req
		return nil
	}}); got.Status != SubmitStarted {
		t.Fatalf("submit B = %+v", got)
	}
	waitLegacySignal(t, startedA, "A 启动")
	waitLegacySignal(t, startedB, "B 启动")

	handleA, ok := scheduler.ActiveTurn("session-a")
	if !ok || handleA.TaskID != "active-a" || handleA.Generation == 0 {
		t.Fatalf("active A handle = %+v ok=%v", handleA, ok)
	}
	handleB, ok := scheduler.ActiveTurn("session-b")
	if !ok || handleB.TaskID != "active-b" || handleB.Generation == 0 {
		t.Fatalf("active B handle = %+v ok=%v", handleB, ok)
	}
	if handleA.Generation == handleB.Generation {
		t.Fatalf("different legacy lanes should have distinct ownership tokens, got A=%d B=%d", handleA.Generation, handleB.Generation)
	}

	got := scheduler.SupplementActiveTurn(context.Background(), ActiveTurnSupplementRequest{
		SessionKey: handleA.SessionKey,
		TaskID:     handleA.TaskID,
		Generation: handleA.Generation,
		Text:       "only A",
		Command:    "btw",
	})
	if got.Status != ActiveTurnSupplementDelivered || got.Err != nil {
		t.Fatalf("supplement A = %+v", got)
	}
	select {
	case req := <-deliveredA:
		if req.Text != "only A" || req.SessionKey != "session-a" {
			t.Fatalf("delivered A = %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("A supplement not delivered")
	}
	select {
	case req := <-deliveredB:
		t.Fatalf("supplement leaked to B: %+v", req)
	default:
	}

	if got := scheduler.SupplementActiveTurn(context.Background(), ActiveTurnSupplementRequest{
		SessionKey: "session-a",
		TaskID:     handleB.TaskID,
		Generation: handleB.Generation,
		Text:       "wrong lane token",
		Command:    "ps",
	}); got.Status != ActiveTurnSupplementStale {
		t.Fatalf("cross-lane token supplement = %+v", got)
	}

	close(release)
}
