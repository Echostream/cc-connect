package core

import (
	"context"
	"testing"
	"time"
)

func waitAdmissionStarted(t *testing.T, started <-chan string, want string) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("started task = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for task %q to start", want)
	}
}

func blockingAdmissionTask(id, sessionKey string, started chan<- string, release <-chan struct{}) ProjectTask {
	return ProjectTask{
		ID:         id,
		SessionKey: sessionKey,
		Run: func(context.Context) TaskResult {
			started <- id
			<-release
			return TaskResult{State: TaskStateCompleted}
		},
	}
}

func TestLegacySchedulerProactiveAdmissionUsesTargetLane(t *testing.T) {
	scheduler := NewLegacyScheduler(2)
	defer scheduler.Stop(context.Background())

	started := make(chan string, 3)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	if got := scheduler.Submit(blockingAdmissionTask("a-active", "session-a", started, releaseA)); got.Status != SubmitStarted {
		t.Fatalf("submit A = %+v, want started", got)
	}
	waitAdmissionStarted(t, started, "a-active")

	if got := scheduler.SubmitProactive(ProjectTask{ID: "a-proactive", SessionKey: "session-a"}); got.Status != SubmitRejectedBusy {
		t.Fatalf("proactive A = %+v, want rejected busy", got)
	}
	snapshot := scheduler.Snapshot()
	if snapshot.WaitingCount != 0 {
		t.Fatalf("waiting after rejected proactive = %d, want 0", snapshot.WaitingCount)
	}

	if got := scheduler.SubmitProactive(blockingAdmissionTask("b-proactive", "session-b", started, releaseB)); got.Status != SubmitStarted {
		t.Fatalf("proactive B = %+v, want started", got)
	}
	waitAdmissionStarted(t, started, "b-proactive")

	close(releaseA)
	close(releaseB)
}

func TestLegacySchedulerOnlyIfIdleUsesTargetLane(t *testing.T) {
	scheduler := NewLegacyScheduler(2)
	defer scheduler.Stop(context.Background())

	started := make(chan string, 3)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	if got := scheduler.Submit(blockingAdmissionTask("a-active", "session-a", started, releaseA)); got.Status != SubmitStarted {
		t.Fatalf("submit A = %+v, want started", got)
	}
	waitAdmissionStarted(t, started, "a-active")

	if got := scheduler.SubmitOnlyIfIdle(ProjectTask{ID: "a-idle", SessionKey: "session-a"}); got.Status != SubmitRejectedBusy {
		t.Fatalf("only-if-idle A = %+v, want rejected busy", got)
	}
	if got := scheduler.SubmitOnlyIfIdle(blockingAdmissionTask("b-idle", "session-b", started, releaseB)); got.Status != SubmitStarted {
		t.Fatalf("only-if-idle B = %+v, want started", got)
	}
	waitAdmissionStarted(t, started, "b-idle")

	close(releaseA)
	close(releaseB)
}

func TestSubmitOptionsOnlyIfIdleCompatibilityAlias(t *testing.T) {
	cases := []struct {
		name      string
		scheduler interface {
			Submit(ProjectTask) SubmitResult
			SubmitWithOptions(ProjectTask, SubmitOptions) SubmitResult
			Stop(context.Context)
		}
	}{
		{name: "legacy", scheduler: NewLegacyScheduler(1)},
		{name: "project", scheduler: NewProjectScheduler(nil, 1, 0, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.scheduler.Stop(context.Background())
			started := make(chan string, 1)
			release := make(chan struct{})
			if got := tc.scheduler.Submit(blockingAdmissionTask("active", "session-a", started, release)); got.Status != SubmitStarted {
				t.Fatalf("submit active = %+v, want started", got)
			}
			waitAdmissionStarted(t, started, "active")
			got := tc.scheduler.SubmitWithOptions(
				ProjectTask{ID: "compat", SessionKey: "session-a"},
				SubmitOptions{true},
			)
			if got.Status != SubmitRejectedBusy {
				t.Fatalf("compat only-if-idle = %+v, want rejected busy", got)
			}
			close(release)
		})
	}
}

func TestProjectSchedulerProactiveAdmissionPreservesGlobalFIFO(t *testing.T) {
	scheduler := NewProjectScheduler(nil, 2, time.Hour, 0)
	defer scheduler.Stop(context.Background())

	started := make(chan string, 3)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	if got := scheduler.Submit(blockingAdmissionTask("a-active", "session-a", started, releaseA)); got.Status != SubmitStarted {
		t.Fatalf("submit A = %+v, want started", got)
	}
	waitAdmissionStarted(t, started, "a-active")

	if got := scheduler.SubmitProactive(blockingAdmissionTask("b-proactive", "session-b", started, releaseB)); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("proactive B = %+v, want queued at position 1", got)
	}
	close(releaseA)
	waitAdmissionStarted(t, started, "b-proactive")
	close(releaseB)
}

func TestLegacySchedulerInteractiveAdmissionStillQueuesTargetLane(t *testing.T) {
	scheduler := NewLegacyScheduler(2)
	defer scheduler.Stop(context.Background())

	started := make(chan string, 2)
	releaseA := make(chan struct{})
	if got := scheduler.Submit(blockingAdmissionTask("a-active", "session-a", started, releaseA)); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v, want started", got)
	}
	waitAdmissionStarted(t, started, "a-active")

	if got := scheduler.Submit(ProjectTask{ID: "a-waiting", SessionKey: "session-a"}); got.Status != SubmitQueued || got.Position != 1 {
		t.Fatalf("interactive A = %+v, want queued at position 1", got)
	}
	close(releaseA)
}

func TestLegacySchedulerRejectedProactiveKeepsCallerOwnership(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	defer scheduler.Stop(context.Background())

	started := make(chan string, 1)
	release := make(chan struct{})
	if got := scheduler.Submit(blockingAdmissionTask("active", "session-a", started, release)); got.Status != SubmitStarted {
		t.Fatalf("submit active = %+v, want started", got)
	}
	waitAdmissionStarted(t, started, "active")

	cleaned := make(chan struct{}, 1)
	finished := make(chan struct{}, 1)
	got := scheduler.SubmitProactive(ProjectTask{
		ID:         "rejected",
		SessionKey: "session-a",
		Cleanup:    func() { cleaned <- struct{}{} },
		OnFinish:   func(TaskResult) { finished <- struct{}{} },
	})
	if got.Status != SubmitRejectedBusy {
		t.Fatalf("proactive submit = %+v, want rejected busy", got)
	}
	select {
	case <-cleaned:
		t.Fatal("rejected proactive cleanup was called")
	default:
	}
	select {
	case <-finished:
		t.Fatal("rejected proactive OnFinish was called")
	default:
	}
	close(release)
}

func TestLegacySchedulerAcceptedProactiveTransfersOwnership(t *testing.T) {
	scheduler := NewLegacyScheduler(1)
	defer scheduler.Stop(context.Background())

	cleaned := make(chan struct{}, 1)
	finished := make(chan TaskResult, 1)
	got := scheduler.SubmitProactive(ProjectTask{
		ID:         "accepted",
		SessionKey: "session-a",
		Run: func(context.Context) TaskResult {
			return TaskResult{State: TaskStateCompleted}
		},
		Cleanup:  func() { cleaned <- struct{}{} },
		OnFinish: func(result TaskResult) { finished <- result },
	})
	if got.Status != SubmitStarted {
		t.Fatalf("proactive submit = %+v, want started", got)
	}
	waitLegacySignal(t, cleaned, "accepted proactive cleanup")
	var result TaskResult
	select {
	case result = <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for accepted proactive OnFinish")
	}
	if result.State != TaskStateCompleted {
		t.Fatalf("terminal state = %q, want completed", result.State)
	}
}

func TestSchedulersRejectEveryAdmissionAfterStop(t *testing.T) {
	cases := []struct {
		name      string
		scheduler EngineScheduler
	}{
		{name: "legacy", scheduler: NewLegacyScheduler(1)},
		{name: "project", scheduler: NewProjectScheduler(nil, 1, 0, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.scheduler.Stop(context.Background())
			proactive, ok := tc.scheduler.(EngineProactiveScheduler)
			if !ok {
				t.Fatalf("scheduler %T does not implement EngineProactiveScheduler", tc.scheduler)
			}
			for name, submit := range map[string]func(ProjectTask) SubmitResult{
				"interactive":  tc.scheduler.Submit,
				"proactive":    proactive.SubmitProactive,
				"only-if-idle": tc.scheduler.SubmitOnlyIfIdle,
			} {
				cleaned := make(chan struct{}, 1)
				got := submit(ProjectTask{ID: name, SessionKey: "session-a", Cleanup: func() { cleaned <- struct{}{} }})
				if got.Status != SubmitRejectedStopped {
					t.Fatalf("%s submit = %+v, want rejected stopped", name, got)
				}
				select {
				case <-cleaned:
					t.Fatalf("%s rejected task cleanup was called", name)
				default:
				}
			}
		})
	}
}

func TestLegacySchedulerProactiveTreatsWaitingAndCompletingTargetLaneAsBusy(t *testing.T) {
	for _, state := range []string{"waiting", "completing"} {
		t.Run(state, func(t *testing.T) {
			scheduler := NewLegacyScheduler(1)
			scheduler.mu.Lock()
			lane := scheduler.newLaneLocked("session-a")
			switch state {
			case "waiting":
				lane.waiting = []ProjectTask{normalizeLegacyTask(ProjectTask{ID: "waiting", SessionKey: "session-a"})}
				scheduler.waitingCount = 1
			case "completing":
				lane.completing = 1
				lane.completionDone = make(chan struct{})
				scheduler.completionCount = 1
			}
			scheduler.mu.Unlock()

			for admission, submit := range map[string]func(ProjectTask) SubmitResult{
				"proactive":    scheduler.SubmitProactive,
				"only-if-idle": scheduler.SubmitOnlyIfIdle,
			} {
				if got := submit(ProjectTask{ID: admission, SessionKey: "session-a"}); got.Status != SubmitRejectedBusy {
					t.Fatalf("%s submit with %s lane = %+v, want rejected busy", admission, state, got)
				}
			}

			scheduler.mu.Lock()
			lane.waiting = nil
			lane.completing = 0
			if lane.completionDone != nil {
				close(lane.completionDone)
				lane.completionDone = nil
			}
			scheduler.waitingCount = 0
			scheduler.completionCount = 0
			scheduler.deleteLaneIfIdleLocked("session-a", lane)
			scheduler.mu.Unlock()
			scheduler.Stop(context.Background())
		})
	}
}

func TestLegacySchedulerProactiveCannotBypassNotificationBacklog(t *testing.T) {
	scheduler := NewLegacyScheduler(0)
	releaseFinish := make(chan struct{})
	defer func() {
		close(releaseFinish)
		scheduler.Stop(context.Background())
	}()

	finishEntered := make(chan struct{}, 2)
	blockingFinish := func(TaskResult) {
		finishEntered <- struct{}{}
		<-releaseFinish
	}
	for _, id := range []string{"first", "second"} {
		if got := scheduler.Submit(ProjectTask{
			ID: id, SessionKey: "session-a",
			OnFinish: blockingFinish,
		}); got.Status != SubmitStarted {
			t.Fatalf("submit %s = %+v, want started", id, got)
		}
		if id == "first" {
			waitLegacySignal(t, finishEntered, "first OnFinish")
		}
		waitLegacyCondition(t, id+" task release", func() bool {
			return scheduler.Snapshot().ActiveCount == 0
		})
	}
	waitLegacyCondition(t, "notification backlog full", func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.NotificationPending == snapshot.NotificationCapacity
	})

	got := scheduler.SubmitProactive(ProjectTask{
		ID:         "proactive",
		SessionKey: "session-a",
		OnFinish:   func(TaskResult) {},
	})
	if got.Status != SubmitRejectedNotificationBacklog {
		t.Fatalf("proactive submit with full notification backlog = %+v, want rejected notification backlog", got)
	}
}
