package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

type ProjectScheduler struct {
	commands chan schedulerCommand
	done     chan struct{}
}

type schedulerCommand interface{}

type submitCommand struct {
	task    ProjectTask
	options SubmitOptions
	resp    chan SubmitResult
}

type snapshotCommand struct {
	resp chan SchedulerSnapshot
}

type stopCommand struct {
	ctx  context.Context
	done chan struct{}
}

type beginLocalMutationCommand struct {
	ctx  context.Context
	resp chan localMutationLease
}

type endLocalMutationCommand struct{}

type localMutationLease struct {
	release func()
	ok      bool
}

type cancelSessionCommand struct {
	sessionKey string
	mode       CancelMode
	resp       chan CancelResult
	done       chan struct{}
}

type cancelSessionWaiterCancelCommand struct {
	done chan struct{}
}

type runFinishedCommand struct {
	result TaskResult
}

type terminalNotification struct {
	publish func(TaskResult)
	result  TaskResult
	release func()
}

type cancelSessionWaiter struct {
	done   chan struct{}
	result CancelResult
}

type notificationSlots struct {
	tokens chan struct{}
}

type taskCompletion struct {
	cleanup func()
	publish func(TaskResult)
	release func()
	once    sync.Once
}

type activeTask struct {
	task       ProjectTask
	cancel     context.CancelFunc
	completion *taskCompletion
	deadline   time.Time
	decision   string
}

type schedulerLoop struct {
	clock      SchedulerClock
	maxWaiting int
	waitingTTL time.Duration
	runTimeout time.Duration

	commands          chan schedulerCommand
	done              chan struct{}
	finishEvents      chan terminalNotification
	notificationSlots *notificationSlots
	waiting           []ProjectTask
	active            *activeTask
	localMutation     bool
	stopped           bool
	stopWaiters       []chan struct{}
	cancelWaiters     []cancelSessionWaiter

	expiryTimer SchedulerTimer
	runTimer    SchedulerTimer

	completed uint64
	failed    uint64
	expired   uint64
	timedOut  uint64
	cancelled uint64
	rejected  uint64
}

func NewProjectScheduler(clock SchedulerClock, maxWaiting int, waitingTTL, runTimeout time.Duration) *ProjectScheduler {
	if clock == nil {
		clock = realSchedulerClock{}
	}
	if maxWaiting < 0 {
		maxWaiting = 0
	}
	notificationCapacity := 2 * (maxWaiting + 1)
	if notificationCapacity < 0 {
		notificationCapacity = 0
	}
	commands := make(chan schedulerCommand)
	done := make(chan struct{})
	slots := &notificationSlots{tokens: make(chan struct{}, notificationCapacity)}
	loop := &schedulerLoop{
		clock:             clock,
		maxWaiting:        maxWaiting,
		waitingTTL:        waitingTTL,
		runTimeout:        runTimeout,
		commands:          commands,
		done:              done,
		finishEvents:      make(chan terminalNotification, notificationCapacity),
		notificationSlots: slots,
	}
	go loop.publishTerminalNotifications()
	go loop.run()
	return &ProjectScheduler{commands: commands, done: done}
}

func (s *ProjectScheduler) Submit(task ProjectTask) SubmitResult {
	return s.SubmitWithOptions(task, SubmitOptions{})
}

func (s *ProjectScheduler) SubmitOnlyIfIdle(task ProjectTask) SubmitResult {
	return s.SubmitWithOptions(task, SubmitOptions{OnlyIfIdle: true})
}

func (s *ProjectScheduler) SubmitWithOptions(task ProjectTask, options SubmitOptions) SubmitResult {
	resp := make(chan SubmitResult, 1)
	select {
	case s.commands <- submitCommand{task: task, options: options, resp: resp}:
		return <-resp
	case <-s.done:
		return SubmitResult{Status: SubmitRejectedStopped}
	}
}

func (s *ProjectScheduler) TryBeginLocalMutation(ctx context.Context) (func(), bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp := make(chan localMutationLease)
	select {
	case s.commands <- beginLocalMutationCommand{ctx: ctx, resp: resp}:
	case <-s.done:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
	select {
	case lease := <-resp:
		return lease.release, lease.ok
	case <-s.done:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

func (s *ProjectScheduler) CancelSession(sessionKey string, mode CancelMode) CancelResult {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.CancelSessionContext(ctx, sessionKey, mode)
}

func (s *ProjectScheduler) CancelSessionContext(ctx context.Context, sessionKey string, mode CancelMode) CancelResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionKey == "" || !isValidCancelMode(mode) {
		return CancelResult{}
	}
	resp := make(chan CancelResult, 1)
	done := make(chan struct{})
	select {
	case s.commands <- cancelSessionCommand{sessionKey: sessionKey, mode: mode, resp: resp, done: done}:
	case <-s.done:
		return CancelResult{}
	case <-ctx.Done():
		return CancelResult{Pending: true}
	}
	var result CancelResult
	select {
	case result = <-resp:
	case <-s.done:
		return CancelResult{}
	case <-ctx.Done():
		return CancelResult{Pending: true}
	}
	if !result.Pending {
		return result
	}
	select {
	case <-done:
		result.Pending = false
		return result
	case <-s.done:
		return result
	case <-ctx.Done():
		select {
		case s.commands <- cancelSessionWaiterCancelCommand{done: done}:
		case <-s.done:
		default:
		}
		return result
	}
}

func isValidCancelMode(mode CancelMode) bool {
	switch mode {
	case CancelStop, CancelAndNew, CancelNew:
		return true
	default:
		return false
	}
}

func (s *ProjectScheduler) Snapshot() SchedulerSnapshot {
	resp := make(chan SchedulerSnapshot, 1)
	select {
	case s.commands <- snapshotCommand{resp: resp}:
		return <-resp
	case <-s.done:
		return SchedulerSnapshot{}
	}
}

func (s *ProjectScheduler) Stop(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	select {
	case s.commands <- stopCommand{ctx: ctx, done: done}:
	case <-s.done:
		return
	case <-ctx.Done():
		return
	}
	select {
	case <-done:
	case <-s.done:
	case <-ctx.Done():
	}
}

func (l *schedulerLoop) publishTerminalNotifications() {
	for event := range l.finishEvents {
		func() {
			defer event.release()
			defer func() { _ = recover() }()
			event.publish(event.result)
		}()
	}
}

func (l *schedulerLoop) run() {
	defer close(l.done)
	defer close(l.finishEvents)
	for {
		var expiryC <-chan time.Time
		var runC <-chan time.Time
		if l.expiryTimer != nil {
			expiryC = l.expiryTimer.C()
		}
		if l.runTimer != nil {
			runC = l.runTimer.C()
		}

		select {
		case cmd := <-l.commands:
			if l.handleCommand(cmd) {
				return
			}
		case <-expiryC:
			l.expiryTimer = nil
			l.dropExpiredWaiting()
			l.armExpiryTimer()
		case <-runC:
			l.runTimer = nil
			l.markActiveTimedOut()
		}
	}
}

func (l *schedulerLoop) handleCommand(cmd schedulerCommand) bool {
	switch c := cmd.(type) {
	case submitCommand:
		c.resp <- l.handleSubmitWithOptions(c.task, c.options)
	case snapshotCommand:
		c.resp <- l.snapshot()
	case beginLocalMutationCommand:
		l.handleBeginLocalMutation(c)
	case endLocalMutationCommand:
		l.localMutation = false
	case cancelSessionCommand:
		l.handleCancelSession(c)
	case cancelSessionWaiterCancelCommand:
		l.handleCancelSessionWaiterCancel(c)
	case stopCommand:
		return l.handleStop(c)
	case runFinishedCommand:
		return l.handleRunFinished(c.result)
	}
	return false
}

func (l *schedulerLoop) handleSubmitWithOptions(task ProjectTask, options SubmitOptions) SubmitResult {
	if l.stopped {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedStopped}
	}
	if l.localMutation {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedBusy}
	}
	if options.OnlyIfIdle && (l.active != nil || len(l.waiting) > 0) {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedBusy}
	}
	task = l.normalizeTask(task)
	if l.active != nil && len(l.waiting) >= l.maxWaiting {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedFull}
	}
	if !l.reserveNotificationSlot(&task) {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedNotificationBacklog}
	}
	if l.active == nil {
		l.startTask(task)
		return SubmitResult{Status: SubmitStarted}
	}
	l.waiting = append(l.waiting, task)
	l.armExpiryTimer()
	return SubmitResult{Status: SubmitQueued, Position: len(l.waiting)}
}

func (l *schedulerLoop) handleRunFinished(result TaskResult) bool {
	if l.active == nil {
		return l.shouldExitAfterStop()
	}
	active := l.active
	l.stopRunTimer()
	if active.decision != "" {
		result = TaskResult{State: active.decision, Err: result.Err}
	} else if !active.deadline.IsZero() && !l.clock.Now().Before(active.deadline) {
		result = TaskResult{State: TaskStateTimedOut, Err: result.Err}
	} else {
		result = normalizeTaskResult(result)
	}
	l.recordTerminal(result.State)
	active.completion.finish(result, l.finishEvents)
	l.active = nil
	if len(l.cancelWaiters) > 0 {
		for _, waiter := range l.cancelWaiters {
			select {
			case <-waiter.done:
			default:
				close(waiter.done)
			}
		}
		l.cancelWaiters = nil
	}
	if l.stopped {
		l.finishStopIfReady()
		return true
	}
	l.startNextEligible()
	return false
}

func (l *schedulerLoop) handleBeginLocalMutation(c beginLocalMutationCommand) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	reply := func(lease localMutationLease) bool {
		select {
		case c.resp <- lease:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if l.stopped || l.active != nil || len(l.waiting) > 0 || l.localMutation {
		reply(localMutationLease{})
		return
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	l.localMutation = true
	releaseOnce := sync.Once{}
	lease := localMutationLease{ok: true, release: func() {
		releaseOnce.Do(func() {
			select {
			case l.commands <- endLocalMutationCommand{}:
			case <-l.done:
			}
		})
	}}
	if !reply(lease) {
		l.localMutation = false
	}
}

func (l *schedulerLoop) handleCancelSession(c cancelSessionCommand) {
	if c.sessionKey == "" || !isValidCancelMode(c.mode) {
		c.resp <- CancelResult{}
		return
	}
	result := CancelResult{}
	kept := l.waiting[:0]
	for _, task := range l.waiting {
		if task.SessionKey == c.sessionKey {
			result.WaitingCancelled++
			l.recordTerminal(TaskStateCancelled)
			newTaskCompletion(task, l.notificationSlots).finish(TaskResult{State: TaskStateCancelled}, l.finishEvents)
			continue
		}
		kept = append(kept, task)
	}
	l.waiting = kept
	l.armExpiryTimer()

	if l.active != nil && l.active.task.SessionKey == c.sessionKey {
		result.ActiveCancelled = 1
		result.Pending = true
		if l.active.decision == "" {
			l.active.decision = TaskStateCancelled
		}
		l.active.cancel()
		l.cancelWaiters = append(l.cancelWaiters, cancelSessionWaiter{done: c.done, result: result})
		c.resp <- result
		return
	}
	c.resp <- result
}

func (l *schedulerLoop) handleCancelSessionWaiterCancel(c cancelSessionWaiterCancelCommand) {
	if c.done == nil || len(l.cancelWaiters) == 0 {
		return
	}
	kept := l.cancelWaiters[:0]
	for _, waiter := range l.cancelWaiters {
		if waiter.done == c.done {
			continue
		}
		kept = append(kept, waiter)
	}
	l.cancelWaiters = kept
}

func (l *schedulerLoop) handleStop(c stopCommand) bool {
	if l.stopped {
		if l.active == nil {
			close(c.done)
		} else {
			l.stopWaiters = append(l.stopWaiters, c.done)
		}
		return false
	}
	l.stopped = true
	l.stopWaiters = append(l.stopWaiters, c.done)
	l.stopExpiryTimer()
	l.stopRunTimer()
	l.cancelWaiting()
	if l.active != nil {
		if l.active.decision == "" {
			l.active.decision = TaskStateCancelled
		}
		l.active.cancel()
		return false
	}
	l.finishStopIfReady()
	return true
}

func (l *schedulerLoop) shouldExitAfterStop() bool {
	if l.stopped {
		l.finishStopIfReady()
		return true
	}
	return false
}

func (l *schedulerLoop) finishStopIfReady() {
	for _, done := range l.stopWaiters {
		close(done)
	}
	l.stopWaiters = nil
}

func (l *schedulerLoop) normalizeTask(task ProjectTask) ProjectTask {
	now := l.clock.Now()
	if task.EnqueuedAt.IsZero() {
		task.EnqueuedAt = now
	}
	if task.ExpiresAt.IsZero() {
		task.ExpiresAt = task.EnqueuedAt.Add(l.waitingTTL)
	}
	// nil Run is intentionally treated as a no-op task so scheduler-only tests and
	// future status-only tasks can exercise queue mechanics without starting Agent code.
	if task.Run == nil {
		task.Run = func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} }
	}
	if task.Cleanup == nil {
		task.Cleanup = func() {}
	}
	return task
}

func (l *schedulerLoop) reserveNotificationSlot(task *ProjectTask) bool {
	if task.OnFinish == nil {
		return true
	}
	return l.notificationSlots.reserve()
}

func (l *schedulerLoop) startNextEligible() {
	l.dropExpiredWaiting()
	if l.stopped || l.active != nil || len(l.waiting) == 0 {
		l.armExpiryTimer()
		return
	}
	next := l.waiting[0]
	copy(l.waiting, l.waiting[1:])
	l.waiting = l.waiting[:len(l.waiting)-1]
	l.startTask(next)
	l.armExpiryTimer()
}

func (l *schedulerLoop) startTask(task ProjectTask) {
	ctx, cancel := context.WithCancel(context.Background())
	completion := newTaskCompletion(task, l.notificationSlots)
	active := &activeTask{task: task, cancel: cancel, completion: completion}
	if l.runTimeout > 0 {
		active.deadline = l.clock.Now().Add(l.runTimeout)
	}
	l.active = active
	l.armRunTimer()
	go func() {
		result := task.Run(ctx)
		select {
		case l.commands <- runFinishedCommand{result: result}:
		case <-l.done:
		}
	}()
}

func (l *schedulerLoop) markActiveTimedOut() {
	if l.active == nil || l.active.decision != "" {
		return
	}
	l.active.decision = TaskStateTimedOut
	l.active.cancel()
}

func (l *schedulerLoop) dropExpiredWaiting() {
	if len(l.waiting) == 0 {
		return
	}
	now := l.clock.Now()
	kept := l.waiting[:0]
	for _, task := range l.waiting {
		if !task.ExpiresAt.IsZero() && !now.Before(task.ExpiresAt) {
			l.recordTerminal(TaskStateExpired)
			newTaskCompletion(task, l.notificationSlots).finish(TaskResult{State: TaskStateExpired}, l.finishEvents)
			continue
		}
		kept = append(kept, task)
	}
	l.waiting = kept
}

func (l *schedulerLoop) cancelWaiting() {
	for _, task := range l.waiting {
		l.recordTerminal(TaskStateCancelled)
		newTaskCompletion(task, l.notificationSlots).finish(TaskResult{State: TaskStateCancelled}, l.finishEvents)
	}
	l.waiting = nil
}

func (l *schedulerLoop) armExpiryTimer() {
	l.stopExpiryTimer()
	if len(l.waiting) == 0 {
		return
	}
	earliest := l.waiting[0].ExpiresAt
	for _, task := range l.waiting[1:] {
		if task.ExpiresAt.Before(earliest) {
			earliest = task.ExpiresAt
		}
	}
	d := earliest.Sub(l.clock.Now())
	if d < 0 {
		d = 0
	}
	l.expiryTimer = l.clock.NewTimer(d)
}

func (l *schedulerLoop) armRunTimer() {
	l.stopRunTimer()
	if l.runTimeout <= 0 {
		return
	}
	l.runTimer = l.clock.NewTimer(l.runTimeout)
}

func (l *schedulerLoop) stopExpiryTimer() {
	if l.expiryTimer != nil {
		// Timers are never Reset/reused; dropping the old timer reference after Stop
		// prevents a stale event from being associated with a later scheduler state.
		l.expiryTimer.Stop()
		l.expiryTimer = nil
	}
}

func (l *schedulerLoop) stopRunTimer() {
	if l.runTimer != nil {
		// Timers are never Reset/reused; dropping the old timer reference after Stop
		// prevents a stale event from being associated with a later scheduler state.
		l.runTimer.Stop()
		l.runTimer = nil
	}
}

func (l *schedulerLoop) snapshot() SchedulerSnapshot {
	snap := SchedulerSnapshot{
		WaitingCount:         len(l.waiting),
		NotificationPending:  l.notificationSlots.pending(),
		NotificationCapacity: l.notificationSlots.capacity(),
		Completed:            l.completed,
		Failed:               l.failed,
		Expired:              l.expired,
		TimedOut:             l.timedOut,
		Cancelled:            l.cancelled,
		Rejected:             l.rejected,
	}
	if l.active != nil {
		snap.ActiveCount = 1
		active := taskSnapshot(l.active.task)
		snap.Active = &active
	}
	if len(l.waiting) > 0 {
		snap.Waiting = make([]SchedulerTaskSnapshot, len(l.waiting))
		for i, task := range l.waiting {
			snap.Waiting[i] = taskSnapshot(task)
		}
	}
	return snap
}

func (l *schedulerLoop) recordRejected() {
	l.rejected++
}

func (l *schedulerLoop) recordTerminal(state string) {
	switch state {
	case TaskStateCompleted:
		l.completed++
	case TaskStateFailed:
		l.failed++
	case TaskStateExpired:
		l.expired++
	case TaskStateTimedOut:
		l.timedOut++
	case TaskStateCancelled:
		l.cancelled++
	}
}

func taskSnapshot(task ProjectTask) SchedulerTaskSnapshot {
	return SchedulerTaskSnapshot{
		ID:         task.ID,
		Source:     task.Source,
		SessionKey: task.SessionKey,
		EnqueuedAt: task.EnqueuedAt,
		ExpiresAt:  task.ExpiresAt,
	}
}

func newTaskCompletion(task ProjectTask, slots *notificationSlots) *taskCompletion {
	completion := &taskCompletion{
		cleanup: task.Cleanup,
		publish: task.OnFinish,
	}
	if task.OnFinish != nil {
		completion.release = slots.release
	}
	return completion
}

func (c *taskCompletion) finish(result TaskResult, events chan<- terminalNotification) {
	c.once.Do(func() {
		result = normalizeTaskResult(result)
		c.cleanup()
		if c.publish == nil {
			return
		}
		events <- terminalNotification{publish: c.publish, result: result, release: c.release}
	})
}

func normalizeTaskResult(result TaskResult) TaskResult {
	// 显式 State 优先于 Err：Run 若返回已知终态，即使 Err 非 nil 也按该终态发布；
	// 未显式给 State 时才根据 Err 归一为 completed/cancelled/failed。
	switch result.State {
	case TaskStateCompleted, TaskStateExpired, TaskStateTimedOut, TaskStateCancelled, TaskStateFailed:
		return result
	}
	if result.Err == nil {
		result.State = TaskStateCompleted
		return result
	}
	if errors.Is(result.Err, context.Canceled) {
		result.State = TaskStateCancelled
		return result
	}
	result.State = TaskStateFailed
	return result
}

func (s *notificationSlots) reserve() bool {
	select {
	case s.tokens <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *notificationSlots) release() {
	select {
	case <-s.tokens:
	default:
	}
}

func (s *notificationSlots) pending() int { return len(s.tokens) }

func (s *notificationSlots) capacity() int { return cap(s.tokens) }
