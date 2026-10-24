package core

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type ProjectScheduler struct {
	commands chan schedulerCommand
	done     chan struct{}
}

type schedulerCommand interface{}

type submitCommand struct {
	task      ProjectTask
	admission submitAdmission
	resp      chan SubmitResult
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

type taskCompletionFinishedCommand struct {
	active *activeTask
}

type detachedTaskCompletionFinishedCommand struct {
	sessionKey string
	generation *projectCompletionGeneration
}

type terminalNotification struct {
	publish func(TaskResult)
	result  TaskResult
	release func()
}

type terminalNotifier struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []terminalNotification
	closed bool
}

type cancelSessionWaiter struct {
	done        chan struct{}
	waitActive  *activeTask
	generations []*projectCompletionGeneration
}

type projectCompletionGeneration struct {
	remaining int
}

type projectDetachedCompletion struct {
	sessionKey string
	completion *taskCompletion
	result     TaskResult
	generation *projectCompletionGeneration
}

type notificationSlots struct {
	mu              sync.Mutex
	logicalCapacity int
	pendingCount    int
}

type taskCompletion struct {
	cleanup  func()
	publish  func(TaskResult)
	release  func()
	notifier *terminalNotifier
	once     sync.Once
}

type activeTask struct {
	task       ProjectTask
	cancel     context.CancelFunc
	completion *taskCompletion
	deadline   time.Time
	decision   string
	completing bool
}

type schedulerLoop struct {
	clock      SchedulerClock
	maxWaiting int
	waitingTTL time.Duration
	runTimeout time.Duration

	commands              chan schedulerCommand
	done                  chan struct{}
	notifier              *terminalNotifier
	notificationSlots     *notificationSlots
	waiting               []ProjectTask
	active                *activeTask
	localMutation         bool
	stopped               bool
	stopWaiters           []chan struct{}
	cancelWaiters         []cancelSessionWaiter
	completionCount       int
	completionGenerations map[string][]*projectCompletionGeneration
	detachedCompletions   []projectDetachedCompletion
	completionRunning     bool

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
	notificationCapacity := schedulerNotificationCapacity(maxWaiting)
	commands := make(chan schedulerCommand)
	done := make(chan struct{})
	slots := newNotificationSlots(notificationCapacity)
	notifier := newTerminalNotifier()
	loop := &schedulerLoop{
		clock:             clock,
		maxWaiting:        maxWaiting,
		waitingTTL:        waitingTTL,
		runTimeout:        runTimeout,
		commands:          commands,
		done:              done,
		notifier:          notifier,
		notificationSlots: slots,
	}
	go notifier.run()
	go loop.run()
	return &ProjectScheduler{commands: commands, done: done}
}

func (s *ProjectScheduler) Submit(task ProjectTask) SubmitResult {
	return s.submit(task, admissionInteractiveQueue)
}

// SubmitProactive 在 Project 调度下与普通交互任务共享全局 FIFO。
func (s *ProjectScheduler) SubmitProactive(task ProjectTask) SubmitResult {
	return s.submit(task, admissionProactive)
}

func (s *ProjectScheduler) SubmitOnlyIfIdle(task ProjectTask) SubmitResult {
	return s.submit(task, admissionOnlyIfIdle)
}

func (s *ProjectScheduler) SubmitWithOptions(task ProjectTask, options SubmitOptions) SubmitResult {
	if options.OnlyIfIdle {
		return s.SubmitOnlyIfIdle(task)
	}
	return s.Submit(task)
}

func (s *ProjectScheduler) submit(task ProjectTask, admission submitAdmission) SubmitResult {
	resp := make(chan SubmitResult, 1)
	select {
	case s.commands <- submitCommand{task: task, admission: admission, resp: resp}:
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

func (l *schedulerLoop) run() {
	defer close(l.done)
	defer l.notifier.close()
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
			l.runDetachedCompletions(l.dropExpiredWaiting())
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
		c.resp <- l.handleSubmit(c.task, c.admission)
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
		l.handleRunFinished(c.result)
	case taskCompletionFinishedCommand:
		return l.handleTaskCompletionFinished(c.active)
	case detachedTaskCompletionFinishedCommand:
		return l.handleDetachedTaskCompletionFinished(c)
	}
	return false
}

func (l *schedulerLoop) handleSubmit(task ProjectTask, admission submitAdmission) SubmitResult {
	if l.stopped {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedStopped}
	}
	if l.localMutation {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedBusy}
	}
	busy := l.active != nil || l.completionCount != 0 || len(l.waiting) > 0
	if admission == admissionOnlyIfIdle && busy {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedBusy}
	}
	task = l.normalizeTask(task)
	if busy && len(l.waiting) >= l.maxWaiting {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedFull}
	}
	if !l.reserveNotificationSlot(&task) {
		l.recordRejected()
		return SubmitResult{Status: SubmitRejectedNotificationBacklog}
	}
	if !busy {
		l.startTask(task)
		return SubmitResult{Status: SubmitStarted}
	}
	l.waiting = append(l.waiting, task)
	l.armExpiryTimer()
	return SubmitResult{Status: SubmitQueued, Position: len(l.waiting)}
}

func (l *schedulerLoop) handleRunFinished(result TaskResult) {
	if l.active == nil {
		return
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
	active.completing = true
	go func() {
		active.completion.finish(result)
		select {
		case l.commands <- taskCompletionFinishedCommand{active: active}:
		case <-l.done:
		}
	}()
}

func (l *schedulerLoop) handleTaskCompletionFinished(active *activeTask) bool {
	if l.active != active {
		return false
	}
	l.active = nil
	l.releaseReadyCancelWaiters()
	if l.stopped {
		return l.finishStopIfReady()
	}
	l.startNextEligible()
	return false
}

func (l *schedulerLoop) handleDetachedTaskCompletionFinished(c detachedTaskCompletionFinishedCommand) bool {
	generation := c.generation
	if generation == nil || generation.remaining == 0 || !l.completionRunning {
		return false
	}
	l.completionRunning = false
	generation.remaining--
	l.completionCount--
	if generation.remaining == 0 {
		generations := l.completionGenerations[c.sessionKey]
		for i, existing := range generations {
			if existing != generation {
				continue
			}
			generations = append(generations[:i], generations[i+1:]...)
			break
		}
		if len(generations) == 0 {
			delete(l.completionGenerations, c.sessionKey)
		} else {
			l.completionGenerations[c.sessionKey] = generations
		}
	}
	l.releaseReadyCancelWaiters()
	l.runDetachedCompletions(nil)
	if l.stopped {
		return l.finishStopIfReady()
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
	if l.stopped || l.active != nil || l.completionCount != 0 || len(l.waiting) > 0 || l.localMutation {
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
	cancelled := make([]ProjectTask, 0)
	for _, task := range l.waiting {
		if task.SessionKey == c.sessionKey {
			result.WaitingCancelled++
			cancelled = append(cancelled, task)
			continue
		}
		kept = append(kept, task)
	}
	l.waiting = kept
	completions := l.detachCompletions(cancelled, TaskStateCancelled)
	l.armExpiryTimer()

	waiter := cancelSessionWaiter{done: c.done}
	if l.active != nil && l.active.task.SessionKey == c.sessionKey {
		waiter.waitActive = l.active
		if !l.active.completing {
			result.ActiveCancelled = 1
			if l.active.decision == "" {
				l.active.decision = TaskStateCancelled
			}
			l.active.cancel()
		}
	}
	waiter.generations = append(waiter.generations, l.completionGenerations[c.sessionKey]...)
	result.Pending = waiter.waitActive != nil || len(waiter.generations) != 0
	if result.Pending {
		l.cancelWaiters = append(l.cancelWaiters, waiter)
	}
	c.resp <- result
	l.runDetachedCompletions(completions)
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

func (l *schedulerLoop) releaseReadyCancelWaiters() {
	if len(l.cancelWaiters) == 0 {
		return
	}
	kept := l.cancelWaiters[:0]
	for _, waiter := range l.cancelWaiters {
		if waiter.waitActive != nil && l.active == waiter.waitActive {
			kept = append(kept, waiter)
			continue
		}
		pending := false
		for _, generation := range waiter.generations {
			if generation.remaining != 0 {
				pending = true
				break
			}
		}
		if pending {
			kept = append(kept, waiter)
			continue
		}
		select {
		case <-waiter.done:
		default:
			close(waiter.done)
		}
	}
	l.cancelWaiters = kept
}

func (l *schedulerLoop) handleStop(c stopCommand) bool {
	if l.stopped {
		if l.active == nil && l.completionCount == 0 {
			close(c.done)
			return true
		}
		l.stopWaiters = append(l.stopWaiters, c.done)
		return false
	}
	l.stopped = true
	l.stopWaiters = append(l.stopWaiters, c.done)
	l.stopExpiryTimer()
	l.stopRunTimer()
	completions := l.cancelWaiting()
	if l.active != nil && !l.active.completing {
		if l.active.decision == "" {
			l.active.decision = TaskStateCancelled
		}
		l.active.cancel()
	}
	l.runDetachedCompletions(completions)
	return l.finishStopIfReady()
}

func (l *schedulerLoop) finishStopIfReady() bool {
	if !l.stopped || l.active != nil || l.completionCount != 0 {
		return false
	}
	for _, done := range l.stopWaiters {
		close(done)
	}
	l.stopWaiters = nil
	return true
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
	l.runDetachedCompletions(l.dropExpiredWaiting())
	if l.stopped || l.active != nil || l.completionCount != 0 || len(l.waiting) == 0 {
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
	completion := newTaskCompletion(task, l.notificationSlots, l.notifier)
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
	if l.active == nil || l.active.completing || l.active.decision != "" {
		return
	}
	l.active.decision = TaskStateTimedOut
	l.active.cancel()
}

func (l *schedulerLoop) dropExpiredWaiting() []projectDetachedCompletion {
	if len(l.waiting) == 0 {
		return nil
	}
	now := l.clock.Now()
	kept := l.waiting[:0]
	expired := make([]ProjectTask, 0)
	for _, task := range l.waiting {
		if !task.ExpiresAt.IsZero() && !now.Before(task.ExpiresAt) {
			expired = append(expired, task)
			continue
		}
		kept = append(kept, task)
	}
	l.waiting = kept
	return l.detachCompletions(expired, TaskStateExpired)
}

func (l *schedulerLoop) cancelWaiting() []projectDetachedCompletion {
	waiting := l.waiting
	l.waiting = nil
	return l.detachCompletions(waiting, TaskStateCancelled)
}

func (l *schedulerLoop) detachCompletions(tasks []ProjectTask, state string) []projectDetachedCompletion {
	if len(tasks) == 0 {
		return nil
	}
	if l.completionGenerations == nil {
		l.completionGenerations = make(map[string][]*projectCompletionGeneration)
	}
	counts := make(map[string]int)
	for _, task := range tasks {
		counts[task.SessionKey]++
	}
	generations := make(map[string]*projectCompletionGeneration, len(counts))
	for sessionKey, count := range counts {
		generation := &projectCompletionGeneration{remaining: count}
		generations[sessionKey] = generation
		l.completionGenerations[sessionKey] = append(l.completionGenerations[sessionKey], generation)
	}
	l.completionCount += len(tasks)
	completions := make([]projectDetachedCompletion, 0, len(tasks))
	for _, task := range tasks {
		l.recordTerminal(state)
		completions = append(completions, projectDetachedCompletion{
			sessionKey: task.SessionKey,
			completion: newTaskCompletion(task, l.notificationSlots, l.notifier),
			result:     TaskResult{State: state},
			generation: generations[task.SessionKey],
		})
	}
	return completions
}

func (l *schedulerLoop) runDetachedCompletions(completions []projectDetachedCompletion) {
	l.detachedCompletions = append(l.detachedCompletions, completions...)
	if l.completionRunning || len(l.detachedCompletions) == 0 {
		return
	}
	completion := l.detachedCompletions[0]
	l.detachedCompletions[0] = projectDetachedCompletion{}
	l.detachedCompletions = l.detachedCompletions[1:]
	l.completionRunning = true
	go func() {
		completion.completion.finish(completion.result)
		l.commands <- detachedTaskCompletionFinishedCommand{
			sessionKey: completion.sessionKey,
			generation: completion.generation,
		}
	}()
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
		snap.ActiveTasks = []SchedulerTaskSnapshot{active}
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

func newTaskCompletion(task ProjectTask, slots *notificationSlots, notifier *terminalNotifier) *taskCompletion {
	completion := &taskCompletion{
		cleanup:  task.Cleanup,
		publish:  task.OnFinish,
		notifier: notifier,
	}
	if task.OnFinish != nil {
		completion.release = slots.release
	}
	return completion
}

func (c *taskCompletion) finish(result TaskResult) {
	c.once.Do(func() {
		result = normalizeTaskResult(result)
		func() {
			defer func() {
				if recover() != nil {
					slog.Error("scheduler task cleanup panic", "component", "scheduler")
				}
			}()
			c.cleanup()
		}()
		if c.publish == nil {
			return
		}
		c.notifier.enqueue(terminalNotification{publish: c.publish, result: result, release: c.release})
	})
}

func newTerminalNotifier() *terminalNotifier {
	n := &terminalNotifier{}
	n.cond = sync.NewCond(&n.mu)
	return n
}

func (n *terminalNotifier) enqueue(event terminalNotification) {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		event.release()
		slog.Error("scheduler terminal notification rejected after close", "component", "scheduler")
		return
	}
	n.queue = append(n.queue, event)
	n.cond.Signal()
	n.mu.Unlock()
}

func (n *terminalNotifier) close() {
	n.mu.Lock()
	if !n.closed {
		n.closed = true
		n.cond.Broadcast()
	}
	n.mu.Unlock()
}

func (n *terminalNotifier) run() {
	for {
		n.mu.Lock()
		for len(n.queue) == 0 && !n.closed {
			n.cond.Wait()
		}
		if len(n.queue) == 0 {
			n.mu.Unlock()
			return
		}
		event := n.queue[0]
		n.queue[0] = terminalNotification{}
		if len(n.queue) == 1 {
			n.queue = nil
		} else {
			n.queue = n.queue[1:]
		}
		n.mu.Unlock()

		func() {
			defer event.release()
			defer func() {
				if recover() != nil {
					slog.Error("scheduler terminal notification panic", "component", "scheduler")
				}
			}()
			event.publish(event.result)
		}()
	}
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

func schedulerNotificationCapacity(maxWaiting int) int {
	const notificationsPerAcceptedTask = 2
	if maxWaiting < 0 || maxWaiting > int(^uint(0)>>1)/notificationsPerAcceptedTask-1 {
		return 0
	}
	return notificationsPerAcceptedTask * (maxWaiting + 1)
}

func newNotificationSlots(capacity int) *notificationSlots {
	return &notificationSlots{logicalCapacity: capacity}
}

func (s *notificationSlots) reserve() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingCount >= s.logicalCapacity {
		return false
	}
	s.pendingCount++
	return true
}

func (s *notificationSlots) release() {
	s.mu.Lock()
	if s.pendingCount > 0 {
		s.pendingCount--
	}
	s.mu.Unlock()
}

func (s *notificationSlots) pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingCount
}

func (s *notificationSlots) capacity() int {
	return s.logicalCapacity
}
