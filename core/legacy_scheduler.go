package core

import (
	"context"
	"sort"
	"sync"
	"time"
)

// LegacyScheduler 按 SessionKey 划分串行 lane：同一会话 FIFO，不同会话可并行。
// maxWaiting 是每个 lane 的 waiting 上限，不包含正在运行或正在清理的任务。
type LegacyScheduler struct {
	mu sync.Mutex

	maxWaiting int
	lanes      map[string]*legacyLane
	stopped    bool
	mutation   bool

	notifier             *terminalNotifier
	notificationCapacity int
	settled              chan struct{}
	settleOnce           sync.Once

	activeCount     int
	waitingCount    int
	completionCount int
	completed       uint64
	failed          uint64
	expired         uint64
	timedOut        uint64
	cancelled       uint64
	rejected        uint64
}

type legacyLane struct {
	active            *legacyActiveTask
	waiting           []ProjectTask
	completing        int
	completionDone    chan struct{}
	notificationSlots *notificationSlots
}

type legacyActivePhase uint8

const (
	legacyActiveRunning legacyActivePhase = iota
	legacyActiveCompleting
)

type legacyActiveTask struct {
	task       ProjectTask
	cancel     context.CancelFunc
	completion *taskCompletion
	decision   string
	phase      legacyActivePhase
	done       chan struct{}
}

type legacyDetachedCompletion struct {
	sessionKey string
	completion *taskCompletion
	result     TaskResult
}

func NewLegacyScheduler(maxWaiting int) *LegacyScheduler {
	if maxWaiting < 0 {
		maxWaiting = 0
	}
	notificationCapacity := schedulerNotificationCapacity(maxWaiting)
	notifier := newTerminalNotifier()
	s := &LegacyScheduler{
		maxWaiting:           maxWaiting,
		lanes:                make(map[string]*legacyLane),
		notifier:             notifier,
		notificationCapacity: notificationCapacity,
		settled:              make(chan struct{}),
	}
	go notifier.run()
	return s
}

func (s *LegacyScheduler) Submit(task ProjectTask) SubmitResult {
	return s.SubmitWithOptions(task, SubmitOptions{})
}

// SubmitOnlyIfIdle 当前采用全 Scheduler 空闲的保守门禁，避免主动任务绕过
// 已接受的普通消息。各主动入口的 beta.3 兼容语义由后续任务逐项接线和验收。
func (s *LegacyScheduler) SubmitOnlyIfIdle(task ProjectTask) SubmitResult {
	return s.SubmitWithOptions(task, SubmitOptions{OnlyIfIdle: true})
}

func (s *LegacyScheduler) SubmitWithOptions(task ProjectTask, options SubmitOptions) SubmitResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		s.rejected++
		return SubmitResult{Status: SubmitRejectedStopped}
	}
	if s.mutation {
		s.rejected++
		return SubmitResult{Status: SubmitRejectedBusy}
	}
	if options.OnlyIfIdle && (s.activeCount != 0 || s.waitingCount != 0 || s.completionCount != 0) {
		s.rejected++
		return SubmitResult{Status: SubmitRejectedBusy}
	}

	task = normalizeLegacyTask(task)
	lane := s.lanes[task.SessionKey]
	laneOwned := lane != nil && (lane.active != nil || lane.completing != 0)
	if laneOwned && len(lane.waiting) >= s.maxWaiting {
		s.rejected++
		return SubmitResult{
			Status:     SubmitRejectedFull,
			Feedback:   SubmitFeedbackQueueFull,
			QueueDepth: len(lane.waiting),
		}
	}
	if lane == nil {
		lane = s.newLaneLocked(task.SessionKey)
	}
	if task.OnFinish != nil && !lane.notificationSlots.reserve() {
		s.deleteLaneIfIdleLocked(task.SessionKey, lane)
		s.rejected++
		return SubmitResult{Status: SubmitRejectedNotificationBacklog}
	}
	if lane.active == nil && lane.completing == 0 {
		s.startTaskLocked(lane, task)
		return SubmitResult{Status: SubmitStarted}
	}
	lane.waiting = append(lane.waiting, task)
	s.waitingCount++
	return SubmitResult{
		Status:   SubmitQueued,
		Position: len(lane.waiting),
		Feedback: SubmitFeedbackQueued,
	}
}

// TryBeginLocalMutation 仅在所有 lane 空闲时授予全局 lease。这是 fail-closed
// 的临时接口实现，不代表命令、卡片或 compact 已完成 Legacy 产品语义验收。
func (s *LegacyScheduler) TryBeginLocalMutation(ctx context.Context) (func(), bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, false
	default:
	}

	return s.tryBeginLocalMutationAfterPrecheck(ctx)
}

// tryBeginLocalMutationAfterPrecheck 的调用方已完成锁前 context 检查；这里在获取
// Scheduler 锁后再次检查，确保等待锁期间过期的调用不能获得 mutation lease。
func (s *LegacyScheduler) tryBeginLocalMutationAfterPrecheck(ctx context.Context) (func(), bool) {
	s.mu.Lock()
	select {
	case <-ctx.Done():
		s.mu.Unlock()
		return nil, false
	default:
	}
	if s.stopped || s.mutation || s.activeCount != 0 || s.waitingCount != 0 || s.completionCount != 0 {
		s.mu.Unlock()
		return nil, false
	}
	s.mutation = true
	s.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			s.mutation = false
			s.mu.Unlock()
		})
	}, true
}

func (s *LegacyScheduler) Snapshot() SchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	notificationPending := 0
	for _, lane := range s.lanes {
		notificationPending += lane.notificationSlots.pending()
	}
	snapshot := SchedulerSnapshot{
		ActiveCount:         s.activeCount,
		WaitingCount:        s.waitingCount,
		NotificationPending: notificationPending,
		// Legacy 的通知额度按 SessionKey lane 独立计算；该字段表达每 lane 上限，
		// 不是所有 lane 的动态全局总上限。
		NotificationCapacity: s.notificationCapacity,
		Completed:            s.completed,
		Failed:               s.failed,
		Expired:              s.expired,
		TimedOut:             s.timedOut,
		Cancelled:            s.cancelled,
		Rejected:             s.rejected,
	}

	keys := make([]string, 0, len(s.lanes))
	for key := range s.lanes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		lane := s.lanes[key]
		if lane.active != nil {
			active := taskSnapshot(lane.active.task)
			snapshot.ActiveTasks = append(snapshot.ActiveTasks, active)
			if snapshot.Active == nil {
				activeCopy := active
				snapshot.Active = &activeCopy
			}
		}
		for _, task := range lane.waiting {
			snapshot.Waiting = append(snapshot.Waiting, taskSnapshot(task))
		}
	}
	return snapshot
}

func (s *LegacyScheduler) CancelSessionContext(ctx context.Context, sessionKey string, mode CancelMode) CancelResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if sessionKey == "" || !isValidCancelMode(mode) {
		return CancelResult{}
	}

	s.mu.Lock()
	lane := s.lanes[sessionKey]
	if lane == nil {
		s.mu.Unlock()
		return CancelResult{}
	}
	result := CancelResult{WaitingCancelled: len(lane.waiting)}
	completions := s.detachWaitingLocked(sessionKey, lane, TaskStateCancelled)

	var activeDone <-chan struct{}
	if lane.active != nil {
		activeDone = lane.active.done
		if lane.active.phase == legacyActiveRunning {
			result.ActiveCancelled = 1
			if lane.active.decision == "" {
				lane.active.decision = TaskStateCancelled
			}
			lane.active.cancel()
		}
	}
	var completionDone <-chan struct{}
	if lane.completing != 0 {
		completionDone = lane.completionDone
	}
	result.Pending = activeDone != nil || completionDone != nil
	s.deleteLaneIfIdleLocked(sessionKey, lane)
	s.mu.Unlock()

	s.runDetachedCompletions(completions)
	if !waitLegacyCompletionBarrier(ctx, activeDone) || !waitLegacyCompletionBarrier(ctx, completionDone) {
		return result
	}
	result.Pending = false
	return result
}

func waitLegacyCompletionBarrier(ctx context.Context, done <-chan struct{}) bool {
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}

func (s *LegacyScheduler) Stop(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	var completions []legacyDetachedCompletion
	if !s.stopped {
		s.stopped = true
		for key, lane := range s.lanes {
			completions = append(completions, s.detachWaitingLocked(key, lane, TaskStateCancelled)...)
			if lane.active != nil && lane.active.phase == legacyActiveRunning {
				if lane.active.decision == "" {
					lane.active.decision = TaskStateCancelled
				}
				lane.active.cancel()
			}
			s.deleteLaneIfIdleLocked(key, lane)
		}
		s.settleIfReadyLocked()
	}
	settled := s.settled
	s.mu.Unlock()

	s.runDetachedCompletions(completions)
	select {
	case <-settled:
	case <-ctx.Done():
	}
}

func (s *LegacyScheduler) newLaneLocked(sessionKey string) *legacyLane {
	lane := &legacyLane{
		notificationSlots: newNotificationSlots(s.notificationCapacity),
	}
	s.lanes[sessionKey] = lane
	return lane
}

func (s *LegacyScheduler) newTaskCompletion(task ProjectTask, lane *legacyLane) *taskCompletion {
	completion := &taskCompletion{
		cleanup:  task.Cleanup,
		publish:  task.OnFinish,
		notifier: s.notifier,
	}
	if task.OnFinish != nil {
		sessionKey := task.SessionKey
		completion.release = func() {
			lane.notificationSlots.release()
			s.notificationReleased(sessionKey, lane)
		}
	}
	return completion
}

func (s *LegacyScheduler) notificationReleased(sessionKey string, lane *legacyLane) {
	s.mu.Lock()
	s.deleteLaneIfIdleLocked(sessionKey, lane)
	s.mu.Unlock()
}

func (s *LegacyScheduler) startTaskLocked(lane *legacyLane, task ProjectTask) {
	ctx, cancel := context.WithCancel(context.Background())
	active := &legacyActiveTask{
		task:       task,
		cancel:     cancel,
		completion: s.newTaskCompletion(task, lane),
		done:       make(chan struct{}),
	}
	lane.active = active
	s.activeCount++
	go func() {
		result := task.Run(ctx)
		s.finishTask(task.SessionKey, active, result)
	}()
}

func (s *LegacyScheduler) finishTask(sessionKey string, active *legacyActiveTask, result TaskResult) {
	s.mu.Lock()
	lane := s.lanes[sessionKey]
	if lane == nil || lane.active != active || active.phase != legacyActiveRunning {
		s.mu.Unlock()
		return
	}
	if active.decision != "" {
		result = TaskResult{State: active.decision, Err: result.Err}
	} else {
		result = normalizeTaskResult(result)
	}
	active.decision = result.State
	active.phase = legacyActiveCompleting
	s.recordTerminalLocked(result.State)
	s.mu.Unlock()

	active.completion.finish(result)

	s.mu.Lock()
	lane = s.lanes[sessionKey]
	if lane == nil || lane.active != active || active.phase != legacyActiveCompleting {
		s.mu.Unlock()
		return
	}
	lane.active = nil
	s.activeCount--
	close(active.done)
	s.advanceLaneLocked(sessionKey, lane)
	s.settleIfReadyLocked()
	s.mu.Unlock()
}

func (s *LegacyScheduler) detachWaitingLocked(sessionKey string, lane *legacyLane, state string) []legacyDetachedCompletion {
	if len(lane.waiting) == 0 {
		return nil
	}
	completions := make([]legacyDetachedCompletion, 0, len(lane.waiting))
	for _, task := range lane.waiting {
		s.recordTerminalLocked(state)
		completions = append(completions, legacyDetachedCompletion{
			sessionKey: sessionKey,
			completion: s.newTaskCompletion(task, lane),
			result:     TaskResult{State: state},
		})
	}
	count := len(lane.waiting)
	s.waitingCount -= count
	lane.waiting = nil
	if lane.completing == 0 {
		lane.completionDone = make(chan struct{})
	}
	lane.completing += count
	s.completionCount += count
	return completions
}

func (s *LegacyScheduler) runDetachedCompletions(completions []legacyDetachedCompletion) {
	for _, completion := range completions {
		completion := completion
		go func() {
			defer s.finishDetachedCompletion(completion.sessionKey)
			completion.completion.finish(completion.result)
		}()
	}
}

func (s *LegacyScheduler) finishDetachedCompletion(sessionKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lane := s.lanes[sessionKey]
	if lane == nil || lane.completing == 0 {
		return
	}
	lane.completing--
	s.completionCount--
	if lane.completing == 0 {
		close(lane.completionDone)
		lane.completionDone = nil
	}
	s.advanceLaneLocked(sessionKey, lane)
	s.settleIfReadyLocked()
}

func (s *LegacyScheduler) advanceLaneLocked(sessionKey string, lane *legacyLane) {
	if !s.stopped && lane.active == nil && lane.completing == 0 && len(lane.waiting) > 0 {
		next := lane.waiting[0]
		copy(lane.waiting, lane.waiting[1:])
		lane.waiting = lane.waiting[:len(lane.waiting)-1]
		s.waitingCount--
		s.startTaskLocked(lane, next)
		return
	}
	s.deleteLaneIfIdleLocked(sessionKey, lane)
}

func (s *LegacyScheduler) deleteLaneIfIdleLocked(sessionKey string, lane *legacyLane) {
	// notification release 在归还 lane slot 后才获取 Scheduler 锁；这段窗口内旧 lane
	// 可能已被回收并由同 key 的新 lane 替代，因此删除前必须校验当前 map identity。
	if s.lanes[sessionKey] == lane && lane.active == nil && lane.completing == 0 &&
		len(lane.waiting) == 0 && lane.notificationSlots.pending() == 0 {
		delete(s.lanes, sessionKey)
	}
}

func (s *LegacyScheduler) settleIfReadyLocked() {
	if !s.stopped || s.activeCount != 0 || s.completionCount != 0 {
		return
	}
	s.settleOnce.Do(func() {
		s.notifier.close()
		close(s.settled)
	})
}

func (s *LegacyScheduler) recordTerminalLocked(state string) {
	switch state {
	case TaskStateCompleted:
		s.completed++
	case TaskStateFailed:
		s.failed++
	case TaskStateExpired:
		s.expired++
	case TaskStateTimedOut:
		s.timedOut++
	case TaskStateCancelled:
		s.cancelled++
	}
}

func normalizeLegacyTask(task ProjectTask) ProjectTask {
	if task.EnqueuedAt.IsZero() {
		task.EnqueuedAt = time.Now()
	}
	if task.Run == nil {
		task.Run = func(context.Context) TaskResult { return TaskResult{State: TaskStateCompleted} }
	}
	if task.Cleanup == nil {
		task.Cleanup = func() {}
	}
	return task
}
