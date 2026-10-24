package core

import (
	"context"
	"time"
)

type AgentTriggerSource string

const (
	TaskSourceInteractive AgentTriggerSource = "interactive"
	TaskSourceTimer       AgentTriggerSource = "timer"
	TaskSourceHeartbeat   AgentTriggerSource = "heartbeat"
	TaskSourceRelay       AgentTriggerSource = "relay"
	TaskSourceCron        AgentTriggerSource = "cron"
)

type CancelMode string

const (
	CancelStop   CancelMode = "stop"
	CancelAndNew CancelMode = "cancel_and_new"
	CancelNew    CancelMode = "new"
)

type CancelResult struct {
	ActiveCancelled  int
	WaitingCancelled int
	Pending          bool
}

const (
	TaskStateCompleted = "completed"
	TaskStateExpired   = "expired"
	TaskStateTimedOut  = "timed_out"
	TaskStateCancelled = "cancelled"
	TaskStateFailed    = "failed"
)

// TaskResult 记录一次调度任务的固定终态。
type TaskResult struct {
	State  string
	Reason string
	Err    error
}

// ProjectTask 表示一次需要占用项目级 Agent 单槽的工作。
//
// Cleanup 由 Scheduler 在已接受任务进入 completed、expired、timed_out、
// cancelled 或 failed 终态后恰好调用一次；Submit 返回 rejected_* 时任务未被
// Scheduler 接管，调用方仍持有附件等临时资源并负责自行清理。Cleanup 不返回
// error；调用方若适配带 error 的资源 Cleanup，必须在闭包内记录固定 reason/task_id
// 的安全日志，不能把 path、文件名或底层错误原样外泄。OnFinish 是可选的
// 终态发布钩子，必须只依赖 TaskResult，不承担附件释放职责。带非 nil OnFinish
// 的任务在 Submit 时预留有界通知槽；若通知 backlog 已满，会返回
// SubmitRejectedNotificationBacklog，且同样不接管资源。
type ProjectTask struct {
	ID         string
	Source     AgentTriggerSource
	SessionKey string
	Platform   Platform
	ReplyCtx   any
	EnqueuedAt time.Time
	ExpiresAt  time.Time
	Run        func(context.Context) TaskResult
	Cleanup    func()
	OnFinish   func(TaskResult)
}

type SubmitStatus string

const (
	SubmitStarted                     SubmitStatus = "started"
	SubmitQueued                      SubmitStatus = "queued"
	SubmitRejectedFull                SubmitStatus = "rejected_full"
	SubmitRejectedBusy                SubmitStatus = "rejected_busy"
	SubmitRejectedStopped             SubmitStatus = "rejected_stopped"
	SubmitRejectedNotificationBacklog SubmitStatus = "rejected_notification_backlog"
)

type SubmitOptions struct {
	OnlyIfIdle bool
}

type SubmitResult struct {
	Status   SubmitStatus
	Position int
}

type SchedulerTaskSnapshot struct {
	ID         string
	Source     AgentTriggerSource
	SessionKey string
	EnqueuedAt time.Time
	ExpiresAt  time.Time
}

type SchedulerSnapshot struct {
	ActiveCount          int
	WaitingCount         int
	NotificationPending  int
	NotificationCapacity int
	Completed            uint64
	Failed               uint64
	Expired              uint64
	TimedOut             uint64
	Cancelled            uint64
	Rejected             uint64
	Active               *SchedulerTaskSnapshot
	Waiting              []SchedulerTaskSnapshot
}
