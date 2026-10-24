package core

import "time"

// SchedulerTimer 是 ProjectScheduler 使用的最小计时器接口，便于真实时钟与测试假时钟共用。
type SchedulerTimer interface {
	C() <-chan time.Time
	Stop() bool
}

// SchedulerClock 抽象当前时间和一次性 Timer，测试中必须可控推进。
type SchedulerClock interface {
	Now() time.Time
	NewTimer(time.Duration) SchedulerTimer
}

type realSchedulerClock struct{}

func (realSchedulerClock) Now() time.Time { return time.Now() }

func (realSchedulerClock) NewTimer(d time.Duration) SchedulerTimer {
	return realSchedulerTimer{timer: time.NewTimer(d)}
}

type realSchedulerTimer struct {
	timer *time.Timer
}

func (t realSchedulerTimer) C() <-chan time.Time { return t.timer.C }

func (t realSchedulerTimer) Stop() bool { return t.timer.Stop() }
