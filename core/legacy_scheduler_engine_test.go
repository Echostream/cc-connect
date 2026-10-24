package core

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func newLegacyEngineSchedulerUnderTest() EngineScheduler {
	return NewLegacyScheduler(2)
}

type legacyEngineAgent struct {
	mu         sync.Mutex
	currentKey string
	workDir    string
	sessions   map[string]*legacyEngineSession
}

func newLegacyEngineAgent() *legacyEngineAgent {
	return &legacyEngineAgent{sessions: make(map[string]*legacyEngineSession)}
}

func (a *legacyEngineAgent) Name() string       { return "legacy-engine" }
func (a *legacyEngineAgent) GetWorkDir() string { return a.workDir }

func (a *legacyEngineAgent) SetSessionEnv(env []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, entry := range env {
		if strings.HasPrefix(entry, "CC_SESSION_KEY=") {
			a.currentKey = strings.TrimPrefix(entry, "CC_SESSION_KEY=")
			return
		}
	}
}

func (a *legacyEngineAgent) StartSession(context.Context, string) (AgentSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	session := newLegacyEngineSession(a.currentKey)
	a.sessions[a.currentKey] = session
	return session, nil
}

func (a *legacyEngineAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}

func (a *legacyEngineAgent) Stop() error { return nil }

func (a *legacyEngineAgent) session(key string) *legacyEngineSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sessions[key]
}

type legacyEngineSession struct {
	key      string
	events   chan Event
	sent     chan legacyEngineSend
	releases chan struct{}
	closed   chan struct{}
	close    sync.Once

	closeGateMu   sync.Mutex
	closeStarted  chan struct{}
	releaseClose  chan struct{}
	closeStartOne sync.Once
}

type legacyEngineSend struct {
	prompt string
	files  []FileAttachment
}

func newLegacyEngineSession(key string) *legacyEngineSession {
	return &legacyEngineSession{
		key:      key,
		events:   make(chan Event, 4),
		sent:     make(chan legacyEngineSend, 4),
		releases: make(chan struct{}, 4),
		closed:   make(chan struct{}),
	}
}

func (s *legacyEngineSession) Send(prompt string, _ string, _ []ImageAttachment, files []FileAttachment) error {
	record := legacyEngineSend{prompt: prompt, files: make([]FileAttachment, len(files))}
	for i := range files {
		record.files[i] = files[i]
		record.files[i].Data = append([]byte(nil), files[i].Data...)
	}
	s.sent <- record
	select {
	case <-s.releases:
		s.events <- Event{Type: EventResult, Content: "done:" + s.key, Done: true}
		return nil
	case <-s.closed:
		return context.Canceled
	}
}

func (s *legacyEngineSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *legacyEngineSession) Events() <-chan Event                             { return s.events }
func (s *legacyEngineSession) CurrentSessionID() string                         { return "session:" + s.key }
func (s *legacyEngineSession) Alive() bool {
	select {
	case <-s.closed:
		return false
	default:
		return true
	}
}
func (s *legacyEngineSession) Close() error {
	s.closeGateMu.Lock()
	started := s.closeStarted
	release := s.releaseClose
	s.closeGateMu.Unlock()
	if started != nil {
		s.closeStartOne.Do(func() { close(started) })
	}
	if release != nil {
		<-release
	}
	s.close.Do(func() {
		close(s.closed)
		close(s.events)
	})
	return nil
}

func (s *legacyEngineSession) blockClose() (<-chan struct{}, chan<- struct{}) {
	s.closeGateMu.Lock()
	defer s.closeGateMu.Unlock()
	s.closeStarted = make(chan struct{})
	s.releaseClose = make(chan struct{})
	return s.closeStarted, s.releaseClose
}

func waitLegacyEngineSession(t *testing.T, agent *legacyEngineAgent, key string) *legacyEngineSession {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if session := agent.session(key); session != nil {
			return session
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待 %s AgentSession 超时", key)
	return nil
}

func waitLegacyEngineSend(t *testing.T, session *legacyEngineSession, what string) legacyEngineSend {
	t.Helper()
	select {
	case record := <-session.sent:
		return record
	case <-time.After(2 * time.Second):
		t.Fatalf("等待%s超时", what)
		return legacyEngineSend{}
	}
}

func TestEngineWithLegacySchedulerCloseBarrierBlocksOnlySameSessionLane(t *testing.T) {
	scheduler := newLegacyEngineSchedulerUnderTest()
	agent := newLegacyEngineAgent()
	agent.workDir = t.TempDir()
	platform := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish, WithScheduler(scheduler))
	e.agentSessionCloseTimeout = 20 * time.Millisecond
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = e.Stop() })

	keyA := "test:close-a"
	keyB := "test:close-b"
	e.handleMessage(platform, &Message{SessionKey: keyA, Platform: "test", MessageID: "a1", UserID: "u", UserName: "u", Content: "a1", ReplyCtx: "ra1"})
	sessionA1 := waitLegacyEngineSession(t, agent, keyA)
	waitLegacyEngineSend(t, sessionA1, "A1 Send")
	closeStarted, releaseClose := sessionA1.blockClose()
	var releaseCloseOnce sync.Once
	t.Cleanup(func() { releaseCloseOnce.Do(func() { close(releaseClose) }) })

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	cancelResult := scheduler.CancelSessionContext(ctx, keyA, CancelStop)
	cancel()
	if cancelResult.ActiveCancelled != 1 || !cancelResult.Pending {
		t.Fatalf("cancel result=%+v, want active=1 pending", cancelResult)
	}
	waitLegacySignal(t, closeStarted, "A1 Close 进入物理屏障")

	e.handleMessage(platform, &Message{SessionKey: keyA, Platform: "test", MessageID: "a2", UserID: "u", UserName: "u", Content: "a2", ReplyCtx: "ra2"})
	e.handleMessage(platform, &Message{SessionKey: keyB, Platform: "test", MessageID: "b1", UserID: "u", UserName: "u", Content: "b1", ReplyCtx: "rb1"})
	sessionB1 := waitLegacyEngineSession(t, agent, keyB)
	waitLegacyEngineSend(t, sessionB1, "B1 在 A1 Close 阻塞时并行 Send")

	if got := agent.session(keyA); got != sessionA1 {
		t.Fatalf("A2 在 A1 Close 返回前创建了新 session: got=%p A1=%p", got, sessionA1)
	}
	if snap := scheduler.Snapshot(); snap.ActiveCount != 2 || snap.WaitingCount != 1 {
		t.Fatalf("Close 阻塞期间 snapshot=%+v, want active=2 waiting=1", snap)
	}

	releaseCloseOnce.Do(func() { close(releaseClose) })
	waitLegacyCondition(t, "A2 在 A1 Close 确认后创建 session", func() bool {
		return agent.session(keyA) != nil && agent.session(keyA) != sessionA1
	})
	sessionA2 := agent.session(keyA)
	waitLegacyEngineSend(t, sessionA2, "A2 在 A1 Close 确认后 Send")

	sessionA2.releases <- struct{}{}
	sessionB1.releases <- struct{}{}
	waitLegacyCondition(t, "Close barrier 集成任务全部终态", func() bool {
		snap := scheduler.Snapshot()
		return snap.ActiveCount == 0 && snap.WaitingCount == 0
	})
}

func TestEngineWithLegacySchedulerUsesOneQueueAndRunsDifferentSessionsInParallel(t *testing.T) {
	scheduler := newLegacyEngineSchedulerUnderTest()
	agent := newLegacyEngineAgent()
	agent.workDir = t.TempDir()
	platform := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", agent, []Platform{platform}, "", LangEnglish, WithScheduler(scheduler))
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = e.Stop() })

	keyA := "test:legacy-a"
	keyB := "test:legacy-b"
	e.handleMessage(platform, &Message{SessionKey: keyA, Platform: "test", MessageID: "a1", UserID: "u", UserName: "u", Content: "a1", ReplyCtx: "ra1"})
	sessionA := waitLegacyEngineSession(t, agent, keyA)
	waitLegacyEngineSend(t, sessionA, "A1 Send")

	second := &Message{
		SessionKey: keyA, Platform: "test", MessageID: "a2", UserID: "u", UserName: "u",
		Content: "a2", ReplyCtx: "ra2",
		Files: []FileAttachment{{FileName: "a2.txt", MimeType: "text/plain", Data: []byte("a2-bytes")}},
	}
	e.handleMessage(platform, second)
	if second.Staged == nil || len(second.Files) != 0 {
		t.Fatalf("A2 未安全 staging 或仍保留 raw bytes: staged=%v files=%d", second.Staged != nil, len(second.Files))
	}
	stagedPath := second.Staged.Entries()[0].Path
	if _, err := os.Stat(stagedPath); err != nil {
		t.Fatalf("A2 waiting 期间 staged 文件不存在: %v", err)
	}
	e.handleMessage(platform, &Message{SessionKey: keyB, Platform: "test", MessageID: "b1", UserID: "u", UserName: "u", Content: "b1", ReplyCtx: "rb1"})
	sessionB := waitLegacyEngineSession(t, agent, keyB)
	waitLegacyEngineSend(t, sessionB, "B1 与 A1 并行 Send")

	snapshot := scheduler.Snapshot()
	if snapshot.ActiveCount != 2 || snapshot.WaitingCount != 1 {
		t.Fatalf("scheduler snapshot=%+v, want active=2 waiting=1", snapshot)
	}
	e.interactiveMu.Lock()
	stateA := e.interactiveStates[keyA]
	e.interactiveMu.Unlock()
	if stateA == nil {
		t.Fatal("A interactive state 不存在")
	}
	stateA.mu.Lock()
	pending := len(stateA.pendingMessages)
	stateA.mu.Unlock()
	if pending != 0 {
		t.Fatalf("普通消息进入第二层 pendingMessages=%d, want 0", pending)
	}
	select {
	case <-sessionA.sent:
		t.Fatal("A2 在 A1 真实终态前提前 Send")
	default:
	}

	sessionA.releases <- struct{}{}
	secondSend := waitLegacyEngineSend(t, sessionA, "A2 在 A1 结束后 Send")
	if len(secondSend.files) != 1 || string(secondSend.files[0].Data) != "a2-bytes" {
		t.Fatalf("A2 Send files=%+v, want staged a2-bytes", secondSend.files)
	}
	if _, err := os.Stat(stagedPath); err != nil {
		t.Fatalf("A2 Run 未返回时 staged 文件被提前清理: %v", err)
	}
	sessionA.releases <- struct{}{}
	sessionB.releases <- struct{}{}
	waitLegacyCondition(t, "Engine Legacy 任务全部终态", func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.ActiveCount == 0 && snapshot.WaitingCount == 0
	})
	if _, err := os.Stat(stagedPath); !os.IsNotExist(err) {
		t.Fatalf("A2 terminal 后 staged 文件未清理: %v", err)
	}

	var aReplies, bReplies []replyCtxCall
	for _, event := range platform.recordedEvents() {
		switch event.content {
		case "done:" + keyA:
			aReplies = append(aReplies, event)
		case "done:" + keyB:
			bReplies = append(bReplies, event)
		}
	}
	if len(aReplies) != 2 || aReplies[0].op != "reply" || aReplies[0].replyCtx != "ra1" ||
		aReplies[1].op != "reply" || aReplies[1].replyCtx != "ra2" {
		t.Fatalf("A 最终文本未按各自 ReplyCtx Reply: %+v", aReplies)
	}
	if len(bReplies) != 1 || bReplies[0].op != "reply" || bReplies[0].replyCtx != "rb1" {
		t.Fatalf("B 最终文本未按自身 ReplyCtx Reply: %+v", bReplies)
	}
}
