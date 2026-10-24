package core

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingStartAgent struct {
	name    string
	workDir string

	started chan struct{}
	release chan struct{}

	calls atomic.Int32

	mu          sync.Mutex
	ctxs        []context.Context
	sessionIDs  []string
	nextSession AgentSession
	nextErr     error
	ignoreCtx   bool
}

func newBlockingStartAgent(name string) *blockingStartAgent {
	return &blockingStartAgent{
		name:    name,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (a *blockingStartAgent) Name() string {
	if a.name != "" {
		return a.name
	}
	return "blocking"
}
func (a *blockingStartAgent) GetWorkDir() string { return a.workDir }

func (a *blockingStartAgent) StartSession(ctx context.Context, sessionID string) (AgentSession, error) {
	a.calls.Add(1)
	a.mu.Lock()
	a.ctxs = append(a.ctxs, ctx)
	a.sessionIDs = append(a.sessionIDs, sessionID)
	a.mu.Unlock()
	select {
	case <-a.started:
	default:
		close(a.started)
	}
	if a.ignoreCtx {
		<-a.release
	} else {
		select {
		case <-a.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if a.nextErr != nil {
		return nil, a.nextErr
	}
	if a.nextSession != nil {
		return a.nextSession, nil
	}
	return &stubAgentSession{}, nil
}

func (a *blockingStartAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}
func (a *blockingStartAgent) Stop() error { return nil }

func (a *blockingStartAgent) startCallCount() int {
	return int(a.calls.Load())
}

type closeRecordingSession struct {
	id string

	closed       chan struct{}
	closeStarted chan struct{}
	releaseClose chan struct{}
	err          error
	once         sync.Once
	startedOnce  sync.Once

	mu      sync.Mutex
	prompts []string
}

func newCloseRecordingSession(id string) *closeRecordingSession {
	return &closeRecordingSession{id: id, closed: make(chan struct{}), closeStarted: make(chan struct{})}
}

func (s *closeRecordingSession) Send(prompt string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.mu.Lock()
	s.prompts = append(s.prompts, prompt)
	s.mu.Unlock()
	return nil
}
func (s *closeRecordingSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *closeRecordingSession) Events() <-chan Event                             { return make(chan Event) }
func (s *closeRecordingSession) CurrentSessionID() string                         { return s.id }
func (s *closeRecordingSession) Alive() bool                                      { return true }
func (s *closeRecordingSession) Close() error {
	s.startedOnce.Do(func() { close(s.closeStarted) })
	if s.releaseClose != nil {
		<-s.releaseClose
	}
	if s.err == nil {
		s.once.Do(func() { close(s.closed) })
	}
	return s.err
}
func (s *closeRecordingSession) promptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.prompts)
}

func waitForClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertNotClosedWithin(t *testing.T, ch <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s closed unexpectedly", what)
	case <-time.After(d):
	}
}

func TestInteractiveStart_DoesNotHoldInteractiveMuWhileAgentStartBlocks(t *testing.T) {
	// Catches the bug where getOrCreateInteractiveStateWithContext runs Agent.StartSession
	// while holding interactiveMu. A blocked start for one key must not stall short
	// state lookups for another key (permission callbacks, /ps, cards, cleanup, etc.).
	agent := newBlockingStartAgent("blocking")
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	platform := &stubPlatformEngine{n: "test"}

	pending := &pendingPermission{RequestID: "perm-1", Resolved: make(chan struct{})}
	e.interactiveMu.Lock()
	e.interactiveStates["key-b"] = &interactiveState{pending: pending, agentSession: &stubAgentSession{}}
	e.interactiveMu.Unlock()

	sessionsA := NewSessionManager("")
	sessionA := sessionsA.GetOrCreateActive("key-a")
	ownerDone := make(chan *interactiveState, 1)
	go func() {
		ownerDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "key-a", platform, "ctx-a", sessionA, sessionsA, agent, "")
	}()
	waitForClosed(t, agent.started, "blocking StartSession to be entered")

	lookupDone := make(chan *pendingPermission, 1)
	go func() {
		_, got := e.lookupPending("key-b")
		lookupDone <- got
	}()

	select {
	case got := <-lookupDone:
		if got != pending {
			t.Fatalf("lookupPending returned %p, want %p", got, pending)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("lookupPending for a different key blocked behind Agent.StartSession")
	}

	close(agent.release)
	state := <-ownerDone
	if state == nil || state.agentSession == nil {
		t.Fatalf("owner start returned no live state: %#v", state)
	}
}

func TestInteractiveStart_SameKeyWaiterContextCancelDoesNotCancelOwner(t *testing.T) {
	// Catches the bug where a second same-key caller waits on interactiveMu and cannot
	// observe its own context cancellation. The waiter must return promptly without
	// canceling the owner start or creating a duplicate agent process.
	agent := newBlockingStartAgent("blocking")
	agent.nextSession = &stubAgentSession{}
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	platform := &stubPlatformEngine{n: "test"}

	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("same-key")

	ownerDone := make(chan *interactiveState, 1)
	go func() {
		ownerDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "same-key", platform, "ctx-owner", session, sessions, agent, "")
	}()
	waitForClosed(t, agent.started, "owner StartSession to be entered")

	waitCtx, cancelWait := context.WithCancel(context.Background())
	waiterDone := make(chan *interactiveState, 1)
	go func() {
		waiterDone <- e.getOrCreateInteractiveStateWithContext(waitCtx, "same-key", platform, "ctx-waiter", session, sessions, agent, "")
	}()
	cancelWait()

	select {
	case waiterState := <-waiterDone:
		if waiterState == nil {
			t.Fatal("waiter returned nil state")
		}
		if waiterState.agentSession != nil {
			t.Fatalf("canceled waiter published/observed an agent session: %#v", waiterState.agentSession)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("same-key waiter did not return after its context was canceled")
	}

	if got := agent.startCallCount(); got != 1 {
		t.Fatalf("StartSession calls before releasing owner = %d, want 1", got)
	}

	close(agent.release)
	ownerState := <-ownerDone
	if ownerState == nil || ownerState.agentSession == nil {
		t.Fatalf("owner was canceled by waiter or failed to publish: %#v", ownerState)
	}
	if got := agent.startCallCount(); got != 1 {
		t.Fatalf("StartSession calls after owner settled = %d, want 1", got)
	}
}

func TestInteractiveStart_SameKeyWaiterReceivesPublishedOwnerState(t *testing.T) {
	// Guards the same-key inflight promise: waiters that do not cancel should wait
	// for the owner result and observe the same published state without a duplicate start.
	agent := newBlockingStartAgent("blocking")
	agent.nextSession = &stubAgentSession{}
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	platform := &stubPlatformEngine{n: "test"}
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("same-key-success")

	ownerDone := make(chan *interactiveState, 1)
	go func() {
		ownerDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "same-key-success", platform, "ctx-owner", session, sessions, agent, "")
	}()
	waitForClosed(t, agent.started, "owner StartSession to be entered")

	waiterDone := make(chan *interactiveState, 1)
	go func() {
		waiterDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "same-key-success", platform, "ctx-waiter", session, sessions, agent, "")
	}()
	select {
	case st := <-waiterDone:
		t.Fatalf("same-key waiter returned before owner start settled: %#v", st)
	case <-time.After(25 * time.Millisecond):
	}

	close(agent.release)
	ownerState := <-ownerDone
	waiterState := <-waiterDone
	if ownerState == nil || ownerState.agentSession == nil {
		t.Fatalf("owner state = %#v, want live session", ownerState)
	}
	if waiterState != ownerState {
		t.Fatalf("waiter state = %p, want owner state %p", waiterState, ownerState)
	}
	if got := agent.startCallCount(); got != 1 {
		t.Fatalf("StartSession calls = %d, want 1", got)
	}
}

func TestInteractiveStart_ResumeContextCancelPreservesSavedIDAndDoesNotFallbackFresh(t *testing.T) {
	// Catches treating context cancellation as a real resume failure. /stop and
	// scheduler cancellation must preserve the saved AgentSessionID and must not
	// clear it or attempt a fresh fallback with an empty ID.
	agent := newBlockingStartAgent("blocking")
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	platform := &stubPlatformEngine{n: "test"}
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("resume-cancel")
	session.SetAgentSessionID("saved-resume-id", agent.Name())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan *interactiveState, 1)
	go func() {
		done <- e.getOrCreateInteractiveStateWithContext(ctx, "resume-cancel", platform, "ctx", session, sessions, agent, "")
	}()
	waitForClosed(t, agent.started, "resume StartSession to be entered")
	cancel()

	state := <-done
	if state == nil {
		t.Fatal("start returned nil state")
	}
	if state.agentSession != nil {
		t.Fatalf("canceled resume returned live session: %#v", state.agentSession)
	}
	if got := session.GetAgentSessionID(); got != "saved-resume-id" {
		t.Fatalf("saved session id = %q, want retained saved-resume-id", got)
	}
	agent.mu.Lock()
	ids := append([]string(nil), agent.sessionIDs...)
	agent.mu.Unlock()
	if len(ids) != 1 || ids[0] != "saved-resume-id" {
		t.Fatalf("StartSession ids = %q, want only saved-resume-id and no fresh fallback", ids)
	}
}

func TestInteractiveStart_FailedStartClearsPlaceholderAndAllowsRetry(t *testing.T) {
	// Catches stale failed placeholders: after a start failure, the next message must
	// be able to run StartSession again instead of reusing an agentSession-nil state.
	firstErr := errors.New("boom")
	calls := atomic.Int32{}
	agent := &controllableAgent{startSessionFn: func(context.Context, string) (AgentSession, error) {
		if calls.Add(1) == 1 {
			return nil, firstErr
		}
		return &stubAgentSession{}, nil
	}}
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	platform := &stubPlatformEngine{n: "test"}
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("retry-key")

	failed := e.getOrCreateInteractiveStateWithContext(context.Background(), "retry-key", platform, "ctx-1", session, sessions, agent, "")
	if failed == nil {
		t.Fatal("first start returned nil state")
	}
	if failed.agentSession != nil {
		t.Fatalf("failed start returned live session: %#v", failed.agentSession)
	}

	e.interactiveMu.Lock()
	_, existsAfterFailure := e.interactiveStates["retry-key"]
	e.interactiveMu.Unlock()
	if existsAfterFailure {
		t.Fatal("failed start left a placeholder in interactiveStates")
	}

	retried := e.getOrCreateInteractiveStateWithContext(context.Background(), "retry-key", platform, "ctx-2", session, sessions, agent, "")
	if retried == nil || retried.agentSession == nil {
		t.Fatalf("retry did not publish a live session: %#v", retried)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("StartSession calls = %d, want 2", got)
	}
}

func TestInteractiveStart_FailedStartDoesNotDropProjectSchedulerWaitingTask(t *testing.T) {
	// Failed interactive start may clear only the current interactive placeholder;
	// it must not delete unrelated ProjectScheduler waiting tasks.
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{}, 1)
	calls := atomic.Int32{}
	agent := &controllableAgent{startSessionFn: func(context.Context, string) (AgentSession, error) {
		call := calls.Add(1)
		if call == 1 {
			close(firstStarted)
			<-releaseFirst
			return nil, errors.New("first start failed")
		}
		secondStarted <- struct{}{}
		return newResultAgentSession("second ok"), nil
	}}
	platform := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	e.handleMessage(platform, &Message{SessionKey: "test:first", Platform: "test", MessageID: "m-first", UserID: "u", UserName: "u", Content: "first", ReplyCtx: "ctx-first"})
	waitForClosed(t, firstStarted, "first failing StartSession to be entered")
	e.handleMessage(platform, &Message{SessionKey: "test:second", Platform: "test", MessageID: "m-second", UserID: "u", UserName: "u", Content: "second", ReplyCtx: "ctx-second"})
	if snap := e.projectScheduler.Snapshot(); snap.WaitingCount != 1 {
		t.Fatalf("waiting count before failed start release = %d, want 1", snap.WaitingCount)
	}

	close(releaseFirst)
	waitForClosed(t, secondStarted, "second waiting task to start after first start failure")
	if got := calls.Load(); got != 2 {
		t.Fatalf("StartSession calls = %d, want failed active plus waiting successor", got)
	}
}

func TestInteractiveStart_LateSuccessAfterStopIsClosedAndNotPublished(t *testing.T) {
	// Catches late publish after /stop: a StartSession that succeeds after its
	// placeholder was stopped must be closed, must not be stored, and must not write
	// the late agent session ID back to the logical Session.
	lateSession := newCloseRecordingSession("late-id")
	agent := newBlockingStartAgent("blocking")
	agent.nextSession = lateSession
	agent.ignoreCtx = true
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	platform := &stubPlatformEngine{n: "test"}
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("late-key")

	ownerDone := make(chan *interactiveState, 1)
	go func() {
		ownerDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "late-key", platform, "ctx", session, sessions, agent, "")
	}()
	waitForClosed(t, agent.started, "StartSession to be entered")

	stopDone := make(chan bool, 1)
	go func() {
		stopDone <- e.stopInteractiveSessionWithOptions("late-key", true)
	}()
	select {
	case stopped := <-stopDone:
		if !stopped {
			t.Fatal("stopInteractiveSessionWithOptions returned false for starting placeholder")
		}
	case <-time.After(200 * time.Millisecond):
		close(agent.release)
		<-ownerDone
		t.Fatal("stopInteractiveSessionWithOptions blocked behind starting Agent.StartSession")
	}

	close(agent.release)
	state := <-ownerDone
	if state == nil {
		t.Fatal("owner returned nil state")
	}
	if state.agentSession != nil {
		t.Fatalf("late stopped start returned/published session: %#v", state.agentSession)
	}
	waitForClosed(t, lateSession.closed, "late successful session to be closed")

	e.interactiveMu.Lock()
	published := e.interactiveStates["late-key"]
	e.interactiveMu.Unlock()
	if published != nil && published.agentSession == lateSession {
		t.Fatal("late successful session was published in interactiveStates")
	}
	if got := session.GetAgentSessionID(); got != "" {
		t.Fatalf("late session ID was saved as %q", got)
	}
}

type envPromptSnapshotAgent struct {
	mu     sync.Mutex
	env    []string
	prompt string

	startEntered chan string
	release      chan struct{}
	snapshots    []envPromptSnapshot
}

type envPromptSnapshot struct {
	sessionID  string
	sessionKey string
	prompt     string
}

func newEnvPromptSnapshotAgent() *envPromptSnapshotAgent {
	return &envPromptSnapshotAgent{
		startEntered: make(chan string, 2),
		release:      make(chan struct{}),
	}
}

func (a *envPromptSnapshotAgent) Name() string { return "snapshot" }
func (a *envPromptSnapshotAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}
func (a *envPromptSnapshotAgent) Stop() error { return nil }
func (a *envPromptSnapshotAgent) SetSessionEnv(env []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.env = append([]string(nil), env...)
}
func (a *envPromptSnapshotAgent) SetPlatformPrompt(prompt string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.prompt = prompt
}
func (a *envPromptSnapshotAgent) StartSession(ctx context.Context, sessionID string) (AgentSession, error) {
	a.mu.Lock()
	snap := envPromptSnapshot{sessionID: sessionID, prompt: a.prompt}
	for _, entry := range a.env {
		if strings.HasPrefix(entry, "CC_SESSION_KEY=") {
			snap.sessionKey = strings.TrimPrefix(entry, "CC_SESSION_KEY=")
		}
	}
	a.snapshots = append(a.snapshots, snap)
	a.mu.Unlock()
	a.startEntered <- snap.sessionKey
	select {
	case <-a.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &stubAgentSession{}, nil
}
func (a *envPromptSnapshotAgent) snapshotList() []envPromptSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]envPromptSnapshot, len(a.snapshots))
	copy(out, a.snapshots)
	return out
}

type formattingStubPlatform struct {
	stubPlatformEngine
	instructions string
}

func (p *formattingStubPlatform) FormattingInstructions() string { return p.instructions }

func TestInteractiveStart_ProjectCancelPendingLateSuccessDoesNotPublishOrSend(t *testing.T) {
	// 覆盖仅 Scheduler 取消的路径：CancelSessionContext 可能返回 Pending，且不会调用
	// stopInteractiveSessionWithOptions。若 StartSession 忽略 ctx 后迟到成功，已取消的
	// owner 必须 close late session，不能 publish/saveID/hook/Send 已取消 prompt。
	lateSession := newCloseRecordingSession("late-project-id")
	agent := newBlockingStartAgent("blocking")
	agent.nextSession = lateSession
	agent.ignoreCtx = true
	platform := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.projectSchedulerCancelTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = e.Stop() })

	var startedHooks atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		startedHooks.Add(1)
	}))
	defer server.Close()
	asyncFalse := false
	e.hooks = NewHookManager("test", []HookConfig{{Event: string(HookEventSessionStarted), Type: string(HookHandlerHTTP), URL: server.URL, Async: &asyncFalse}}, defaultShell(), defaultShellFlag(), "")

	e.handleMessage(platform, &Message{SessionKey: "test:project-cancel", Platform: "test", MessageID: "m-cancel", UserID: "u", UserName: "u", Content: "must not send", ReplyCtx: "ctx"})
	waitForClosed(t, agent.started, "StartSession to be entered")

	e.cmdStop(platform, &Message{SessionKey: "test:project-cancel", Platform: "test", ReplyCtx: "stop-ctx"})
	if got := lateSession.promptCount(); got != 0 {
		t.Fatalf("prompt count before late start release = %d, want 0", got)
	}

	close(agent.release)
	waitForClosed(t, lateSession.closed, "late project-canceled session to be closed")
	if got := lateSession.promptCount(); got != 0 {
		t.Fatalf("late project-canceled session received %d prompts, want 0", got)
	}
	if got := e.sessions.GetOrCreateActive("test:project-cancel").GetAgentSessionID(); got != "" {
		t.Fatalf("late project-canceled session id saved as %q", got)
	}
	e.interactiveMu.Lock()
	published := e.interactiveStates["test:project-cancel"]
	e.interactiveMu.Unlock()
	if published != nil && published.agentSession == lateSession {
		t.Fatal("late project-canceled session was published")
	}
	if got := startedHooks.Load(); got != 0 {
		t.Fatalf("session.started hooks = %d, want 0", got)
	}
}

func TestInteractiveStart_ProjectCancelLateCloseBlockedKeepsStagedAndWaitingUntilNil(t *testing.T) {
	// 真实 Engine + ProjectScheduler + staged 附件路径：active start 被运行态取消后，
	// late Start success 的 Close 阻塞期间，owner Run 不能完成，因此 staged 文件不能被
	// terminal cleanup 提前删除，其他 key 的 waiting 任务也不能启动；Close 返回 nil 后才收敛。
	workDir := t.TempDir()
	lateSession := newCloseRecordingSession("late-staged-id")
	lateSession.releaseClose = make(chan struct{})
	agent := newBlockingStartAgent("blocking")
	agent.workDir = workDir
	agent.nextSession = lateSession
	agent.ignoreCtx = true
	platform := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.projectSchedulerCancelTimeout = 20 * time.Millisecond
	e.agentSessionCloseTimeout = 20 * time.Millisecond
	t.Cleanup(func() { _ = e.Stop() })

	first := &Message{SessionKey: "test:staged-active", Platform: "test", MessageID: "m-staged-active", UserID: "u", UserName: "u", Content: "active", Files: []FileAttachment{{FileName: "active.txt", MimeType: "text/plain", Data: []byte("active-bytes")}}, ReplyCtx: "ctx-active"}
	e.handleMessage(platform, first)
	waitForClosed(t, agent.started, "active staged StartSession to be entered")
	waitAttachmentCondition(t, func() bool { return first.Staged != nil && len(first.Files) == 0 }, "active message staged")
	stagedPath := first.Staged.Entries()[0].Path
	e.interactiveMu.Lock()
	start := e.interactiveStates["test:staged-active"].starting
	e.interactiveMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result := e.projectScheduler.CancelSessionContext(ctx, "test:staged-active", CancelStop)
	if !result.Pending {
		t.Fatalf("CancelSessionContext result = %+v, want pending while StartSession ignores ctx", result)
	}
	second := &Message{SessionKey: "test:staged-waiting", Platform: "test", MessageID: "m-staged-waiting", UserID: "u", UserName: "u", Content: "waiting", ReplyCtx: "ctx-waiting"}
	e.handleMessage(platform, second)
	if snap := e.projectScheduler.Snapshot(); snap.WaitingCount != 1 {
		t.Fatalf("waiting count = %d, want 1 while active late close is unresolved", snap.WaitingCount)
	}

	close(agent.release)
	waitForClosed(t, lateSession.closeStarted, "late staged Close to start")
	// unsafeResume 只会在本 session 的 late-close 超时分支被置位；以此为确定性屏障，
	// 避免仅靠 sleep 猜测是否已经越过 agentSessionCloseTimeout。
	waitAttachmentCondition(t, func() bool { return e.peekUnsafeResume("test:staged-active") }, "late staged Close timeout marker")
	assertNotClosedWithin(t, start.done, 10*time.Millisecond, "start.done after late Close timeout")
	if snap := e.projectScheduler.Snapshot(); snap.ActiveCount != 1 || snap.WaitingCount != 1 {
		t.Fatalf("scheduler snapshot after late Close timeout = %+v, want active and waiting retained", snap)
	}
	if _, err := os.Stat(stagedPath); err != nil {
		t.Fatalf("staged file cleaned before late Close nil: %v", err)
	}
	if got := agent.startCallCount(); got != 1 {
		t.Fatalf("StartSession count while late Close blocked = %d, want only active", got)
	}

	close(lateSession.releaseClose)
	waitForClosed(t, start.done, "start.done after late Close nil")
	waitAttachmentCondition(t, func() bool {
		_, err := os.Stat(stagedPath)
		return os.IsNotExist(err)
	}, "staged cleanup after late Close nil")
	waitAttachmentCondition(t, func() bool { return agent.startCallCount() >= 2 }, "waiting task starts after late Close nil")
}

func TestInteractiveStart_EngineStopLateCloseErrorKeepsStartUnsettledAndSameKeyBlocked(t *testing.T) {
	// 覆盖 Engine.Stop 专属缺陷：late Start success 时 e.ctx 已经取消，Close error
	// 不能让 close registration/start.done 收敛，且未确认 close 必须阻断 same-key 重复启动。
	lateSession := newCloseRecordingSession("late-close-error-id")
	lateSession.err = errors.New("close failed")
	agent := newBlockingStartAgent("blocking")
	agent.nextSession = lateSession
	agent.ignoreCtx = true
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	platform := &stubPlatformEngine{n: "test"}
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("engine-stop-close-error")

	ownerDone := make(chan *interactiveState, 1)
	go func() {
		ownerDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "engine-stop-close-error", platform, "ctx", session, sessions, agent, "")
	}()
	waitForClosed(t, agent.started, "StartSession to be entered")
	e.interactiveMu.Lock()
	start := e.interactiveStates["engine-stop-close-error"].starting
	e.interactiveMu.Unlock()

	stopDone := make(chan error, 1)
	go func() { stopDone <- e.Stop() }()
	select {
	case err := <-stopDone:
		if err == nil {
			t.Fatal("Engine.Stop returned nil while StartSession was still ignoring cancellation")
		}
	case <-time.After(200 * time.Millisecond):
		close(agent.release)
		t.Fatal("Engine.Stop did not return a bounded error for non-cooperative StartSession")
	}

	close(agent.release)
	waitForClosed(t, lateSession.closeStarted, "late Close to start")
	assertNotClosedWithin(t, start.done, 75*time.Millisecond, "start.done after late Close error")

	waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelWaiter()
	waiterDone := make(chan *interactiveState, 1)
	go func() {
		waiterDone <- e.getOrCreateInteractiveStateWithContext(waiterCtx, "engine-stop-close-error", platform, "ctx-waiter", session, sessions, agent, "")
	}()
	select {
	case <-waiterDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("same-key waiter did not return on its own context while close remained unconfirmed")
	}
	if got := agent.startCallCount(); got != 1 {
		t.Fatalf("StartSession calls = %d, want no duplicate same-key start while close unconfirmed", got)
	}
}

func TestInteractiveStart_EngineStopLateCloseBlockedUntilNilBeforeStartSettles(t *testing.T) {
	// late Close 阻塞时，即使 e.ctx 已取消也必须保持 start.done 未关闭；
	// 只有 Close 返回 nil 后，close registration/start 生命周期才允许收敛。
	lateSession := newCloseRecordingSession("late-close-blocked-id")
	lateSession.releaseClose = make(chan struct{})
	agent := newBlockingStartAgent("blocking")
	agent.nextSession = lateSession
	agent.ignoreCtx = true
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	e.agentSessionCloseTimeout = 20 * time.Millisecond
	platform := &stubPlatformEngine{n: "test"}
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("engine-stop-close-blocked")

	ownerDone := make(chan *interactiveState, 1)
	go func() {
		ownerDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "engine-stop-close-blocked", platform, "ctx", session, sessions, agent, "")
	}()
	waitForClosed(t, agent.started, "StartSession to be entered")
	e.interactiveMu.Lock()
	start := e.interactiveStates["engine-stop-close-blocked"].starting
	e.interactiveMu.Unlock()

	stopDone := make(chan error, 1)
	go func() { stopDone <- e.Stop() }()
	select {
	case err := <-stopDone:
		if err == nil {
			t.Fatal("Engine.Stop returned nil while StartSession was still ignoring cancellation")
		}
	case <-time.After(200 * time.Millisecond):
		close(agent.release)
		t.Fatal("Engine.Stop did not return a bounded error for non-cooperative StartSession")
	}

	close(agent.release)
	waitForClosed(t, lateSession.closeStarted, "late blocked Close to start")
	assertNotClosedWithin(t, start.done, 75*time.Millisecond, "start.done while late Close is blocked")
	close(lateSession.releaseClose)
	waitForClosed(t, start.done, "start.done after late Close nil")
	if state := <-ownerDone; state == nil || state.agentSession != nil {
		t.Fatalf("owner state after late blocked Close = %#v, want non-live placeholder", state)
	}
}

func TestInteractiveStart_CancelAndNewCooperativeStartingResetsOnlyAfterStartSettles(t *testing.T) {
	// 合作式 /cancel 与 /new 仍应快速完成，并且只在 active starting task
	// 实际观察到取消后重置会话。
	for _, tc := range []struct {
		name string
		run  func(*Engine, Platform, *Message)
	}{
		{name: "cancel", run: func(e *Engine, p Platform, m *Message) { e.cmdCancel(p, m) }},
		{name: "new", run: func(e *Engine, p Platform, m *Message) { e.cmdNew(p, m, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent := newBlockingStartAgent("blocking")
			platform := &stubPlatformEngine{n: "test"}
			e := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
			e.projectSchedulerCancelTimeout = 200 * time.Millisecond
			t.Cleanup(func() { _ = e.Stop() })
			key := "test:" + tc.name + "-cooperative"
			old := e.sessions.GetOrCreateActive(key)
			old.SetAgentSessionID("old-id", agent.Name())
			old.AddHistory("user", "old history")
			oldSessionID := old.ID

			e.handleMessage(platform, &Message{SessionKey: key, Platform: "test", MessageID: "m-start", UserID: "u", UserName: "u", Content: "start", ReplyCtx: "ctx-start"})
			waitForClosed(t, agent.started, "cooperative StartSession to be entered")
			tc.run(e, platform, &Message{SessionKey: key, Platform: "test", ReplyCtx: "ctx-cmd"})

			after := e.sessions.GetOrCreateActive(key)
			if after.ID == oldSessionID {
				t.Fatalf("%s kept old active session id %q after cooperative starting cancel", tc.name, oldSessionID)
			}
			if got := after.GetAgentSessionID(); got != "" {
				t.Fatalf("%s new active AgentSessionID = %q, want empty", tc.name, got)
			}
			if hist := after.GetHistory(0); len(hist) != 0 {
				t.Fatalf("%s new active history = %#v, want empty", tc.name, hist)
			}
		})
	}
}

func TestInteractiveStart_EngineStopNonCooperativeStartReturnsBoundedErrorThenClosesLateSuccess(t *testing.T) {
	// Engine.Stop 可以有界返回，但 StartSession 仍忽略取消时不能返回成功；
	// 该 late start 后续成功时仍必须 close，而不能 publish。
	lateSession := newCloseRecordingSession("late-stop-id")
	agent := newBlockingStartAgent("blocking")
	agent.nextSession = lateSession
	agent.ignoreCtx = true
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	e.projectSchedulerStopTimeout = 20 * time.Millisecond
	platform := &stubPlatformEngine{n: "test"}
	sessions := NewSessionManager("")
	session := sessions.GetOrCreateActive("engine-stop-key")

	ownerDone := make(chan *interactiveState, 1)
	go func() {
		ownerDone <- e.getOrCreateInteractiveStateWithContext(context.Background(), "engine-stop-key", platform, "ctx", session, sessions, agent, "")
	}()
	waitForClosed(t, agent.started, "StartSession to be entered")

	stopDone := make(chan error, 1)
	go func() { stopDone <- e.Stop() }()
	select {
	case err := <-stopDone:
		if err == nil {
			t.Fatal("Engine.Stop returned nil while StartSession was still ignoring cancellation")
		}
	case <-time.After(200 * time.Millisecond):
		close(agent.release)
		<-ownerDone
		t.Fatal("Engine.Stop did not return a bounded error for non-cooperative StartSession")
	}

	close(agent.release)
	state := <-ownerDone
	if state == nil {
		t.Fatal("owner returned nil state")
	}
	if state.agentSession != nil {
		t.Fatalf("late engine-stop start returned/published session: %#v", state.agentSession)
	}
	waitForClosed(t, lateSession.closed, "late engine-stop session to be closed")
}

func TestInteractiveStart_StartSemaphoreWaiterContextCancelDoesNotMutateEnvPrompt(t *testing.T) {
	// 等待共享 Agent 启动事务期间被取消的 start，必须在 SetSessionEnv /
	// SetPlatformPrompt / StartSession 产生 Agent 副作用前退出。
	agent := newEnvPromptSnapshotAgent()
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	p1 := &formattingStubPlatform{stubPlatformEngine: stubPlatformEngine{n: "p1"}, instructions: "prompt-one"}
	p2 := &formattingStubPlatform{stubPlatformEngine: stubPlatformEngine{n: "p2"}, instructions: "prompt-two"}
	sessions1 := NewSessionManager("")
	session1 := sessions1.GetOrCreateActive("sem-key-1")
	sessions2 := NewSessionManager("")
	session2 := sessions2.GetOrCreateActive("sem-key-2")

	done1 := make(chan *interactiveState, 1)
	go func() {
		done1 <- e.getOrCreateInteractiveStateWithContext(context.Background(), "sem-key-1", p1, "ctx-1", session1, sessions1, agent, "")
	}()
	firstKey := <-agent.startEntered
	if firstKey != "sem-key-1" {
		t.Fatalf("first start CC_SESSION_KEY = %q, want sem-key-1", firstKey)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan *interactiveState, 1)
	go func() {
		done2 <- e.getOrCreateInteractiveStateWithContext(ctx2, "sem-key-2", p2, "ctx-2", session2, sessions2, agent, "")
	}()
	cancel2()
	select {
	case st := <-done2:
		if st == nil {
			t.Fatal("canceled start-semaphore waiter returned nil state")
		}
		if st.agentSession != nil {
			t.Fatalf("canceled start-semaphore waiter returned live session: %#v", st.agentSession)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("canceled start-semaphore waiter did not return")
	}
	if snaps := agent.snapshotList(); len(snaps) != 1 || snaps[0].sessionKey != "sem-key-1" {
		t.Fatalf("snapshots after canceled waiter = %#v, want only sem-key-1", snaps)
	}

	close(agent.release)
	if st := <-done1; st == nil || st.agentSession == nil {
		t.Fatalf("first start failed: %#v", st)
	}
}

func TestInteractiveStart_SerializesPerAgentEnvPromptSnapshot(t *testing.T) {
	// 保护 SetSessionEnv/SetPlatformPrompt 到 Agent.StartSession 的启动事务；
	// StartSession 移出 interactiveMu 后，同一 Agent 的两个 start 不能读到彼此的
	// CC_SESSION_KEY 或 platform prompt。
	agent := newEnvPromptSnapshotAgent()
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "", LangEnglish)
	p1 := &formattingStubPlatform{stubPlatformEngine: stubPlatformEngine{n: "p1"}, instructions: "prompt-one"}
	p2 := &formattingStubPlatform{stubPlatformEngine: stubPlatformEngine{n: "p2"}, instructions: "prompt-two"}
	sessions1 := NewSessionManager("")
	session1 := sessions1.GetOrCreateActive("key-1")
	sessions2 := NewSessionManager("")
	session2 := sessions2.GetOrCreateActive("key-2")

	done1 := make(chan *interactiveState, 1)
	go func() {
		done1 <- e.getOrCreateInteractiveStateWithContext(context.Background(), "key-1", p1, "ctx-1", session1, sessions1, agent, "")
	}()
	firstKey := <-agent.startEntered
	if firstKey != "key-1" {
		t.Fatalf("first start CC_SESSION_KEY = %q, want key-1", firstKey)
	}

	done2 := make(chan *interactiveState, 1)
	go func() {
		done2 <- e.getOrCreateInteractiveStateWithContext(context.Background(), "key-2", p2, "ctx-2", session2, sessions2, agent, "")
	}()

	assertNotClosedWithin(t, func() <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			select {
			case <-agent.startEntered:
				close(ch)
			case <-time.After(50 * time.Millisecond):
			}
		}()
		return ch
	}(), 75*time.Millisecond, "second StartSession entered while first same-agent start was in progress")

	close(agent.release)
	if st := <-done1; st == nil || st.agentSession == nil {
		t.Fatalf("first start failed: %#v", st)
	}
	if st := <-done2; st == nil || st.agentSession == nil {
		t.Fatalf("second start failed: %#v", st)
	}

	snaps := agent.snapshotList()
	if len(snaps) != 2 {
		t.Fatalf("snapshots = %#v, want 2", snaps)
	}
	byKey := map[string]envPromptSnapshot{}
	for _, snap := range snaps {
		byKey[snap.sessionKey] = snap
	}
	if got := byKey["key-1"].prompt; got != "prompt-one" {
		t.Fatalf("key-1 prompt = %q, want prompt-one (snapshots=%#v)", got, snaps)
	}
	if got := byKey["key-2"].prompt; got != "prompt-two" {
		t.Fatalf("key-2 prompt = %q, want prompt-two (snapshots=%#v)", got, snaps)
	}
}
