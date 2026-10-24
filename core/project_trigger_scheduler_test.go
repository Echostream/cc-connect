package core

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type projectTriggerPlatform struct {
	stubPlatformEngine
}

func (p *projectTriggerPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	return "rctx:" + sessionKey, nil
}

type projectTriggerAgent struct {
	mu            sync.Mutex
	starts        []string
	current       int
	maxConcurrent int
	started       chan string
	release       chan struct{}
}

func newProjectTriggerAgent() *projectTriggerAgent {
	return &projectTriggerAgent{started: make(chan string, 16), release: make(chan struct{}, 16)}
}

func (a *projectTriggerAgent) Name() string { return "project-trigger" }
func (a *projectTriggerAgent) StartSession(_ context.Context, _ string) (AgentSession, error) {
	return &projectTriggerSession{agent: a, events: make(chan Event, 4), id: fmt.Sprintf("pt-%d", time.Now().UnixNano())}, nil
}
func (a *projectTriggerAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}
func (a *projectTriggerAgent) Stop() error { return nil }

func (a *projectTriggerAgent) begin(label string) {
	a.mu.Lock()
	a.starts = append(a.starts, label)
	a.current++
	if a.current > a.maxConcurrent {
		a.maxConcurrent = a.current
	}
	a.mu.Unlock()
	a.started <- label
}

func (a *projectTriggerAgent) end() {
	a.mu.Lock()
	a.current--
	a.mu.Unlock()
}

func (a *projectTriggerAgent) snapshot() ([]string, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.starts...), a.maxConcurrent
}

type projectTriggerSession struct {
	agent  *projectTriggerAgent
	events chan Event
	id     string
}

func (s *projectTriggerSession) Send(prompt string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	label := projectTriggerLabel(prompt)
	s.agent.begin(label)
	<-s.agent.release
	s.events <- Event{Type: EventResult, Content: "ok", Done: true, SessionID: s.id}
	s.agent.end()
	return nil
}
func (s *projectTriggerSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *projectTriggerSession) Events() <-chan Event                             { return s.events }
func (s *projectTriggerSession) CurrentSessionID() string                         { return s.id }
func (s *projectTriggerSession) Alive() bool                                      { return true }
func (s *projectTriggerSession) Close() error                                     { return nil }

func projectTriggerLabel(prompt string) string {
	for _, label := range []string{"interactive-1", "timer", "heartbeat", "relay", "cron", "interactive-2"} {
		if strings.Contains(prompt, label) {
			return label
		}
	}
	return prompt
}

func waitProjectTriggerStart(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("start order got %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for start %q", want)
	}
}

func waitProjectTriggerSnapshot(t *testing.T, scheduler *ProjectScheduler, wantWaiting int) SchedulerSnapshot {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	var last SchedulerSnapshot
	for time.Now().Before(deadline) {
		last = scheduler.Snapshot()
		if last.ActiveCount == 1 && last.WaitingCount == wantWaiting {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("scheduler snapshot never reached active=1 waiting=%d; last=%+v", wantWaiting, last)
	return last
}

func TestAllAgentTriggersShareOneFIFO(t *testing.T) {
	// Catches: any Agent trigger that bypasses ProjectScheduler starts while an
	// interactive turn is active, or enters a second queue instead of the single
	// project FIFO. Expected order is hand-derived from submission order below.
	clock := newFakeSchedulerClock(time.Unix(8_000, 0))
	scheduler := NewProjectScheduler(clock, 10, 6*time.Hour, 30*time.Minute)
	agent := newProjectTriggerAgent()
	platform := &projectTriggerPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.projectScheduler = scheduler
	t.Cleanup(func() { _ = e.Stop() })

	e.handleMessage(platform, &Message{SessionKey: "test:interactive-1:user", Platform: "test", MessageID: "i1", UserID: "u", UserName: "u", Content: "interactive-1", ReplyCtx: "ri1"})
	waitProjectTriggerStart(t, agent.started, "interactive-1")

	done := make(chan error, 4)
	go func() {
		done <- e.ExecuteTimerJob(&TimerJob{ID: "tm", Project: "test", SessionKey: "test:timer:user", Prompt: "timer"})
	}()
	waitProjectTriggerSnapshot(t, scheduler, 1)
	go func() { done <- e.ExecuteHeartbeat("test:heartbeat:user", "heartbeat", true) }()
	waitProjectTriggerSnapshot(t, scheduler, 2)
	relayDone := make(chan error, 1)
	go func() {
		_, err := e.HandleRelay(context.Background(), "source", "test:relay:user", "relay")
		relayDone <- err
	}()
	waitProjectTriggerSnapshot(t, scheduler, 3)
	go func() {
		done <- e.ExecuteCronJob(&CronJob{ID: "cr", Project: "test", SessionKey: "test:cron:user", Prompt: "cron"})
	}()
	waitProjectTriggerSnapshot(t, scheduler, 4)
	e.handleMessage(platform, &Message{SessionKey: "test:interactive-2:user", Platform: "test", MessageID: "i2", UserID: "u", UserName: "u", Content: "interactive-2", ReplyCtx: "ri2"})

	snap := waitProjectTriggerSnapshot(t, scheduler, 5)
	gotSources := make([]string, 0, len(snap.Waiting))
	for _, task := range snap.Waiting {
		gotSources = append(gotSources, string(task.Source))
	}
	wantSources := []string{string(TaskSourceTimer), string(TaskSourceHeartbeat), string(TaskSourceRelay), string(TaskSourceCron), string(TaskSourceInteractive)}
	if strings.Join(gotSources, ",") != strings.Join(wantSources, ",") {
		t.Fatalf("waiting sources = %v, want %v", gotSources, wantSources)
	}

	for _, want := range []string{"timer", "heartbeat", "relay", "cron", "interactive-2"} {
		agent.release <- struct{}{}
		waitProjectTriggerStart(t, agent.started, want)
	}
	agent.release <- struct{}{}

	deadline := time.After(2 * time.Second)
	for completed := 0; completed < 4; completed++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("proactive trigger returned error: %v", err)
			}
		case err := <-relayDone:
			if err != nil {
				t.Fatalf("relay returned error: %v", err)
			}
		case <-deadline:
			t.Fatal("timed out waiting for proactive trigger completion")
		}
	}

	starts, maxConcurrent := agent.snapshot()
	wantStarts := []string{"interactive-1", "timer", "heartbeat", "relay", "cron", "interactive-2"}
	if strings.Join(starts, ",") != strings.Join(wantStarts, ",") {
		t.Fatalf("start order = %v, want %v", starts, wantStarts)
	}
	if maxConcurrent != 1 {
		t.Fatalf("max concurrent agent turns = %d, want 1", maxConcurrent)
	}
}

func TestAgentSessionBypassStaticGate(t *testing.T) {
	// Catches: direct Agent.StartSession / AgentSession.Send outside reviewed
	// project task runners, wrapper bypasses such as processInteractiveMessageWith,
	// receiver renames, and new production files under core/. Uses go/types method
	// binding, not receiver variable names.
	fset := token.NewFileSet()
	files := make([]*ast.File, 0)
	pkg, err := build.Default.ImportDir(".", 0)
	if err != nil {
		t.Fatal(err)
	}
	paths := pkg.GoFiles
	for _, path := range paths {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files = append(files, file)
	}
	parents := map[ast.Node]ast.Node{}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				return true
			}
			for _, child := range astChildren(n) {
				parents[child] = n
			}
			return true
		})
	}
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil), Error: func(error) {}}
	if _, err := conf.Check("github.com/chenhg5/cc-connect/core", fset, files, info); err != nil {
		t.Fatalf("type-check core production files: %v", err)
	}
	violations := projectAgentBypassViolations(fset, files, info, parents)
	if len(violations) > 0 {
		t.Fatalf("agent session bypasses project scheduler:\n%s", strings.Join(violations, "\n"))
	}
}

// projectAgentBypassViolations 是生产扫描与负例共用的门禁，防止仅验证辅助分类函数。
func projectAgentBypassViolations(fset *token.FileSet, files []*ast.File, info *types.Info, parents map[ast.Node]ast.Node) []string {
	allowedStart := map[string]bool{
		"getOrCreateInteractiveStateWithContext": true,
		"runRelayProjectTask":                    true,
	}
	allowedAgentSend := map[string]bool{
		"processInteractiveMessageWithContext": true,
		"processInteractiveEventsWithContext":  true,
		"cmdPs":                                true,
		"runCompress":                          true,
		"drainPendingMessages":                 true,
		"runRelayProjectTask":                  true,
	}
	var violations []string
	for _, file := range files {
		// 包级方法值也不能成为以后绕过调度器的可调用变量。
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			for cur := parents[sel]; cur != nil; cur = parents[cur] {
				if _, ok := cur.(*ast.FuncDecl); ok {
					return true
				}
			}
			selection := info.Selections[sel]
			if selection == nil {
				return true
			}
			obj := selection.Obj()
			if isProjectAgentRunnerMethod(selection) || (obj.Name() == "StartSession" && isStartSessionSignature(obj.Type())) || (obj.Name() == "Send" && isAgentSessionSendSignature(obj.Type())) {
				violations = append(violations, fmt.Sprintf("%s: package-level sensitive method escape %s", fset.Position(sel.Pos()), obj.Name()))
			}
			return true
		})
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				selection := info.Selections[sel]
				if selection == nil {
					return true
				}
				obj := selection.Obj()
				if obj == nil {
					return true
				}
				start := obj.Name() == "StartSession" && isStartSessionSignature(obj.Type())
				send := obj.Name() == "Send" && isAgentSessionSendSignature(obj.Type())
				runner := isProjectAgentRunnerMethod(selection)
				if !start && !send && !runner {
					return true
				}
				pos := fset.Position(sel.Pos()).String()
				call, direct := parents[sel].(*ast.CallExpr)
				if !direct || call.Fun != sel {
					violations = append(violations, fmt.Sprintf("%s: %s takes sensitive method value %s", pos, fn.Name.Name, obj.Name()))
					return true
				}
				if start && !allowedStart[fn.Name.Name] {
					violations = append(violations, fmt.Sprintf("%s: %s calls Agent.StartSession", pos, fn.Name.Name))
				}
				if send && !allowedAgentSend[fn.Name.Name] {
					violations = append(violations, fmt.Sprintf("%s: %s calls AgentSession.Send", pos, fn.Name.Name))
				}
				if runner {
					// 旧 wrapper 只允许其顶层函数体内的同步直接转调，不能从闭包或 go/defer 逃逸。
					wrapperForward := (fn.Name.Name == "processInteractiveMessage" || fn.Name.Name == "processInteractiveMessageWith" || (fn.Name.Name == "processInteractiveEventsWithContext" && obj.Name() == "runCompress")) && isSynchronousFunctionBody(call, fn, parents)
					if !wrapperForward && !isInsideProjectTaskRun(call, parents, info) {
						violations = append(violations, fmt.Sprintf("%s: %s calls %s outside synchronous ProjectTask.Run", pos, fn.Name.Name, obj.Name()))
					}
				}
				return true
			})
		}
	}
	return violations
}

func isStartSessionSignature(typ types.Type) bool {
	sig, ok := typ.(*types.Signature)
	return ok && sig.Params().Len() == 2 && sig.Results().Len() == 2 && sig.Params().At(1).Type().String() == "string"
}

func isAgentSessionSendSignature(typ types.Type) bool {
	sig, ok := typ.(*types.Signature)
	if !ok || sig.Params().Len() != 4 || sig.Results().Len() != 1 {
		return false
	}
	return sig.Params().At(0).Type().String() == "string" && sig.Params().At(1).Type().String() == "string"
}

// isProjectAgentRunnerMethod 用类型对象绑定 Engine 方法，避免同名平台/测试方法误报。
func isProjectAgentRunnerMethod(selection *types.Selection) bool {
	switch selection.Obj().Name() {
	case "processInteractiveMessage", "processInteractiveMessageWith", "processInteractiveMessageWithContext", "runRelayProjectTask", "runCompress":
	default:
		return false
	}
	typ := selection.Recv()
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	named, ok := types.Unalias(typ).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Name() == "Engine" && obj.Pkg() != nil && obj.Pkg().Path() == "github.com/chenhg5/cc-connect/core" && obj.Pkg().Scope().Lookup("Engine") == obj
}

// 仅支持 ProjectTask.Run 字段内直接同步调用 runner；任何嵌套闭包、go/defer 或敏感方法值都拒绝。
// 这是受支持语法门禁，不是任意 higher-order Go 程序的形式化执行证明。
func isInsideProjectTaskRun(n ast.Node, parents map[ast.Node]ast.Node, info *types.Info) bool {
	for cur := n; cur != nil; cur = parents[cur] {
		switch node := cur.(type) {
		case *ast.GoStmt, *ast.DeferStmt:
			return false
		case *ast.FuncLit:
			kv, ok := parents[node].(*ast.KeyValueExpr)
			if !ok {
				return false
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || key.Name != "Run" || kv.Value != node {
				return false
			}
			composite, ok := parents[kv].(*ast.CompositeLit)
			if !ok {
				return false
			}
			typ := info.TypeOf(composite)
			if typ == nil {
				return false
			}
			named, ok := types.Unalias(typ).(*types.Named)
			if !ok {
				return false
			}
			obj := named.Obj()
			return obj.Name() == "ProjectTask" && obj.Pkg() != nil && obj.Pkg().Path() == "github.com/chenhg5/cc-connect/core" && obj.Pkg().Scope().Lookup("ProjectTask") == obj
		case *ast.FuncDecl:
			return false
		}
	}
	return false
}

func isSynchronousFunctionBody(n ast.Node, fn *ast.FuncDecl, parents map[ast.Node]ast.Node) bool {
	for cur := n; cur != nil; cur = parents[cur] {
		switch cur.(type) {
		case *ast.FuncLit, *ast.GoStmt, *ast.DeferStmt:
			return false
		}
		if cur == fn {
			return true
		}
	}
	return false
}

func astChildren(n ast.Node) []ast.Node {
	var children []ast.Node
	ast.Inspect(n, func(child ast.Node) bool {
		if child != nil && child != n {
			children = append(children, child)
			return false
		}
		return true
	})
	return children
}

func exprString(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return exprString(v.X) + "." + v.Sel.Name
	default:
		return fmt.Sprintf("%T", expr)
	}
}

type captureEngineScheduler struct {
	mu       sync.Mutex
	tasks    []ProjectTask
	statuses []SubmitStatus
	onSubmit func(ProjectTask) SubmitResult
	stopped  bool
}

func (s *captureEngineScheduler) Submit(task ProjectTask) SubmitResult {
	if s.onSubmit != nil {
		return s.onSubmit(task)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)
	return SubmitResult{Status: SubmitStarted}
}
func (s *captureEngineScheduler) SubmitOnlyIfIdle(task ProjectTask) SubmitResult {
	return s.Submit(task)
}
func (s *captureEngineScheduler) TryBeginLocalMutation(context.Context) (func(), bool) {
	return func() {}, true
}
func (s *captureEngineScheduler) Snapshot() SchedulerSnapshot { return SchedulerSnapshot{} }
func (s *captureEngineScheduler) CancelSessionContext(context.Context, string, CancelMode) CancelResult {
	return CancelResult{}
}
func (s *captureEngineScheduler) Stop(context.Context) { s.stopped = true }
func (s *captureEngineScheduler) taskAt(t *testing.T, index int) ProjectTask {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		if len(s.tasks) > index {
			task := s.tasks[index]
			s.mu.Unlock()
			return task
		}
		s.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for captured task %d", index)
	return ProjectTask{}
}

func TestNewEngineUsesInjectedSchedulerOptionsIdentity(t *testing.T) {
	// Catches: NewEngine computes an injected scheduler option but still assigns a
	// freshly-created scheduler, masking main wiring and fake-scheduler tests.
	cases := []struct {
		name string
		opt  func(EngineScheduler) EngineOption
	}{
		{name: "neutral WithScheduler", opt: WithScheduler},
		{name: "compat WithProjectScheduler", opt: WithProjectScheduler},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			injected := &captureEngineScheduler{}
			e := NewEngine("test", &stubAgent{}, []Platform{&stubPlatformEngine{n: "test"}}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish, tc.opt(injected))
			if e.projectScheduler != injected {
				t.Fatalf("NewEngine projectScheduler = %T %p, want injected %p", e.projectScheduler, e.projectScheduler, injected)
			}
			_ = e.Stop()
			if !injected.stopped {
				t.Fatal("Engine.Stop did not stop injected scheduler")
			}
		})
	}
}

func TestNewEngineWithNilSchedulerOptionKeepsDefaultProjectSchedulerAndStopCleanup(t *testing.T) {
	// Catches: the neutral scheduler option accidentally disables the default
	// ProjectScheduler path or makes Engine.Stop skip accepted task cleanup.
	e := NewEngine("test", &stubAgent{}, []Platform{&stubPlatformEngine{n: "test"}}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish, WithScheduler(nil))
	scheduler, ok := e.projectScheduler.(*ProjectScheduler)
	if !ok {
		t.Fatalf("NewEngine default scheduler = %T, want *ProjectScheduler", e.projectScheduler)
	}

	started := make(chan struct{})
	cleaned := make(chan struct{}, 1)
	releaseRun := make(chan struct{})
	result := scheduler.Submit(ProjectTask{
		ID:         "default-stop-cleanup",
		SessionKey: "test:default",
		Run: func(ctx context.Context) TaskResult {
			close(started)
			select {
			case <-releaseRun:
				return TaskResult{State: TaskStateCompleted}
			case <-ctx.Done():
				return TaskResult{State: TaskStateCancelled, Err: ctx.Err()}
			}
		},
		Cleanup: func() { cleaned <- struct{}{} },
	})
	if result.Status != SubmitStarted {
		t.Fatalf("Submit status = %s, want %s", result.Status, SubmitStarted)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for default scheduler task to start")
	}

	if err := e.Stop(); err != nil {
		t.Fatalf("Engine.Stop() error = %v", err)
	}
	close(releaseRun)
	select {
	case <-cleaned:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for default scheduler cleanup")
	}
}

func TestProactiveEntriesSubmitCompleteProjectTaskContract(t *testing.T) {
	// Catches: timer/heartbeat/relay/cron entries submitting incomplete tasks
	// (missing source/session/run/cleanup/onfinish) or using the wrong source.
	platform := &projectTriggerPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	agent := newProjectTriggerAgent()
	cases := []struct {
		name       string
		wantSource AgentTriggerSource
		wantKey    string
		invoke     func(*Engine) <-chan error
	}{
		{name: "timer", wantSource: TaskSourceTimer, wantKey: "test:timer:user", invoke: func(e *Engine) <-chan error {
			done := make(chan error, 1)
			go func() {
				done <- e.ExecuteTimerJob(&TimerJob{ID: "tm-contract", Project: "test", SessionKey: "test:timer:user", Prompt: "timer"})
			}()
			return done
		}},
		{name: "heartbeat", wantSource: TaskSourceHeartbeat, wantKey: "test:heartbeat:user", invoke: func(e *Engine) <-chan error {
			done := make(chan error, 1)
			go func() { done <- e.ExecuteHeartbeat("test:heartbeat:user", "heartbeat", true) }()
			return done
		}},
		{name: "relay", wantSource: TaskSourceRelay, wantKey: "relay:source:test:relay", invoke: func(e *Engine) <-chan error {
			done := make(chan error, 1)
			go func() {
				_, err := e.HandleRelay(context.Background(), "source", "test:relay:user", "relay")
				done <- err
			}()
			return done
		}},
		{name: "cron", wantSource: TaskSourceCron, wantKey: "test:cron:user", invoke: func(e *Engine) <-chan error {
			done := make(chan error, 1)
			go func() {
				done <- e.ExecuteCronJob(&CronJob{ID: "cr-contract", Project: "test", SessionKey: "test:cron:user", Prompt: "cron"})
			}()
			return done
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheduler := &captureEngineScheduler{}
			e := NewEngine("test", agent, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish, WithProjectScheduler(scheduler))
			done := tc.invoke(e)
			task := scheduler.taskAt(t, 0)
			if task.Source != tc.wantSource || task.SessionKey != tc.wantKey {
				t.Fatalf("task Source/SessionKey = %q/%q, want %q/%q", task.Source, task.SessionKey, tc.wantSource, tc.wantKey)
			}
			if task.Run == nil || task.Cleanup == nil || task.OnFinish == nil {
				t.Fatalf("task callbacks Run=%v Cleanup=%v OnFinish=%v, want all non-nil", task.Run != nil, task.Cleanup != nil, task.OnFinish != nil)
			}
			task.Cleanup()
			task.OnFinish(TaskResult{State: TaskStateCompleted})
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("entry returned error after completed OnFinish: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("entry did not wait on and observe OnFinish")
			}
			_ = e.Stop()
		})
	}
}

func TestCustomCommandAndSkillSubmitInteractiveProjectTasks(t *testing.T) {
	// Catches: custom command / skill prompt paths calling processInteractiveMessageWith
	// directly instead of entering the project FIFO as TaskSourceInteractive.
	scheduler := &captureEngineScheduler{}
	platform := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish, WithProjectScheduler(scheduler))
	msg := &Message{SessionKey: "test:cmd:user", Platform: "test", MessageID: "cmd1", UserID: "u", UserName: "user", Content: "/cmd", ReplyCtx: "r"}
	e.executeCustomCommand(platform, msg, &CustomCommand{Name: "cmd", Prompt: "custom prompt"}, nil)
	customTask := scheduler.taskAt(t, 0)
	if customTask.Source != TaskSourceInteractive || customTask.SessionKey != "test:cmd:user" || customTask.Run == nil || customTask.Cleanup == nil {
		t.Fatalf("custom task = %+v, want interactive task with run/cleanup", customTask)
	}
	msg2 := &Message{SessionKey: "test:skill:user", Platform: "test", MessageID: "skill1", UserID: "u", UserName: "user", Content: "/skill", ReplyCtx: "r2"}
	e.executeSkill(platform, msg2, &Skill{Name: "skill", Prompt: "do skill"}, nil)
	skillTask := scheduler.taskAt(t, 1)
	if skillTask.Source != TaskSourceInteractive || skillTask.SessionKey != "test:skill:user" || skillTask.Run == nil || skillTask.Cleanup == nil {
		t.Fatalf("skill task = %+v, want interactive task with run/cleanup", skillTask)
	}
	_ = e.Stop()
}

func TestRelayTerminalResultOverridesPartialSuccess(t *testing.T) {
	// Catches: HandleRelay ignoring scheduler terminal state and returning a stale
	// state.final success after cancellation/timeout/failure.
	scheduler := &captureEngineScheduler{}
	scheduler.onSubmit = func(task ProjectTask) SubmitResult {
		scheduler.mu.Lock()
		scheduler.tasks = append(scheduler.tasks, task)
		scheduler.mu.Unlock()
		go task.OnFinish(TaskResult{State: TaskStateTimedOut, Err: context.DeadlineExceeded})
		return SubmitResult{Status: SubmitStarted}
	}
	e := NewEngine("test", &stubAgent{}, []Platform{&stubPlatformEngine{n: "test"}}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish, WithProjectScheduler(scheduler))
	resp, err := e.HandleRelay(context.Background(), "source", "test:relay:user", "relay")
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("HandleRelay() err = %v, want deadline exceeded terminal error", err)
	}
	if resp != "" {
		t.Fatalf("HandleRelay() response = %q, want empty without partial", resp)
	}
	_ = e.Stop()
}

func TestProjectTaskRunAncestryCheckerFixtures(t *testing.T) {
	// Catches: the old round-1 function allowlist would have allowed a wrapper
	// call anywhere inside a broad entry function. The ancestry checker must
	// distinguish ProjectTask.Run literals from ordinary function bodies.
	const src = `package core
import "context"
type Engine struct{}
type TaskResult struct{}
type ProjectTask struct { Run func(context.Context) TaskResult }
func (*Engine) processInteractiveMessageWithContext(context.Context,any,any,any,any,any,string,string,string,uint64) {}
func (e *Engine) handleMessageBypass() { e.processInteractiveMessageWithContext(context.Background(), nil, nil, nil, nil, nil, "", "", "", 0) }
func (e *Engine) handleMessageRun() { _ = ProjectTask{Run: func(ctx context.Context) TaskResult { e.processInteractiveMessageWithContext(ctx, nil, nil, nil, nil, nil, "", "", "", 0); return TaskResult{} }} }
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	parents := map[ast.Node]ast.Node{}
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			return true
		}
		for _, child := range astChildren(n) {
			parents[child] = n
		}
		return true
	})
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	if _, err := conf.Check("github.com/chenhg5/cc-connect/core", fset, []*ast.File{file}, info); err != nil {
		t.Fatal(err)
	}
	seenBypass := false
	seenRun := false
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "processInteractiveMessageWithContext" {
			return true
		}
		inside := isInsideProjectTaskRun(call, parents, info)
		pos := fset.Position(call.Pos()).Line
		switch pos {
		case 7:
			seenBypass = true
			if inside {
				t.Fatal("bypass wrapper call was incorrectly treated as inside ProjectTask.Run")
			}
		case 8:
			seenRun = true
			if !inside {
				t.Fatal("Run literal wrapper call was not recognized as inside ProjectTask.Run")
			}
		}
		return true
	})
	if !seenBypass || !seenRun {
		t.Fatalf("fixture calls seenBypass=%v seenRun=%v", seenBypass, seenRun)
	}
}

func TestTypedGateDistinguishesReceiverRenameFromPlatformSend(t *testing.T) {
	const src = `package fixture
import "context"
type AgentSession interface { Send(string, string, []int, []int) error }
type Platform interface { Send(context.Context, any, string) error }
func badReceiverRename(renamed AgentSession) { _ = renamed.Send("", "", nil, nil) }
func goodPlatformSend(p Platform) { _ = p.Send(context.Background(), nil, "ok") }
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil), Error: func(error) {}}
	if _, err := conf.Check("fixture", fset, []*ast.File{file}, info); err != nil {
		t.Fatal(err)
	}
	var agentSends, platformSends int
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Send" {
			return true
		}
		selection := info.Selections[sel]
		if selection == nil {
			return true
		}
		if isAgentSessionSendSignature(selection.Obj().Type()) {
			agentSends++
		} else {
			platformSends++
		}
		return true
	})
	if agentSends != 1 || platformSends != 1 {
		t.Fatalf("typed Send classification agent=%d platform=%d, want 1/1", agentSends, platformSends)
	}
}

func TestProjectAgentBypassGateSupportedSyntax(t *testing.T) {
	const prefix = `package core
import "context"
type TaskResult struct{}
type ProjectTask struct { Run func(context.Context) TaskResult }
type fakeTask struct { Run func(context.Context) TaskResult }
type AgentSession interface { Send(string,string,[]int,[]int) error }
type Platform interface { Send(context.Context,any,string) error }
type Engine struct{}
func (*Engine) processInteractiveMessageWithContext(context.Context) {}
`
	cases := []struct {
		name, body string
		reject     bool
	}{
		{"sync_real_task", `func handleMessage(e *Engine) { _ = ProjectTask{Run:func(ctx context.Context) TaskResult { e.processInteractiveMessageWithContext(ctx); return TaskResult{} }} }`, false},
		{"platform_send", `func good(p Platform) { _ = p.Send(context.Background(), nil, "ok") }`, false},
		{"fake_task", `func handleMessage(e *Engine) { _ = fakeTask{Run:func(ctx context.Context) TaskResult { e.processInteractiveMessageWithContext(ctx); return TaskResult{} }} }`, true},
		{"package_method_value", `var escaped = (*Engine).processInteractiveMessageWithContext`, true},
		{"wrapper_method_value", `func handleMessage(e *Engine) { f := e.processInteractiveMessageWithContext; f(context.Background()) }`, true},
		{"agent_method_value", `func runCompress(s AgentSession) { f := s.Send; _ = f("","",nil,nil) }`, true},
		{"parameter_escape", `func forward(f func(context.Context)) {} ; func handleMessage(e *Engine) { forward(e.processInteractiveMessageWithContext) }`, true},
		{"nested_go", `func handleMessage(e *Engine) { _ = ProjectTask{Run:func(ctx context.Context) TaskResult { go func(){ e.processInteractiveMessageWithContext(ctx) }(); return TaskResult{} }} }`, true},
		{"direct_go", `func handleMessage(e *Engine) { _ = ProjectTask{Run:func(ctx context.Context) TaskResult { go e.processInteractiveMessageWithContext(ctx); return TaskResult{} }} }`, true},
		{"nested_defer", `func handleMessage(e *Engine) { _ = ProjectTask{Run:func(ctx context.Context) TaskResult { defer func(){ e.processInteractiveMessageWithContext(ctx) }(); return TaskResult{} }} }`, true},
		{"saved_closure", `var saved func(); func handleMessage(e *Engine) { _ = ProjectTask{Run:func(ctx context.Context) TaskResult { saved = func(){ e.processInteractiveMessageWithContext(ctx) }; return TaskResult{} }} }`, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture.go", prefix+tt.body, 0)
			if err != nil {
				t.Fatal(err)
			}
			parents := map[ast.Node]ast.Node{}
			ast.Inspect(file, func(n ast.Node) bool {
				if n != nil {
					for _, child := range astChildren(n) {
						parents[child] = n
					}
				}
				return true
			})
			info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
			conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
			if _, err := conf.Check("github.com/chenhg5/cc-connect/core", fset, []*ast.File{file}, info); err != nil {
				t.Fatal(err)
			}
			violations := projectAgentBypassViolations(fset, []*ast.File{file}, info, parents)
			if (len(violations) > 0) != tt.reject {
				t.Fatalf("reject=%v want=%v violations=%v", len(violations) > 0, tt.reject, violations)
			}
		})
	}
}
