package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// WebhookServer exposes an HTTP endpoint for external systems
// (git hooks, CI/CD, file watchers, etc.) to trigger agent or shell actions.
type WebhookServer struct {
	port     int
	token    string
	path     string
	server   *http.Server
	engines  map[string]*Engine
	mu       sync.RWMutex
	stopped  bool
	ctx      context.Context
	cancel   context.CancelFunc
	inflight sync.WaitGroup
}

// WebhookRequest is the JSON body for POST /hook.
type WebhookRequest struct {
	Event      string `json:"event,omitempty"`    // event name for logging (e.g. "git:commit")
	Project    string `json:"project,omitempty"`  // target project; optional if single project
	SessionKey string `json:"session_key"`        // target session key (required)
	Prompt     string `json:"prompt,omitempty"`   // agent prompt (mutually exclusive with exec)
	Exec       string `json:"exec,omitempty"`     // shell command (mutually exclusive with prompt)
	WorkDir    string `json:"work_dir,omitempty"` // working dir for exec
	Silent     bool   `json:"silent,omitempty"`   // suppress notification
	Payload    any    `json:"payload,omitempty"`  // arbitrary extra data; appended to prompt context
}

func NewWebhookServer(port int, token, path string) *WebhookServer {
	if port <= 0 {
		port = 9111
	}
	if path == "" {
		path = "/hook"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &WebhookServer{
		port:    port,
		token:   token,
		path:    path,
		engines: make(map[string]*Engine),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (ws *WebhookServer) RegisterEngine(name string, e *Engine) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.engines[name] = e
}

func (ws *WebhookServer) Start() {
	mux := http.NewServeMux()
	mux.HandleFunc(ws.path, ws.handleHook)

	addr := fmt.Sprintf(":%d", ws.port)
	ws.server = &http.Server{Addr: addr, Handler: mux}

	go func() {
		slog.Info("webhook: server started", "addr", addr, "path", ws.path)
		if err := ws.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("webhook: server error", "error", err)
		}
	}()
}

func (ws *WebhookServer) CloseAdmission() {
	ws.mu.Lock()
	if ws.stopped {
		ws.mu.Unlock()
		return
	}
	ws.stopped = true
	if ws.cancel != nil {
		ws.cancel()
	}
	server := ws.server
	ws.mu.Unlock()
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
}

func (ws *WebhookServer) Wait() {
	ws.inflight.Wait()
}

func (ws *WebhookServer) Stop() {
	ws.CloseAdmission()
	ws.Wait()
}

func (ws *WebhookServer) handleHook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	if !ws.authenticate(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req WebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.SessionKey == "" {
		http.Error(w, "session_key is required", http.StatusBadRequest)
		return
	}
	if req.Prompt == "" && req.Exec == "" {
		http.Error(w, "either prompt or exec is required", http.StatusBadRequest)
		return
	}
	if req.Prompt != "" && req.Exec != "" {
		http.Error(w, "prompt and exec are mutually exclusive", http.StatusBadRequest)
		return
	}

	engine, err := ws.resolveEngine(req.Project)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	eventName := req.Event
	if eventName == "" {
		eventName = "webhook"
	}

	slog.Info("webhook: received",
		"event", eventName,
		"project", req.Project,
		"session_key", req.SessionKey,
		"has_prompt", req.Prompt != "",
		"has_exec", req.Exec != "",
	)

	if req.Exec != "" {
		prepared, err := ws.prepareShell(engine, req, eventName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if !ws.beginInFlight() {
			http.Error(w, engine.i18n.T(MsgWebhookServerStopped), http.StatusServiceUnavailable)
			return
		}
		go func() {
			defer ws.finishInFlight()
			ws.executeShell(prepared)
		}()
	} else {
		prompt := req.Prompt
		if req.Payload != nil {
			if payloadJSON, err := json.Marshal(req.Payload); err == nil {
				prompt += "\n\nContext:\n```json\n" + string(payloadJSON) + "\n```"
			}
		}
		if !ws.beginInFlight() {
			http.Error(w, engine.i18n.T(MsgWebhookServerStopped), http.StatusServiceUnavailable)
			return
		}
		submitted, err := ws.submitPrompt(engine, req.SessionKey, prompt, req.Silent, eventName)
		if err != nil {
			ws.finishInFlight()
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		go func() {
			defer ws.finishInFlight()
			if err := engine.waitSubmittedProjectAgentTask(ws.ctx, submitted); err != nil {
				slog.Warn("webhook: prompt execution failed", "event", eventName, "session_key", req.SessionKey, "error", err)
				return
			}
			slog.Info("webhook: prompt completed", "event", eventName, "session_key", req.SessionKey)
		}()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "accepted",
		"event":  eventName,
	})
}

func (ws *WebhookServer) beginInFlight() bool {
	ws.mu.Lock()
	if ws.stopped {
		ws.mu.Unlock()
		return false
	}
	ws.inflight.Add(1)
	ws.mu.Unlock()
	return true
}

func (ws *WebhookServer) finishInFlight() {
	ws.inflight.Done()
}

func (ws *WebhookServer) runAsync(fn func()) bool {
	if !ws.beginInFlight() {
		return false
	}
	go func() {
		defer ws.finishInFlight()
		fn()
	}()
	return true
}

func (ws *WebhookServer) authenticate(r *http.Request) bool {
	if ws.token == "" {
		return true
	}

	// Check Authorization: Bearer <token>
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") {
			got := auth[7:]
			return subtle.ConstantTimeCompare([]byte(got), []byte(ws.token)) == 1
		}
	}

	// Check X-Webhook-Token header
	if tok := r.Header.Get("X-Webhook-Token"); tok != "" {
		return subtle.ConstantTimeCompare([]byte(tok), []byte(ws.token)) == 1
	}

	// Check query parameter as fallback
	if tok := r.URL.Query().Get("token"); tok != "" {
		return subtle.ConstantTimeCompare([]byte(tok), []byte(ws.token)) == 1
	}

	return false
}

func (ws *WebhookServer) resolveEngine(project string) (*Engine, error) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	if project != "" {
		e, ok := ws.engines[project]
		if !ok {
			return nil, fmt.Errorf("project %q not found", project)
		}
		return e, nil
	}

	if len(ws.engines) == 1 {
		for _, e := range ws.engines {
			return e, nil
		}
	}

	return nil, fmt.Errorf("project is required (multiple projects configured)")
}

func (ws *WebhookServer) executePrompt(engine *Engine, sessionKey, prompt string, silent bool, event string) {
	if _, err := ws.submitPrompt(engine, sessionKey, prompt, silent, event); err != nil {
		slog.Warn("webhook: prompt not accepted", "event", event, "session_key", sessionKey, "error", err)
	}
}

func (ws *WebhookServer) executePromptAndWait(engine *Engine, sessionKey, prompt string, silent bool, event string) {
	submitted, err := ws.submitPrompt(engine, sessionKey, prompt, silent, event)
	if err != nil {
		slog.Warn("webhook: prompt not accepted", "event", event, "session_key", sessionKey, "error", err)
		return
	}
	if err := engine.waitSubmittedProjectAgentTask(ws.ctx, submitted); err != nil {
		slog.Warn("webhook: prompt execution failed", "event", event, "session_key", sessionKey, "error", err)
		return
	}
	slog.Info("webhook: prompt completed", "event", event, "session_key", sessionKey)
}

func (ws *WebhookServer) submitPrompt(engine *Engine, sessionKey, prompt string, silent bool, event string) (*submittedProjectAgentTask, error) {
	platformName := ""
	if idx := strings.Index(sessionKey, ":"); idx > 0 {
		platformName = sessionKey[:idx]
	}

	var targetPlatform Platform
	for _, p := range engine.platforms {
		if p.Name() == platformName {
			targetPlatform = p
			break
		}
	}
	if targetPlatform == nil {
		err := fmt.Errorf("platform %q not found", platformName)
		slog.Error("webhook: platform not found", "event", event, "platform", platformName)
		return nil, err
	}

	rc, ok := targetPlatform.(ReplyContextReconstructor)
	if !ok {
		err := fmt.Errorf("platform %q does not support proactive messaging", platformName)
		slog.Error("webhook: platform does not support proactive messaging", "event", event, "platform", platformName)
		return nil, err
	}

	replyCtx, err := rc.ReconstructReplyCtx(sessionKey)
	if err != nil {
		slog.Error("webhook: reconstruct reply context failed", "event", event, "error", err)
		return nil, err
	}

	notifyStarted := func() {}
	if !silent {
		notifyStarted = func() { engine.send(targetPlatform, replyCtx, fmt.Sprintf("🪝 %s", event)) }
	}

	msg := &Message{
		SessionKey: sessionKey,
		Platform:   platformName,
		UserID:     "webhook",
		UserName:   "webhook",
		Content:    prompt,
		ReplyCtx:   replyCtx,
	}

	submitted, err := engine.submitProjectAgentTask(projectAgentTaskRequest{
		ID:         newProducerTaskID("webhook:" + sessionKey),
		Source:     TaskSourceInteractive,
		SessionKey: sessionKey,
		Platform:   targetPlatform,
		Message:    *msg,
		Agent:      engine.agent,
		Sessions:   engine.sessions,
		OnStart:    notifyStarted,
		Proactive:  true,
	})
	if err != nil {
		slog.Warn("webhook: prompt not accepted", "event", event, "session_key", sessionKey, "error", err)
		if !silent && errors.Is(err, errProjectSchedulerBusy) {
			engine.send(targetPlatform, replyCtx, fmt.Sprintf("🪝 ⚠️ project busy, skipped: %s", event))
		}
		return nil, err
	}

	slog.Info("webhook: prompt accepted", "event", event, "session_key", sessionKey, "task_id", submitted.taskID)
	return submitted, nil
}

const (
	webhookShellTimeout         = 5 * time.Minute
	webhookShellCancelWaitLimit = 500 * time.Millisecond
	webhookShellOutputDrainWait = 50 * time.Millisecond
	webhookShellOutputReadLimit = 64 * 1024
)

type preparedWebhookShell struct {
	ws             *WebhookServer
	engine         *Engine
	req            WebhookRequest
	event          string
	sessionKey     string
	targetPlatform Platform
	replyCtx       any
	workDir        string
}

func (ws *WebhookServer) prepareShell(engine *Engine, req WebhookRequest, event string) (*preparedWebhookShell, error) {
	sessionKey := req.SessionKey
	platformName := ""
	if idx := strings.Index(sessionKey, ":"); idx > 0 {
		platformName = sessionKey[:idx]
	}

	var targetPlatform Platform
	for _, p := range engine.platforms {
		if p.Name() == platformName {
			targetPlatform = p
			break
		}
	}
	if targetPlatform == nil {
		err := fmt.Errorf("platform %q not found", platformName)
		slog.Error("webhook: platform not found for shell exec", "event", event, "platform", platformName)
		return nil, err
	}

	rc, ok := targetPlatform.(ReplyContextReconstructor)
	if !ok {
		err := fmt.Errorf("platform %q does not support proactive messaging", platformName)
		slog.Error("webhook: platform does not support proactive messaging", "event", event, "platform", platformName)
		return nil, err
	}

	replyCtx, err := rc.ReconstructReplyCtx(sessionKey)
	if err != nil {
		slog.Error("webhook: reconstruct reply context failed", "event", event, "error", err)
		return nil, err
	}

	workDir := req.WorkDir
	if workDir == "" {
		if wd, ok := engine.agent.(interface{ GetWorkDir() string }); ok {
			workDir = wd.GetWorkDir()
		}
	}
	if workDir == "" {
		workDir, _ = os.Getwd()
	}
	info, err := os.Stat(workDir)
	if err != nil {
		return nil, fmt.Errorf("work_dir %q is not accessible: %w", workDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("work_dir %q is not a directory", workDir)
	}

	return &preparedWebhookShell{
		ws:             ws,
		engine:         engine,
		req:            req,
		event:          event,
		sessionKey:     sessionKey,
		targetPlatform: targetPlatform,
		replyCtx:       replyCtx,
		workDir:        workDir,
	}, nil
}

func (ws *WebhookServer) executeShell(prepared *preparedWebhookShell) {
	engine := prepared.engine
	req := prepared.req
	event := prepared.event
	sessionKey := prepared.sessionKey
	targetPlatform := prepared.targetPlatform
	replyCtx := prepared.replyCtx

	if !req.Silent {
		engine.send(targetPlatform, replyCtx, fmt.Sprintf("🪝 %s: `%s`", event, truncateStr(req.Exec, 60)))
	}

	ctx, cancel := context.WithTimeout(ws.ctx, webhookShellTimeout)
	defer cancel()

	cmd := shellExecCommandNoContext(engine.shell, engine.shellFlag, engine.shellProfile, req.Exec)
	cmd.Dir = prepared.workDir
	output, execErr := runWebhookShellCommand(ctx, cmd, newWebhookShellToken())

	result := strings.TrimSpace(string(output))

	if ctx.Err() == context.Canceled {
		return
	}
	if ctx.Err() == context.DeadlineExceeded {
		engine.send(targetPlatform, replyCtx, fmt.Sprintf("🪝 ⚠️ timeout: `%s`", truncateStr(req.Exec, 60)))
		return
	}

	if execErr != nil {
		msg := fmt.Sprintf("🪝 ❌ `%s`", truncateStr(req.Exec, 60))
		if result != "" {
			msg += "\n\n" + truncateStr(result, 3000)
		}
		msg += "\n\nerror: " + execErr.Error()
		engine.send(targetPlatform, replyCtx, msg)
	} else {
		if result == "" {
			result = "(no output)"
		}
		engine.send(targetPlatform, replyCtx, fmt.Sprintf("🪝 ✅ `%s`\n\n%s", truncateStr(req.Exec, 60), truncateStr(result, 3000)))
	}

	slog.Info("webhook: shell executed", "event", event, "session_key", sessionKey, "success", execErr == nil)
}

func newWebhookShellToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

func runWebhookShellCommand(ctx context.Context, cmd *exec.Cmd, token string) ([]byte, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	cmd.Stdout = writer
	cmd.Stderr = writer

	var output bytes.Buffer
	copyDone := make(chan struct{})
	go func() {
		defer close(copyDone)
		_, _ = io.Copy(&boundedBufferWriter{buf: &output, limit: webhookShellOutputReadLimit}, reader)
	}()

	finishOutput := func() []byte {
		_ = writer.Close()
		timer := time.NewTimer(webhookShellOutputDrainWait)
		defer timer.Stop()
		select {
		case <-copyDone:
		case <-timer.C:
			_ = reader.Close()
			<-copyDone
		}
		return output.Bytes()
	}

	prepareWebhookShellCommand(cmd, token)
	if err := ctx.Err(); err != nil {
		_ = writer.Close()
		<-copyDone
		return output.Bytes(), err
	}
	if err := cmd.Start(); err != nil {
		_ = writer.Close()
		<-copyDone
		return output.Bytes(), err
	}
	_ = writer.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return finishOutput(), err
	case <-ctx.Done():
		terminateWebhookShellCommand(cmd, token)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.NewTimer(webhookShellCancelWaitLimit)
		defer deadline.Stop()
		for {
			select {
			case err := <-done:
				if err != nil {
					return finishOutput(), err
				}
				return finishOutput(), ctx.Err()
			case <-ticker.C:
				terminateWebhookShellCommand(cmd, token)
			case <-deadline.C:
				terminateWebhookShellCommand(cmd, token)
				return finishOutput(), ctx.Err()
			}
		}
	}
}

type boundedBufferWriter struct {
	buf   *bytes.Buffer
	limit int
}

func (w *boundedBufferWriter) Write(p []byte) (int, error) {
	if w.limit > 0 && w.buf.Len() < w.limit {
		remaining := w.limit - w.buf.Len()
		if len(p) > remaining {
			_, _ = w.buf.Write(p[:remaining])
		} else {
			_, _ = w.buf.Write(p)
		}
	}
	return len(p), nil
}
