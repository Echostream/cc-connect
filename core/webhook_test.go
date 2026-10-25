package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type failingWebhookReplyPlatform struct {
	stubPlatformEngine
}

func (p *failingWebhookReplyPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	return nil, errors.New("reconstruct failed for " + sessionKey)
}

func TestWebhookServer_AuthBearer(t *testing.T) {
	ws := NewWebhookServer(0, "my-secret", "/hook")
	r := httptest.NewRequest(http.MethodPost, "/hook", nil)
	r.Header.Set("Authorization", "Bearer my-secret")
	if !ws.authenticate(r) {
		t.Error("expected auth to succeed with correct Bearer token")
	}
	r.Header.Set("Authorization", "Bearer wrong")
	if ws.authenticate(r) {
		t.Error("expected auth to fail with wrong Bearer token")
	}
}

func TestWebhookServer_AuthHeader(t *testing.T) {
	ws := NewWebhookServer(0, "tok123", "/hook")
	r := httptest.NewRequest(http.MethodPost, "/hook", nil)
	r.Header.Set("X-Webhook-Token", "tok123")
	if !ws.authenticate(r) {
		t.Error("expected auth to succeed with X-Webhook-Token")
	}
}

func TestWebhookServer_AuthQuery(t *testing.T) {
	ws := NewWebhookServer(0, "qsecret", "/hook")
	r := httptest.NewRequest(http.MethodPost, "/hook?token=qsecret", nil)
	if !ws.authenticate(r) {
		t.Error("expected auth to succeed with query token")
	}
}

func TestWebhookServer_NoTokenRequired(t *testing.T) {
	ws := NewWebhookServer(0, "", "/hook")
	r := httptest.NewRequest(http.MethodPost, "/hook", nil)
	if !ws.authenticate(r) {
		t.Error("expected auth to pass when no token configured")
	}
}

func TestWebhookServer_HandleHook_MethodNotAllowed(t *testing.T) {
	ws := NewWebhookServer(0, "", "/hook")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/hook", nil)
	ws.handleHook(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestWebhookServer_HandleHook_Unauthorized(t *testing.T) {
	ws := NewWebhookServer(0, "secret", "/hook")
	w := httptest.NewRecorder()
	body, _ := json.Marshal(WebhookRequest{SessionKey: "tg:1:1", Prompt: "hi"})
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	ws.handleHook(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

func TestWebhookServer_HandleHook_Validation(t *testing.T) {
	ws := NewWebhookServer(0, "", "/hook")

	tests := []struct {
		name string
		body WebhookRequest
		code int
	}{
		{"missing session_key", WebhookRequest{Prompt: "hi"}, http.StatusBadRequest},
		{"missing prompt and exec", WebhookRequest{SessionKey: "tg:1:1"}, http.StatusBadRequest},
		{"both prompt and exec", WebhookRequest{SessionKey: "tg:1:1", Prompt: "hi", Exec: "ls"}, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			body, _ := json.Marshal(tt.body)
			r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
			ws.handleHook(w, r)
			if w.Code != tt.code {
				t.Errorf("expected %d, got %d: %s", tt.code, w.Code, w.Body.String())
			}
		})
	}
}

func TestWebhookServer_StopWaitsForAcceptedPrompt(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	sess := &blockingTimerSession{started: started, release: release}
	platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &resultAgent{session: sess}, []Platform{platform}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	ws := NewWebhookServer(0, "", "/hook")
	ws.RegisterEngine("test", e)
	body, _ := json.Marshal(WebhookRequest{Project: "test", SessionKey: "test:webhook:user", Prompt: "webhook prompt", Silent: true})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	ws.handleHook(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("handleHook status = %d, body=%s", w.Code, w.Body.String())
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("webhook prompt did not start")
	}

	stopped := make(chan struct{})
	go func() {
		ws.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned before accepted webhook prompt completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after accepted webhook prompt completed")
	}
}

func TestWebhookServer_HandleHookRejectsPromptWhenSchedulerStopped(t *testing.T) {
	platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
	e.projectScheduler.Stop(context.Background())
	t.Cleanup(func() { _ = e.Stop() })

	ws := NewWebhookServer(0, "", "/hook")
	ws.RegisterEngine("test", e)
	body, _ := json.Marshal(WebhookRequest{Project: "test", SessionKey: "test:webhook:user", Prompt: "webhook prompt", Silent: true})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	ws.handleHook(w, r)
	if w.Code == http.StatusOK {
		t.Fatalf("handleHook returned accepted after scheduler stopped: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestWebhookServer_HandleHookRejectsShellBeforeAcceptedWhenAdmissionInvalid(t *testing.T) {
	t.Run("platform not found", func(t *testing.T) {
		platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
		e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
		t.Cleanup(func() { _ = e.Stop() })

		ws := NewWebhookServer(0, "", "/hook")
		ws.RegisterEngine("test", e)
		body, _ := json.Marshal(WebhookRequest{Project: "test", SessionKey: "missing:webhook:user", Exec: "true", Silent: true})
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
		ws.handleHook(w, r)
		if w.Code == http.StatusOK {
			t.Fatalf("handleHook accepted shell with missing platform: status=%d body=%s", w.Code, w.Body.String())
		}
	})

	t.Run("reply context reconstruction fails", func(t *testing.T) {
		platform := &failingWebhookReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
		e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
		t.Cleanup(func() { _ = e.Stop() })

		ws := NewWebhookServer(0, "", "/hook")
		ws.RegisterEngine("test", e)
		body, _ := json.Marshal(WebhookRequest{Project: "test", SessionKey: "test:webhook:user", Exec: "true", Silent: true})
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
		ws.handleHook(w, r)
		if w.Code == http.StatusOK {
			t.Fatalf("handleHook accepted shell after reconstruct failure: status=%d body=%s", w.Code, w.Body.String())
		}
	})

	t.Run("work dir missing", func(t *testing.T) {
		platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
		e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
		t.Cleanup(func() { _ = e.Stop() })

		ws := NewWebhookServer(0, "", "/hook")
		ws.RegisterEngine("test", e)
		body, _ := json.Marshal(WebhookRequest{Project: "test", SessionKey: "test:webhook:user", Exec: "true", WorkDir: filepath.Join(t.TempDir(), "missing"), Silent: true})
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
		ws.handleHook(w, r)
		if w.Code == http.StatusOK {
			t.Fatalf("handleHook accepted shell with missing work_dir: status=%d body=%s", w.Code, w.Body.String())
		}
	})
}

func TestWebhookServer_StopCancelsAcceptedShell(t *testing.T) {
	platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	ws := NewWebhookServer(0, "", "/hook")
	ws.RegisterEngine("test", e)
	body, _ := json.Marshal(WebhookRequest{Project: "test", SessionKey: "test:webhook:user", Exec: "sleep 2", Silent: true})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	ws.handleHook(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("handleHook status = %d, body=%s", w.Code, w.Body.String())
	}

	stopped := make(chan struct{})
	go func() {
		ws.Stop()
		close(stopped)
	}()

	returnedQuickly := false
	select {
	case <-stopped:
		returnedQuickly = true
	case <-time.After(300 * time.Millisecond):
	}
	if returnedQuickly {
		return
	}

	select {
	case <-stopped:
		t.Fatal("Stop waited for shell command instead of cancelling accepted shell work")
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return after shell command should have exited")
	}
}

func TestWebhookServer_StopCancelsAcceptedShellProcessTree(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process group cancellation semantics differ on Windows")
	}
	platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	workDir := t.TempDir()
	startedPath := filepath.Join(workDir, "started")
	delayedPath := filepath.Join(workDir, "delayed")
	ws := NewWebhookServer(0, "", "/hook")
	ws.RegisterEngine("test", e)
	body, _ := json.Marshal(WebhookRequest{
		Project:    "test",
		SessionKey: "test:webhook:user",
		Exec:       "(touch started; sleep 1; touch delayed) & sleep 5 & wait",
		WorkDir:    workDir,
		Silent:     true,
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	ws.handleHook(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("handleHook status = %d, body=%s", w.Code, w.Body.String())
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(startedPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shell command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopped := make(chan struct{})
	go func() {
		ws.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(700 * time.Millisecond):
		t.Fatal("Stop did not cancel shell child process tree promptly")
	}

	time.Sleep(1200 * time.Millisecond)
	if _, err := os.Stat(delayedPath); err == nil {
		t.Fatal("shell child process survived Stop and produced delayed output")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat delayed marker: %v", err)
	}
}

func TestWebhookServer_StopCancelsEscapedShellDescendant(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process tree cancellation semantics differ on Windows")
	}
	platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	workDir := t.TempDir()
	startedPath := filepath.Join(workDir, "started")
	delayedPath := filepath.Join(workDir, "delayed")
	ws := NewWebhookServer(0, "", "/hook")
	ws.RegisterEngine("test", e)
	execScript := `python3 - <<'PY'
import os, time
pid = os.fork()
if pid == 0:
    os.setsid()
    pid2 = os.fork()
    if pid2 == 0:
        with open("started", "w") as f:
            f.write("started")
        time.sleep(1)
        with open("delayed", "w") as f:
            f.write("delayed")
    os._exit(0)
os._exit(0)
PY
sleep 5`
	body, _ := json.Marshal(WebhookRequest{
		Project:    "test",
		SessionKey: "test:webhook:user",
		Exec:       execScript,
		WorkDir:    workDir,
		Silent:     true,
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	ws.handleHook(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("handleHook status = %d, body=%s", w.Code, w.Body.String())
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(startedPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("escaped shell descendant did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopped := make(chan struct{})
	go func() {
		ws.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(700 * time.Millisecond):
		t.Fatal("Stop did not cancel escaped shell descendant promptly")
	}

	time.Sleep(1200 * time.Millisecond)
	if _, err := os.Stat(delayedPath); err == nil {
		t.Fatal("escaped shell descendant survived Stop and produced delayed output")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat delayed marker: %v", err)
	}
}

func TestWebhookServer_StopDoesNotBlockOnEscapedShellAfterTokenCleared(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process tree cancellation semantics differ on Windows")
	}
	platform := &timerReplyPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &resultAgent{session: &stubAgentSession{}}, []Platform{platform}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })

	workDir := t.TempDir()
	startedPath := filepath.Join(workDir, "started")
	ws := NewWebhookServer(0, "", "/hook")
	ws.RegisterEngine("test", e)
	execScript := `python3 - <<'PY'
import os, sys, time
pid = os.fork()
if pid == 0:
    os.setsid()
    if "CC_CONNECT_WEBHOOK_SHELL_TOKEN" in os.environ:
        del os.environ["CC_CONNECT_WEBHOOK_SHELL_TOKEN"]
    with open("started", "w") as f:
        f.write("started")
    sys.stdout.write("holding stdout open\\n")
    sys.stdout.flush()
    time.sleep(5)
    os._exit(0)
os._exit(0)
PY
sleep 5`
	body, _ := json.Marshal(WebhookRequest{
		Project:    "test",
		SessionKey: "test:webhook:user",
		Exec:       execScript,
		WorkDir:    workDir,
		Silent:     true,
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	ws.handleHook(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("handleHook status = %d, body=%s", w.Code, w.Body.String())
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(startedPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("escaped shell descendant did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopped := make(chan struct{})
	go func() {
		ws.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("Stop blocked on escaped shell descendant after token was cleared")
	}
}

func TestWebhookServer_DefaultValues(t *testing.T) {
	ws := NewWebhookServer(0, "", "")
	if ws.port != 9111 {
		t.Errorf("expected default port 9111, got %d", ws.port)
	}
	if ws.path != "/hook" {
		t.Errorf("expected default path /hook, got %s", ws.path)
	}
}
