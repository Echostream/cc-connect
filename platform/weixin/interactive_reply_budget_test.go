package weixin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type interactiveReplyBudgetAgent struct {
	session *interactiveReplyBudgetSession
}

func (a *interactiveReplyBudgetAgent) Name() string { return "interactive-reply-budget" }
func (a *interactiveReplyBudgetAgent) StartSession(context.Context, string) (core.AgentSession, error) {
	if a.session == nil {
		a.session = &interactiveReplyBudgetSession{events: make(chan core.Event, 4)}
	}
	return a.session, nil
}
func (a *interactiveReplyBudgetAgent) ListSessions(context.Context) ([]core.AgentSessionInfo, error) {
	return nil, nil
}
func (a *interactiveReplyBudgetAgent) Stop() error { return nil }

type interactiveReplyBudgetSession struct {
	events chan core.Event
}

func (s *interactiveReplyBudgetSession) Send(string, string, []core.ImageAttachment, []core.FileAttachment) error {
	s.events <- core.Event{Type: core.EventResult, Content: "interactive final answer", Done: true}
	return nil
}
func (s *interactiveReplyBudgetSession) RespondPermission(string, core.PermissionResult) error {
	return nil
}
func (s *interactiveReplyBudgetSession) Events() <-chan core.Event { return s.events }
func (s *interactiveReplyBudgetSession) CurrentSessionID() string {
	return "interactive-reply-budget-session"
}
func (s *interactiveReplyBudgetSession) Alive() bool  { return true }
func (s *interactiveReplyBudgetSession) Close() error { close(s.events); return nil }

func TestInteractiveFinalReplyBypassesPushBudget(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []sendMessageReq
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/ilink/bot/sendmessage":
			var req sendMessageReq
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode sendmessage request: %v", err)
				return
			}
			mu.Lock()
			requests = append(requests, req)
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ret":0}`))
		case "/ilink/bot/getconfig":
			_, _ = w.Write([]byte(`{"ret":0,"errcode":0,"typing_ticket":"ticket"}`))
		case "/ilink/bot/sendtyping":
			_, _ = w.Write([]byte(`{"ret":0}`))
		default:
			t.Errorf("unexpected API path: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := &Platform{
		baseURL:         server.URL,
		httpClient:      server.Client(),
		api:             newAPIClient(server.URL, "token", "", server.Client()),
		sendQuotaLimit:  1,
		sendQuotaWindow: time.Hour,
		sendQuotaTimes:  []time.Time{time.Now()},
		tokens:          map[string]string{"peer-user": "stored-token"},
		typingTickets:   make(map[string]typingTicketEntry),
	}
	agent := &interactiveReplyBudgetAgent{}
	engine := core.NewEngine("weixin-interactive-reply-budget", agent, []core.Platform{p}, "", core.LangEnglish)
	defer engine.Stop()

	engine.ReceiveMessage(p, &core.Message{
		Platform:   p.Name(),
		SessionKey: "weixin:peer-user",
		UserID:     "peer-user",
		UserName:   "Peer User",
		Content:    "hello",
		MessageID:  "msg-1",
		ReplyCtx:   &replyContext{peerUserID: "peer-user", contextToken: "ctx-token"},
	})

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		mu.Lock()
		got := len(requests)
		mu.Unlock()
		if got >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("sendmessage requests = %d, want 1; interactive final replies must bypass the exhausted push budget", got)
		case <-ticker.C:
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("sendmessage requests = %d, want exactly 1", len(requests))
	}
	items := requests[0].Msg.ItemList
	if len(items) != 1 || items[0].TextItem == nil || items[0].TextItem.Text != "interactive final answer" {
		t.Fatalf("sent item = %#v, want one text item with final answer", items)
	}
}
