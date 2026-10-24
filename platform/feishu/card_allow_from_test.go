package feishu

import (
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	callback "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
)

// newCardAuthTestPlatform constructs an interactivePlatform with strict
// conversation authorization and a handler channel that lets the test assert
// whether a card action triggered a downstream message dispatch.
func newCardAuthTestPlatform(t *testing.T) (*interactivePlatform, chan *core.Message) {
	t.Helper()
	platformAny, err := New(map[string]any{
		"app_id":              "cli_xxx",
		"app_secret":          "secret",
		"dm_allow_from":       "ou_owner",
		"allow_chat":          "oc_test_chat",
		"group_member_policy": "all",
		"enable_feishu_card":  true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ip, ok := platformAny.(*interactivePlatform)
	if !ok {
		t.Fatalf("platform type = %T, want *interactivePlatform", platformAny)
	}
	msgCh := make(chan *core.Message, 4)
	ip.handler = func(_ core.Platform, msg *core.Message) {
		msgCh <- msg
	}
	return ip, msgCh
}

// waitNoMessage fails the test if the handler channel receives any message
// within the given timeout. Used to assert that an unauthorized card action
// was silently dropped before reaching dispatchCoreMessage.
func waitNoMessage(t *testing.T, ch <-chan *core.Message, timeout time.Duration, prefix string) {
	t.Helper()
	select {
	case msg := <-ch:
		t.Fatalf("%s: expected no dispatched message, got %+v", prefix, msg)
	case <-time.After(timeout):
		// ok — handler was not invoked
	}
}

// waitAnyMessage fails the test if no message reaches the handler within the
// given timeout.
func waitAnyMessage(t *testing.T, ch <-chan *core.Message, timeout time.Duration, prefix string) *core.Message {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(timeout):
		t.Fatalf("%s: expected dispatched message, got none", prefix)
		return nil
	}
}

func cardActionEventInChat(chatID, openID, action string) *callback.CardActionTriggerEvent {
	return &callback.CardActionTriggerEvent{
		Event: &callback.CardActionTriggerRequest{
			Operator: &callback.Operator{OpenID: openID},
			Action:   &callback.CallBackAction{Value: map[string]any{"action": action}},
			Context:  &callback.Context{OpenChatID: chatID, OpenMessageID: "om_test_message"},
		},
	}
}

func cardActionEvent(openID, action string) *callback.CardActionTriggerEvent {
	return cardActionEventInChat("oc_test_chat", openID, action)
}

func TestOnCardAction_ContextAuthorizationBlocksUntrustedChatActions(t *testing.T) {
	tests := []struct {
		name   string
		action string
	}{
		{"cmd", "cmd:/help"},
		{"perm allow", "perm:allow"},
		{"perm deny", "perm:deny"},
		{"perm allow all", "perm:allow_all"},
		{"askq", "askq:option_a"},
		{"act", "act:/foo"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ip, msgCh := newCardAuthTestPlatform(t)
			_, err := ip.onCardAction(cardActionEventInChat("oc_untrusted", "ou_owner", tc.action))
			if err != nil {
				t.Fatalf("onCardAction() error = %v", err)
			}
			waitNoMessage(t, msgCh, 200*time.Millisecond, tc.action+" from untrusted chat")
		})
	}
}

func TestOnCardAction_ContextAuthorizationBlocksUntrustedChatNavigation(t *testing.T) {
	ip, _ := newCardAuthTestPlatform(t)
	navCalled := make(chan struct{}, 1)
	ip.cardNavHandler = func(action string, sessionKey string) *core.Card {
		select {
		case navCalled <- struct{}{}:
		default:
		}
		return nil
	}
	_, err := ip.onCardAction(cardActionEventInChat("oc_untrusted", "ou_owner", "nav:/foo"))
	if err != nil {
		t.Fatalf("onCardAction() error = %v", err)
	}
	select {
	case <-navCalled:
		t.Fatal("nav:/foo from untrusted chat should not invoke cardNavHandler")
	case <-time.After(200 * time.Millisecond):
		// ok
	}
}

func TestOnCardAction_ContextAuthorizationBlocksEmptyOperator(t *testing.T) {
	ip, msgCh := newCardAuthTestPlatform(t)
	ev := &callback.CardActionTriggerEvent{
		Event: &callback.CardActionTriggerRequest{
			Operator: nil,
			Action:   &callback.CallBackAction{Value: map[string]any{"action": "cmd:/help"}},
			Context:  &callback.Context{OpenChatID: "oc_test_chat", OpenMessageID: "om_test_message"},
		},
	}
	_, err := ip.onCardAction(ev)
	if err != nil {
		t.Fatalf("onCardAction() error = %v", err)
	}
	waitNoMessage(t, msgCh, 200*time.Millisecond, "cmd:/help with no Operator (empty userID)")
}

func TestOnCardAction_ContextAuthorizationAllowsTrustedGroupMemberCmd(t *testing.T) {
	ip, msgCh := newCardAuthTestPlatform(t)
	_, err := ip.onCardAction(cardActionEvent("ou_member", "cmd:/help"))
	if err != nil {
		t.Fatalf("onCardAction() error = %v", err)
	}
	msg := waitAnyMessage(t, msgCh, 2*time.Second, "cmd:/help from trusted group member")
	if msg.Content != "/help" {
		t.Fatalf("dispatched content = %q, want %q", msg.Content, "/help")
	}
	if msg.UserID != "ou_member" {
		t.Fatalf("dispatched UserID = %q, want %q", msg.UserID, "ou_member")
	}
}

func TestOnCardAction_ContextAuthorizationAllowsTrustedGroupMemberPermAllow(t *testing.T) {
	ip, msgCh := newCardAuthTestPlatform(t)
	_, err := ip.onCardAction(cardActionEvent("ou_member", "perm:allow"))
	if err != nil {
		t.Fatalf("onCardAction() error = %v", err)
	}
	msg := waitAnyMessage(t, msgCh, 2*time.Second, "perm:allow from trusted group member")
	if !msg.IsPermissionResponse {
		t.Fatalf("dispatched IsPermissionResponse = false, want true")
	}
	if msg.Content != "allow" {
		t.Fatalf("dispatched content = %q, want %q", msg.Content, "allow")
	}
}

func TestOnCardAction_ContextAuthorizationRejectsMissingChatContext(t *testing.T) {
	tests := []struct {
		name   string
		action string
	}{
		{"cmd", "cmd:/help"},
		{"perm", "perm:allow"},
		{"askq", "askq:option_a"},
		{"act", "act:/foo"},
		{"nav", "nav:/foo"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ip, msgCh := newCardAuthTestPlatform(t)
			navCalled := make(chan struct{}, 1)
			ip.cardNavHandler = func(action string, sessionKey string) *core.Card {
				navCalled <- struct{}{}
				return nil
			}
			_, err := ip.onCardAction(&callback.CardActionTriggerEvent{Event: &callback.CardActionTriggerRequest{
				Operator: &callback.Operator{OpenID: "ou_owner"},
				Action:   &callback.CallBackAction{Value: map[string]any{"action": tc.action}},
				Context:  &callback.Context{OpenMessageID: "om_missing_chat"},
			}})
			if err != nil {
				t.Fatalf("onCardAction() error = %v", err)
			}
			waitNoMessage(t, msgCh, 200*time.Millisecond, tc.action+" without OpenChatID")
			select {
			case <-navCalled:
				t.Fatal("card without OpenChatID must not invoke cardNavHandler")
			default:
			}
		})
	}
}

func TestOnCardAction_ContextAuthorizationRejectsMalformedAndUntrustedCallbackContext(t *testing.T) {
	tests := []struct {
		name   string
		chatID string
		openID string
		action string
	}{
		{"missing message context", "", "ou_owner", "cmd:/help"},
		{"malformed no operator", "oc_test_chat", "", "cmd:/help"},
		{"untrusted group callback", "oc_untrusted", "ou_owner", "perm:allow_all"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ip, msgCh := newCardAuthTestPlatform(t)
			ev := cardActionEventInChat(tc.chatID, tc.openID, tc.action)
			if tc.openID == "" {
				ev.Event.Operator = nil
			}
			_, err := ip.onCardAction(ev)
			if err != nil {
				t.Fatalf("onCardAction() error = %v", err)
			}
			waitNoMessage(t, msgCh, 200*time.Millisecond, tc.name)
		})
	}
}

func TestOnCardAction_ContextAuthorizationWildcardRejected(t *testing.T) {
	_, err := New(map[string]any{
		"app_id":              "cli_xxx",
		"app_secret":          "secret",
		"dm_allow_from":       "*",
		"allow_chat":          "oc_test_chat",
		"group_member_policy": "all",
		"enable_feishu_card":  true,
	})
	if err == nil || !strings.Contains(err.Error(), "dm_allow_from") {
		t.Fatalf("New() error = %v, want dm_allow_from wildcard rejection", err)
	}
}

func TestOnCardAction_ContextAuthorizationRejectsLegacyAllowFrom(t *testing.T) {
	_, err := New(map[string]any{
		"app_id":              "cli_xxx",
		"app_secret":          "secret",
		"allow_from":          "ou_legacy",
		"dm_allow_from":       "ou_owner",
		"allow_chat":          "oc_test_chat",
		"group_member_policy": "all",
		"enable_feishu_card":  true,
	})
	if err == nil || !strings.Contains(err.Error(), "allow_from") {
		t.Fatalf("New() error = %v, want allow_from rejection", err)
	}
}
