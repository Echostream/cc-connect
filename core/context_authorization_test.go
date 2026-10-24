package core

import "testing"

func TestContextAuthorizer(t *testing.T) {
	authorizer := ContextAuthorizer{
		DMAllowFrom:       "owner-1,owner-2",
		AllowChat:         "chat-a,chat-b",
		GroupMemberPolicy: "all",
	}
	tests := []struct {
		name    string
		ctx     ChatContext
		allowed bool
		reason  string
	}{
		{"owner dm", ChatContext{Kind: ChatKindDM, UserID: "owner-1"}, true, "allowed_dm"},
		{"group member dm", ChatContext{Kind: ChatKindDM, UserID: "member-1"}, false, "dm_not_allowed"},
		{"trusted group member", ChatContext{Kind: ChatKindGroup, ChatID: "chat-a", UserID: "member-1"}, true, "allowed_group"},
		{"unknown group", ChatContext{Kind: ChatKindGroup, ChatID: "chat-x", UserID: "owner-1"}, false, "chat_not_allowed"},
		{"missing chat", ChatContext{Kind: ChatKindGroup, UserID: "owner-1"}, false, "invalid_context"},
		{"missing user", ChatContext{Kind: ChatKindDM}, false, "invalid_context"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := authorizer.Authorize(tt.ctx)
			if got.Allowed != tt.allowed || got.Reason != tt.reason {
				t.Fatalf("Authorize() = %#v, want allowed=%v reason=%q", got, tt.allowed, tt.reason)
			}
		})
	}
}

func TestContextAuthorizerRejectsWildcardEmptyAndUnknownPolicy(t *testing.T) {
	dm := ChatContext{Kind: ChatKindDM, UserID: "owner"}
	group := ChatContext{Kind: ChatKindGroup, ChatID: "chat-a", UserID: "member"}
	for _, a := range []ContextAuthorizer{
		{DMAllowFrom: "*", AllowChat: "chat-a", GroupMemberPolicy: "all"},
		{DMAllowFrom: "", AllowChat: "chat-a", GroupMemberPolicy: "all"},
		{DMAllowFrom: "owner,*", AllowChat: "chat-a", GroupMemberPolicy: "all"},
	} {
		if got := a.Authorize(dm); got.Allowed || got.Reason != "dm_not_allowed" {
			t.Fatalf("unsafe DM config decision = %#v for authorizer %#v, want dm_not_allowed", got, a)
		}
	}
	for _, a := range []ContextAuthorizer{
		{DMAllowFrom: "owner", AllowChat: "*", GroupMemberPolicy: "all"},
		{DMAllowFrom: "owner", AllowChat: "", GroupMemberPolicy: "all"},
		{DMAllowFrom: "owner", AllowChat: "chat-a,*", GroupMemberPolicy: "all"},
		{DMAllowFrom: "owner", AllowChat: "chat-a", GroupMemberPolicy: "owner-only"},
	} {
		if got := a.Authorize(group); got.Allowed {
			t.Fatalf("unsafe group config allowed: decision=%#v authorizer=%#v", got, a)
		}
	}
}

func TestContextAuthorizerStrictListSemantics(t *testing.T) {
	authorizer := ContextAuthorizer{
		DMAllowFrom:       " owner-1,owner-1,,owner-2 ",
		AllowChat:         " chat-a,chat-a,,chat-b ",
		GroupMemberPolicy: "all",
	}
	tests := []struct {
		name    string
		ctx     ChatContext
		allowed bool
		reason  string
	}{
		{"trims and tolerates duplicate dm entries", ChatContext{Kind: ChatKindDM, UserID: "owner-1"}, true, "allowed_dm"},
		{"trims and tolerates duplicate group entries", ChatContext{Kind: ChatKindGroup, ChatID: "chat-a", UserID: "member-1"}, true, "allowed_group"},
		{"empty list items do not authorize empty dm user", ChatContext{Kind: ChatKindDM, UserID: ""}, false, "invalid_context"},
		{"empty list items do not authorize empty group chat", ChatContext{Kind: ChatKindGroup, ChatID: "", UserID: "member-1"}, false, "invalid_context"},
		{"dm list is not prefix match", ChatContext{Kind: ChatKindDM, UserID: "owner"}, false, "dm_not_allowed"},
		{"dm list is not substring match", ChatContext{Kind: ChatKindDM, UserID: "ner-1"}, false, "dm_not_allowed"},
		{"group list is not prefix match", ChatContext{Kind: ChatKindGroup, ChatID: "chat", UserID: "member-1"}, false, "chat_not_allowed"},
		{"group list is not substring match", ChatContext{Kind: ChatKindGroup, ChatID: "hat-a", UserID: "member-1"}, false, "chat_not_allowed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := authorizer.Authorize(tt.ctx)
			if got.Allowed != tt.allowed || got.Reason != tt.reason {
				t.Fatalf("Authorize() = %#v, want allowed=%v reason=%q", got, tt.allowed, tt.reason)
			}
		})
	}
}

func TestContextAuthorizerRejectsMalformedContext(t *testing.T) {
	authorizer := ContextAuthorizer{
		DMAllowFrom:       "owner-1",
		AllowChat:         "chat-a",
		GroupMemberPolicy: "all",
	}
	tests := []ChatContext{
		{},
		{Kind: ChatKind("channel"), ChatID: "chat-a", UserID: "owner-1"},
		{Kind: ChatKindGroup, ChatID: "chat-a"},
		{Kind: ChatKindDM, ChatID: "chat-a"},
	}
	for _, ctx := range tests {
		got := authorizer.Authorize(ctx)
		if got.Allowed || got.Reason != "invalid_context" {
			t.Fatalf("Authorize(%#v) = %#v, want invalid_context", ctx, got)
		}
	}
}

func TestContextAuthorizerUsesIndependentRulesAndCaseSensitiveExactIDs(t *testing.T) {
	authorizer := ContextAuthorizer{
		DMAllowFrom:       "Owner-1",
		AllowChat:         "chat-a",
		GroupMemberPolicy: "all",
	}
	tests := []struct {
		name    string
		ctx     ChatContext
		allowed bool
		reason  string
	}{
		{"case sensitive dm exact id", ChatContext{Kind: ChatKindDM, UserID: "owner-1"}, false, "dm_not_allowed"},
		{"case sensitive group exact id", ChatContext{Kind: ChatKindGroup, ChatID: "Chat-A", UserID: "member-1"}, false, "chat_not_allowed"},
		{"group allow chat does not grant dm", ChatContext{Kind: ChatKindDM, UserID: "chat-a"}, false, "dm_not_allowed"},
		{"dm allow user does not grant group", ChatContext{Kind: ChatKindGroup, ChatID: "Owner-1", UserID: "member-1"}, false, "chat_not_allowed"},
		{"allowed dm exact case", ChatContext{Kind: ChatKindDM, UserID: "Owner-1"}, true, "allowed_dm"},
		{"allowed group independent of user allowlist", ChatContext{Kind: ChatKindGroup, ChatID: "chat-a", UserID: "stranger"}, true, "allowed_group"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := authorizer.Authorize(tt.ctx)
			if got.Allowed != tt.allowed || got.Reason != tt.reason {
				t.Fatalf("Authorize() = %#v, want allowed=%v reason=%q", got, tt.allowed, tt.reason)
			}
		})
	}
}

func TestContextAuthorizerReasonIsLowCardinality(t *testing.T) {
	authorizer := ContextAuthorizer{
		DMAllowFrom:       "owner-1",
		AllowChat:         "chat-a",
		GroupMemberPolicy: "all",
	}
	allowedReasons := map[string]bool{
		"allowed_dm":       true,
		"allowed_group":    true,
		"invalid_context":  true,
		"dm_not_allowed":   true,
		"chat_not_allowed": true,
		"invalid_policy":   true,
	}
	for _, ctx := range []ChatContext{
		{Kind: ChatKindDM, UserID: "owner-1"},
		{Kind: ChatKindDM, UserID: "secret-user"},
		{Kind: ChatKindGroup, ChatID: "chat-a", UserID: "secret-user"},
		{Kind: ChatKindGroup, ChatID: "secret-chat", UserID: "owner-1"},
		{Kind: ChatKind("broken"), ChatID: "secret-chat", UserID: "secret-user"},
	} {
		got := authorizer.Authorize(ctx)
		if !allowedReasons[got.Reason] {
			t.Fatalf("Authorize(%#v) reason %q is not fixed low-cardinality", ctx, got.Reason)
		}
		if got.Reason == ctx.UserID || got.Reason == ctx.ChatID {
			t.Fatalf("Authorize(%#v) leaked identity in reason %q", ctx, got.Reason)
		}
	}
}
