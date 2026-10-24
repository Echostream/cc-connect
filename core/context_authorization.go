package core

import "strings"

type ChatKind string

const (
	ChatKindDM    ChatKind = "dm"
	ChatKindGroup ChatKind = "group"
)

type ChatContext struct {
	Kind   ChatKind
	ChatID string
	UserID string
}

type AuthorizationDecision struct {
	Allowed bool
	Reason  string
}

type ContextAuthorizer struct {
	DMAllowFrom       string
	AllowChat         string
	GroupMemberPolicy string
}

func (a ContextAuthorizer) Authorize(ctx ChatContext) AuthorizationDecision {
	switch ctx.Kind {
	case ChatKindDM:
		if ctx.UserID == "" {
			return AuthorizationDecision{Reason: "invalid_context"}
		}
		if !strictListContains(a.DMAllowFrom, ctx.UserID) {
			return AuthorizationDecision{Reason: "dm_not_allowed"}
		}
		return AuthorizationDecision{Allowed: true, Reason: "allowed_dm"}
	case ChatKindGroup:
		if ctx.ChatID == "" || ctx.UserID == "" {
			return AuthorizationDecision{Reason: "invalid_context"}
		}
		if a.GroupMemberPolicy != "all" {
			return AuthorizationDecision{Reason: "invalid_policy"}
		}
		if !strictListContains(a.AllowChat, ctx.ChatID) {
			return AuthorizationDecision{Reason: "chat_not_allowed"}
		}
		return AuthorizationDecision{Allowed: true, Reason: "allowed_group"}
	default:
		return AuthorizationDecision{Reason: "invalid_context"}
	}
}

func strictListContains(list, want string) bool {
	list = strings.TrimSpace(list)
	if list == "" || want == "" {
		return false
	}
	items := strings.Split(list, ",")
	for _, item := range items {
		if strings.TrimSpace(item) == "*" {
			return false
		}
	}
	for _, item := range items {
		if strings.TrimSpace(item) == want {
			return true
		}
	}
	return false
}
