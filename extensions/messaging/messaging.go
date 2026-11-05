// Package messaging connects a durable [core.Runtime] to chat apps, so an
// agent can reach a person and a person can reach an agent.
//
// A [Router] follows the runtime-wide change feed ([core.Runtime.WaitChanges]).
// When a run pauses on a durable approval or question, the router posts a
// notice to every configured [Channel]. Approve and Deny buttons, and replies
// to a notice, come back through [Router.Handle], which checks the sender
// against [Approvers] and calls [core.RunHandle.ResolveWait] with the wait's
// action digest. When the wait settles, by chat or by any other path, the
// router edits the notice to show the outcome.
//
// A plain message that is not a reply to a notice can start a run through an
// optional [Route], as the next turn of a durable conversation per chat, and
// the run's final output is posted back to that chat.
//
// The router keeps its own small records (notice tokens, posted messages,
// feed cursor, sent replies) in the [core.Store] it is given, usually the
// runtime's own store, so a restart neither loses nor reposts notices.
// Platform adapters live in subpackages, for example
// [github.com/taylortyler-1/automata/extensions/messaging/telegram].
package messaging

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/taylortyler-1/automata/core"
)

// Errors returned by [Router.Handle]. Channels show them to the sender.
var (
	// ErrNotApprover rejects an action or message from someone who is not in
	// [Approvers] for that channel.
	ErrNotApprover = errors.New("you are not allowed to answer this request")
	// ErrUnknownNotice reports a button or reply whose notice is unknown or
	// no longer kept.
	ErrUnknownNotice = errors.New("this request is no longer active")
	// ErrAlreadySettled reports an answer to a wait that is no longer
	// pending: answered, expired or cancelled.
	ErrAlreadySettled = errors.New("this request was already settled")
	// ErrInvalidAction reports an event the router can never apply, such as
	// a button on a question or an unknown action or channel.
	ErrInvalidAction = errors.New("this action is not supported")
)

// Handled reports whether a [Router.Handle] result is final for the event,
// so the channel should confirm the delivery to the platform: success, or an
// error that retrying cannot change (the sender is not an approver, the
// notice is unknown or settled, or the action is invalid). Any other error is
// transient, for example a store failure, and the channel must leave the
// event to be redelivered.
func Handled(err error) bool {
	return err == nil || errors.Is(err, ErrNotApprover) || errors.Is(err, ErrUnknownNotice) ||
		errors.Is(err, ErrAlreadySettled) || errors.Is(err, ErrInvalidAction)
}

// MessageRef identifies one posted chat message.
type MessageRef struct {
	Channel string `json:"channel"`
	Chat    string `json:"chat"`
	ID      string `json:"id"`
}

// Notice is a durable wait rendered for a person. Channels render it from
// these fields only, never from model text other than the wait's Prompt.
type Notice struct {
	// Token is the opaque value a channel puts on its buttons. It maps to the
	// wait inside the router's store and is not a credential.
	Token  string        `json:"token"`
	Kind   core.WaitKind `json:"kind"`
	RunID  string        `json:"run_id"`
	WaitID string        `json:"wait_id"`
	// Agent is the run's definition ID.
	Agent  string `json:"agent"`
	Tool   string `json:"tool"`
	Prompt string `json:"prompt,omitempty"`
	Target string `json:"target,omitempty"`
	// Arguments is the tool call's JSON input, indented and cut to a
	// readable size.
	Arguments string `json:"arguments"`
	// Digest is the start of the action digest, shown so a person can match
	// the notice to an audit record.
	Digest    string    `json:"digest,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

// Settlement is the outcome a channel shows on a settled notice.
type Settlement struct {
	State    core.WaitState `json:"state"`
	Decision core.Decision  `json:"decision,omitempty"`
	// Actor is who answered, as stored on the resolution.
	Actor  string `json:"actor,omitempty"`
	Reason string `json:"reason,omitempty"`
	// Answer is a question's answer as text.
	Answer string    `json:"answer,omitempty"`
	At     time.Time `json:"at,omitzero"`
}

// Outbound is a plain message to a chat.
type Outbound struct {
	Chat string
	Text string
	// ReplyTo is the ID of the message this one answers, when the channel
	// supports threading.
	ReplyTo string
}

// Channel is one chat platform. Implementations must be safe for concurrent
// use. Notify posts to the channel's configured destination.
type Channel interface {
	Name() string
	Notify(context.Context, Notice) (MessageRef, error)
	Settle(context.Context, MessageRef, Notice, Settlement) error
	Send(context.Context, Outbound) (MessageRef, error)
}

// InboundKind says what a person did.
type InboundKind string

const (
	// InboundAction is a button press on a notice.
	InboundAction InboundKind = "action"
	// InboundMessage is a text message, possibly a reply to a notice.
	InboundMessage InboundKind = "message"
)

// Button actions carried by [Inbound].Action.
const (
	ActionApprove = "approve"
	ActionDeny    = "deny"
)

// Inbound is one verified event from a channel. Channels must verify the
// platform's signature or secret before building one.
type Inbound struct {
	Channel string
	Kind    InboundKind
	// DeliveryID is the platform's unique ID for the event; a redelivered
	// event carries the same one.
	DeliveryID string
	// User is the platform-verified sender ID.
	User string
	// Name is the sender's display name, for messages only.
	Name string
	Chat string
	// MessageID is the ID of the incoming message (InboundMessage).
	MessageID string
	// Token and Action are set for InboundAction.
	Token  string
	Action string
	// Text is the message text (InboundMessage).
	Text string
	// ReplyTo is the ID of the message this one replies to, if any.
	ReplyTo string
}

// Actor returns the actor string the router records on resolutions and the
// authorizer checks: "<channel>:<user>".
func Actor(channel, user string) string { return channel + ":" + user }

// Approvers lists, per channel name, the platform user IDs allowed to answer
// waits and to message routed agents. It is the zero-config approver model:
// a static allowlist in host config.
type Approvers map[string][]string

// Allowed reports whether user may answer waits on channel.
func (a Approvers) Allowed(channel, user string) bool {
	return user != "" && slices.Contains(a[channel], user)
}

// Authorizer returns a [core.ApprovalAuthorizer] for
// [core.RuntimeConfig].Authorizer. An actor of the form "<channel>:<user>" for
// a channel listed in a is allowed only when the user is listed; any other
// actor is passed to fallback, or denied when fallback is nil. Core consults
// it when an approval is accepted and again just before dispatch, so removing
// a user from the list stops approvals they already gave from running.
func (a Approvers) Authorizer(fallback core.ApprovalAuthorizer) core.ApprovalAuthorizer {
	return core.ApprovalAuthorizerFunc(func(ctx context.Context, check core.ApprovalAuthorization) error {
		if channel, user, ok := strings.Cut(check.Actor, ":"); ok {
			if _, known := a[channel]; known {
				if a.Allowed(channel, user) {
					return nil
				}
				return core.ErrApprovalUnauthorized
			}
		}
		if fallback == nil {
			return core.ErrApprovalUnauthorized
		}
		return fallback.AuthorizeApproval(ctx, check)
	})
}

// Route starts or continues a run when an approver messages a channel
// without replying to a notice. The message text is the run's task, never
// its system prompt.
type Route struct {
	Definition core.DefinitionRef
	// Conversation admits each message as the next turn of one durable
	// conversation per chat, so the agent sees the earlier messages.
	Conversation bool
	// Deadline, when positive, bounds each run.
	Deadline time.Duration
}
