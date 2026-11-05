# Messaging

`extensions/messaging` lets a durable agent reach you in a chat app and lets
you reach the agent. It is standard library only. Telegram is the first
channel (`extensions/messaging/telegram`); Slack and others plug into the same
`Channel` interface.

- **Approvals.** When a run pauses on a `core.WaitApproval`, the router posts
  the exact call (tool, target, arguments, a short action digest) with
  Approve and Deny buttons. A tap calls `ResolveWait` with the wait's action
  digest, so a stale message can never approve a different call. Replying to
  the message with text denies it with that text as the reason, which the
  model sees and can act on. Approving always takes the button.
- **Questions.** A `core.WaitQuestion` is posted as a message you reply to;
  the reply text becomes the answer.
- **Settling.** When a wait is answered, expires, or is cancelled, by chat or
  by any other path such as `extensions/httpserver`, the message is edited to
  show the outcome and its buttons are removed.
- **Wake-ups.** With a `Route`, a plain message from an approver starts a run,
  or with `Conversation: true` the next turn of one durable conversation per
  chat. The final output is posted back as a reply, once.

The router follows `Runtime.WaitChanges`, so it sees waits from every run
without the host tracking run IDs. Its own records (tokens, posted message
IDs, open notices, feed cursor, sent replies) live in the store you pass,
normally the runtime's own. On restart it replays the feed from its cursor
and reconciles every notice still open. On first start, or when retention
has pruned the feed past its cursor, it resynchronizes from current state
before saving a new cursor: it posts notices for waiting runs, settles open
notices whose waits were resolved, expired or cancelled (or whose run is
gone), and posts the output of retained finished routed runs that have no
reply record. A change whose events were pruned is reconciled from the run's
snapshot.

**Delivery is at least once.** Sending to Telegram and writing the local
record are not atomic, so a crash after a message is sent but before it is
recorded sends that notice, edit, or reply again on restart. Nothing is
skipped, but a rare duplicate is possible.

## Who may answer

`Approvers` is a static allowlist of platform user IDs per channel. Install
its authorizer on the runtime: core consults it when an approval is accepted
and again just before the tool runs, so removing someone from the list stops
approvals they already gave. Actors are recorded as `<channel>:<user>`, for
example `telegram:42`. Other actors (an HTTP API, a CLI) are passed to the
fallback authorizer you give it.

Messages from anyone not on the list are ignored, and a button tap from them
gets an error toast. Inbound text becomes a run's task, never its system
prompt.

## Host wiring

```go
bot, _ := telegram.New(telegram.Config{Token: os.Getenv("TELEGRAM_BOT_TOKEN"), ChatID: myID})
approvers := messaging.Approvers{"telegram": {strconv.FormatInt(myID, 10)}}

store, _ := sqlite.Open(ctx, "/data/runtime.db")
rt, _ := core.NewRuntime(ctx, core.RuntimeConfig{Store: store, Authorizer: approvers.Authorizer(nil)})
def, _ := rt.Register("assistant", "v1", agent)
_ = rt.Recover(ctx)

router, _ := messaging.New(messaging.Config{
	Runtime: rt, Store: store, Channels: []messaging.Channel{bot}, Approvers: approvers,
	Route: &messaging.Route{Definition: def, Conversation: true}, // optional
})
go router.Run(ctx)
bot.Poll(ctx, router) // or mux.Handle("/telegram", bot.WebhookHandler(router))
```

`examples/telegram` is a runnable version with an approval tool and a
question tool.

## Telegram notes

- Long polling (`Poll`) needs no public URL, which suits a laptop or a single
  Fly Machine. `WebhookHandler` checks the `X-Telegram-Bot-Api-Secret-Token`
  header before parsing; register the webhook with the same secret. Telegram
  allows only one of the two at a time, and `Poll` deletes any webhook first.
- Button `callback_data` carries `a:` or `d:` and a 22 character random token,
  well under Telegram's 64 byte limit. The token points at the wait in the
  router's store and is not a credential.
- An update is confirmed only after the router has handled it. A transient
  failure, such as the store being briefly unavailable, leaves it
  unconfirmed: `Poll` waits and fetches again from that update before any
  later one, and the webhook answers 503 so Telegram redelivers it. Refusals
  that retrying cannot change (an unlisted sender, an unknown or settled
  notice) count as handled. Answering a wait and admitting a message are both
  idempotent, so redelivery is safe, and a failed confirmation toast never
  causes one.

## Not yet

- Risk levels from the design doc (hosted sign-in link for high-risk
  actions). Every approval is "standard" today: buttons from a listed user.
- Choice buttons for questions, and Slack, Discord and Teams channels.
- Steering a run that is already in progress (needs core signals); until then
  a message sent while a conversation turn is running gets "I'm still working
  on your last message."
- Pruning the router's records when runs are pruned.
