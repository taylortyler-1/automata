// Command telegram runs a durable agent you can talk to, and that asks you
// before acting, over Telegram.
//
// Message the bot and each message becomes the next turn of one durable
// conversation. When the agent wants to publish a note it pauses on a durable
// approval: you get a message with Approve and Deny buttons, and replying to
// it with text denies it with that reason. When it needs information it asks
// a question, which you answer by replying. Runs, waits and posted notices
// live in a SQLite file, so you can stop the process while a request is
// pending, start it again, and still answer it.
//
// Setup:
//
//  1. Create a bot with @BotFather and copy its token.
//
//  2. Send the bot any message, then find your user ID by opening
//     https://api.telegram.org/bot<TOKEN>/getUpdates (message.from.id).
//
//  3. Run:
//
//     export ANTHROPIC_API_KEY=sk-ant-...
//     export TELEGRAM_BOT_TOKEN=123:abc
//     export TELEGRAM_CHAT_ID=<your user ID>
//     go run .
//
// Then message the bot: "Draft a note about today's plan and publish it."
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/taylortyler-1/automata/core"
	"github.com/taylortyler-1/automata/extensions/messaging"
	"github.com/taylortyler-1/automata/extensions/messaging/telegram"
	"github.com/taylortyler-1/automata/extensions/sqlite"
	"github.com/taylortyler-1/automata/providers/claude"
)

type publishInput struct {
	Title string `json:"title" desc:"note title"`
	Body  string `json:"body" desc:"note body in Markdown"`
}

type askInput struct {
	Question string `json:"question" desc:"what to ask the owner"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	chatID, err := strconv.ParseInt(os.Getenv("TELEGRAM_CHAT_ID"), 10, 64)
	if err != nil {
		log.Fatal("set TELEGRAM_CHAT_ID to your Telegram user ID")
	}
	model := os.Getenv("ANTHROPIC_MODEL")
	if model == "" {
		model = "claude-sonnet-5-5"
	}
	dir := os.Getenv("DATA_DIR")
	if dir == "" {
		dir = "."
	}

	bot, err := telegram.New(telegram.Config{Token: os.Getenv("TELEGRAM_BOT_TOKEN"), ChatID: chatID})
	if err != nil {
		log.Fatal(err)
	}
	// Only you may approve, answer, or talk to the agent.
	approvers := messaging.Approvers{bot.Name(): {strconv.FormatInt(chatID, 10)}}

	store, err := sqlite.Open(ctx, filepath.Join(dir, "telegram-agent.db"))
	if err != nil {
		log.Fatal(err)
	}
	runtime, err := core.NewRuntime(ctx, core.RuntimeConfig{Store: store, Authorizer: approvers.Authorizer(nil)})
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()

	notesDir := filepath.Join(dir, "notes")
	publish := core.WithDurableWait(
		core.Func("publish_note", "Publish a note to the notes folder. The owner must approve it.",
			func(_ context.Context, in publishInput) (string, error) {
				if err := os.MkdirAll(notesDir, 0o755); err != nil {
					return "", err
				}
				path := filepath.Join(notesDir, fmt.Sprintf("%s.md", sanitize(in.Title)))
				if err := os.WriteFile(path, []byte("# "+in.Title+"\n\n"+in.Body+"\n"), 0o644); err != nil {
					return "", err
				}
				return "published to " + path, nil
			}),
		core.DurableWaitPolicy{
			Kind: core.WaitApproval,
			Prompt: func(raw json.RawMessage) (string, error) {
				var in publishInput
				if err := json.Unmarshal(raw, &in); err != nil {
					return "", err
				}
				return "Publish the note “" + in.Title + "”?", nil
			},
			Target:        func(json.RawMessage) (string, error) { return notesDir, nil },
			PolicyContext: "telegram-example/v1",
		})
	ask := core.WithDurableWait(
		core.Func("ask_owner", "Ask the owner a question and wait for the answer.",
			func(context.Context, askInput) (string, error) { return "", fmt.Errorf("answered by the owner") }),
		core.DurableWaitPolicy{
			Kind: core.WaitQuestion,
			Prompt: func(raw json.RawMessage) (string, error) {
				var in askInput
				err := json.Unmarshal(raw, &in)
				return in.Question, err
			},
		})

	agent, err := core.New(claude.New(model, os.Getenv("ANTHROPIC_API_KEY")), core.AgentConfig{
		SystemPrompt: "You are a helpful assistant reached over Telegram. Keep replies short. " +
			"Use ask_owner when you need information, and publish_note to publish; publishing waits for the owner's approval. " +
			"If a publish is denied, read the reason and revise.",
		MaxTurns: 12,
		Tools:    []core.Tool{publish, ask},
	})
	if err != nil {
		log.Fatal(err)
	}
	def, err := runtime.Register("assistant", "v1", agent)
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Recover(ctx); err != nil {
		log.Fatal(err)
	}

	router, err := messaging.New(messaging.Config{
		Runtime: runtime, Store: store, Channels: []messaging.Channel{bot}, Approvers: approvers,
		Route: &messaging.Route{Definition: def, Conversation: true},
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := router.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("router stopped: %v", err)
			stop()
		}
	}()
	log.Printf("listening on Telegram as chat %d; Ctrl-C to stop", chatID)
	if err := bot.Poll(ctx, router); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func sanitize(title string) string {
	out := []rune{}
	for _, r := range title {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		case r == ' ':
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "note"
	}
	return string(out)
}
