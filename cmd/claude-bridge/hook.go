package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/asd-noor/claude-bridge/internal/broker"
	"github.com/asd-noor/claude-bridge/internal/config"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// hookEventName is the Claude Code hook event this subcommand answers.
const hookEventName = "UserPromptSubmit"

// hookInput is the subset of the UserPromptSubmit hook stdin payload we use.
type hookInput struct {
	CWD string `json:"cwd"`
}

// hookOutput is the UserPromptSubmit hook response that injects context.
type hookOutput struct {
	HookSpecificOutput hookSpecificOutput `json:"hookSpecificOutput"`
}

type hookSpecificOutput struct {
	HookEventName     string `json:"hookEventName"`
	AdditionalContext string `json:"additionalContext"`
}

// runHook implements `claude-bridge hook`: invoked by Claude Code's
// UserPromptSubmit hook, it drains the bridge inbox for the session running in
// the current working directory and injects any pending peer messages into the
// turn as additionalContext. It always exits 0 and stays silent when there is
// nothing to inject, so it never disrupts the user's prompt.
func runHook(cfg config.Config, logger *slog.Logger) int {
	cwd := hookCWD(logger)
	if cwd == "" {
		return 0
	}

	sessionID, ok := readSessionMap(cfg, cwd)
	if !ok {
		return 0 // no shim registered for this directory
	}

	msgs, ok := drainInbox(cfg, sessionID, logger)
	if !ok || len(msgs) == 0 {
		return 0
	}

	emitContext(formatMessages(msgs), logger)
	return 0
}

// hookCWD reads the working directory from the hook's stdin payload, falling
// back to the process cwd.
func hookCWD(logger *slog.Logger) string {
	if raw, err := io.ReadAll(os.Stdin); err == nil && len(raw) > 0 {
		var in hookInput
		if err := json.Unmarshal(raw, &in); err == nil && in.CWD != "" {
			return in.CWD
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		logger.Debug("hook getwd", "err", err)
		return ""
	}
	return cwd
}

// readSessionMap resolves the bridge session_id owning projectPath, if any.
func readSessionMap(cfg config.Config, projectPath string) (string, bool) {
	data, err := os.ReadFile(config.SessionMapPath(cfg, projectPath))
	if err != nil {
		return "", false
	}
	id := strings.TrimSpace(string(data))
	return id, id != ""
}

// drainInbox polls and clears the session's inbox. It does not auto-spawn the
// daemon: if none is running there is nothing to deliver.
func drainInbox(cfg config.Config, sessionID string, logger *slog.Logger) ([]broker.Message, bool) {
	client, err := daemonrpc.Dial(config.SockPath(cfg))
	if err != nil {
		return nil, false
	}
	defer client.Close()

	raw, err := client.CallAs(sessionID, daemonrpc.MethodPoll, struct{}{})
	if err != nil {
		logger.Debug("hook poll", "err", err)
		return nil, false
	}
	var res daemonrpc.PollResult
	if err := json.Unmarshal(raw, &res); err != nil {
		logger.Debug("hook decode", "err", err)
		return nil, false
	}
	return res.Messages, true
}

// formatMessages renders pending messages into a context block instructing the
// model how to act on and reply to them.
func formatMessages(msgs []broker.Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[claude-bridge] %d new message(s) from peer Claude Code session(s). "+
		"Reply with the send_message tool, setting in_reply_to to the message id when answering.\n", len(msgs))
	for _, m := range msgs {
		fmt.Fprintf(&b, "- id=%s from=%s", m.ID, m.From)
		if m.InReplyTo != "" {
			fmt.Fprintf(&b, " in_reply_to=%s", m.InReplyTo)
		}
		if m.ExpectsReply {
			b.WriteString(" [expects a reply]")
		}
		fmt.Fprintf(&b, ": %s\n", m.Content)
	}
	return b.String()
}

// emitContext writes the additionalContext hook response to stdout.
func emitContext(text string, logger *slog.Logger) {
	out := hookOutput{HookSpecificOutput: hookSpecificOutput{
		HookEventName:     hookEventName,
		AdditionalContext: text,
	}}
	if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
		logger.Debug("hook encode", "err", err)
	}
}
