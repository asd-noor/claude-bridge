package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/asd-noor/claude-bridge/internal/broker"
	"github.com/asd-noor/claude-bridge/internal/config"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// Hook event names this subcommand answers (read from the hook payload).
const (
	eventUserPromptSubmit = "UserPromptSubmit"
	eventStop             = "Stop"
)

// maxStopContinues bounds how many times the Stop hook will auto-continue a
// session to process peer messages between user turns. It is the loop guard
// that stops two auto-replying agents from ping-ponging forever; a real user
// turn resets the budget.
const maxStopContinues = 5

// hookInput is the subset of the hook stdin payload we use.
type hookInput struct {
	CWD           string `json:"cwd"`
	HookEventName string `json:"hook_event_name"`
}

// promptOutput injects context on a UserPromptSubmit turn.
type promptOutput struct {
	HookSpecificOutput promptSpecific `json:"hookSpecificOutput"`
}

type promptSpecific struct {
	HookEventName     string `json:"hookEventName"`
	AdditionalContext string `json:"additionalContext"`
}

// stopOutput blocks a Stop so the session continues and processes the messages
// carried in reason.
type stopOutput struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
}

// runHook implements `claude-bridge hook`, wired to both UserPromptSubmit and
// Stop. It resolves the bridge session owning the cwd and delivers pending peer
// messages — by injecting context on a user turn, or by continuing the turn at
// Stop so an active session keeps draining without a new prompt. It always exits
// 0 and stays silent when there is nothing to do.
func runHook(cfg config.Config, logger *slog.Logger) int {
	in := readHookInput(logger)
	if in.CWD == "" {
		return 0
	}
	sessionID, ok := readSessionMap(cfg, in.CWD)
	if !ok {
		return 0 // no shim registered for this directory
	}

	if in.HookEventName == eventStop {
		return stopHook(cfg, in.CWD, sessionID, logger)
	}
	return promptHook(cfg, in.CWD, sessionID, logger)
}

// promptHook drains the inbox and injects pending messages as additionalContext.
// A user turn also resets the Stop continue budget.
func promptHook(cfg config.Config, cwd, sessionID string, logger *slog.Logger) int {
	resetContinueBudget(cfg, cwd)

	msgs, ok := drainInbox(cfg, sessionID, logger)
	if !ok || len(msgs) == 0 {
		return 0
	}
	emit(promptOutput{HookSpecificOutput: promptSpecific{
		HookEventName:     eventUserPromptSubmit,
		AdditionalContext: formatMessages(msgs),
	}}, logger)
	return 0
}

// stopHook continues an ending turn when peer messages are pending, so an active
// session processes them without a new user prompt. The continue budget (reset
// on each user turn) caps consecutive auto-continues to break reply loops; when
// it is exhausted the inbox is left intact for the next user turn to surface.
func stopHook(cfg config.Config, cwd, sessionID string, logger *slog.Logger) int {
	if readContinueBudget(cfg, cwd) <= 0 {
		return 0 // loop guard: stop and wait for a user turn
	}

	msgs, ok := drainInbox(cfg, sessionID, logger)
	if !ok || len(msgs) == 0 {
		return 0 // nothing pending: allow the stop
	}

	decrementContinueBudget(cfg, cwd)
	emit(stopOutput{
		Decision: "block",
		Reason:   formatMessages(msgs),
	}, logger)
	return 0
}

// readHookInput parses the hook stdin payload, falling back to the process cwd.
func readHookInput(logger *slog.Logger) hookInput {
	var in hookInput
	if raw, err := io.ReadAll(os.Stdin); err == nil && len(raw) > 0 {
		_ = json.Unmarshal(raw, &in)
	}
	if in.CWD == "" {
		if cwd, err := os.Getwd(); err == nil {
			in.CWD = cwd
		} else {
			logger.Debug("hook getwd", "err", err)
		}
	}
	return in
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

// formatMessages renders pending messages into a block instructing the model how
// to act on and reply to them.
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

// continueBudgetPath is the per-cwd Stop continue-budget file.
func continueBudgetPath(cfg config.Config, cwd string) string {
	return config.SessionMapPath(cfg, cwd) + ".cont"
}

// readContinueBudget returns the remaining Stop continues, defaulting to the max
// when no budget has been recorded yet.
func readContinueBudget(cfg config.Config, cwd string) int {
	data, err := os.ReadFile(continueBudgetPath(cfg, cwd))
	if err != nil {
		return maxStopContinues
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return maxStopContinues
	}
	return n
}

// resetContinueBudget restores the full continue budget on a user turn.
func resetContinueBudget(cfg config.Config, cwd string) {
	writeContinueBudget(cfg, cwd, maxStopContinues)
}

// decrementContinueBudget records one consumed continue.
func decrementContinueBudget(cfg config.Config, cwd string) {
	writeContinueBudget(cfg, cwd, readContinueBudget(cfg, cwd)-1)
}

func writeContinueBudget(cfg config.Config, cwd string, n int) {
	_ = os.WriteFile(continueBudgetPath(cfg, cwd), []byte(strconv.Itoa(n)), pidFileMode)
}

// emit writes a hook response as JSON to stdout.
func emit(v any, logger *slog.Logger) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		logger.Debug("hook encode", "err", err)
	}
}
