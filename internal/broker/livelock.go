package broker

import (
	"slices"
	"strings"
	"time"
)

// recentWindow is how many recent message bodies per session-pair the breaker
// remembers when judging "no progress". A window of 4 catches the classic
// two-distinct-echo loop (A:"ack", B:"ok", A:"ack", …) where each body equals
// one from two messages back but never the immediately preceding one.
const recentWindow = 4

// livelockTracker trips a no-progress reply chain between a pair of sessions. It
// counts consecutive content-free exchanges (a new body that repeats a recent
// one) and trips once the run exceeds maxChain, at which point the broker stops
// pushing wakes for that pair (inboxes stay intact — poll still works). A chain
// whose content keeps changing never trips; an idle gap resets it. State is
// touched only by the broker run loop, so it needs no synchronization.
//
// ponytail: progress is inferred from changing content, not real work (the broker
// can't see tool use). Good enough for echo loops; swap normalized-equality for a
// similarity ratio if templated-but-varying echoes start slipping through.
type livelockTracker struct {
	enabled   bool
	maxChain  int
	resetIdle time.Duration
	pairs     map[string]*chainState
}

// chainState is one session-pair's running echo count.
type chainState struct {
	recent   []string // normalized recent bodies, newest last, capped at recentWindow
	repeats  int      // consecutive bodies that repeated a recent one
	lastSeen time.Time
}

// newLivelockTracker builds a tracker from cfg. A disabled tracker (or maxChain
// <= 0) never trips.
func newLivelockTracker(cfg Config) *livelockTracker {
	return &livelockTracker{
		enabled:   cfg.LivelockEnabled,
		maxChain:  cfg.LivelockMaxChain,
		resetIdle: cfg.LivelockResetIdle,
		pairs:     make(map[string]*chainState),
	}
}

// trip records msg against its session-pair and reports whether the pair is now
// livelocked and its wakes should be suppressed. An idle gap longer than
// resetIdle starts a fresh chain.
func (t *livelockTracker) trip(msg Message, now time.Time) bool {
	if t == nil || !t.enabled || t.maxChain <= 0 {
		return false
	}
	key := pairKey(msg.From, msg.To)
	cs := t.pairs[key]
	if cs == nil || now.Sub(cs.lastSeen) > t.resetIdle {
		cs = &chainState{}
		t.pairs[key] = cs
	}

	body := normalizeBody(msg.Content)
	if slices.Contains(cs.recent, body) {
		cs.repeats++
	} else {
		cs.repeats = 0
	}
	cs.recent = appendCapped(cs.recent, body, recentWindow)
	cs.lastSeen = now

	return cs.repeats > t.maxChain
}

// prune drops pair state that has been idle past resetIdle, keeping the map from
// growing without bound. Called from the broker's cleanup tick.
func (t *livelockTracker) prune(now time.Time) {
	if t == nil {
		return
	}
	for key, cs := range t.pairs {
		if now.Sub(cs.lastSeen) > t.resetIdle {
			delete(t.pairs, key)
		}
	}
}

// pairKey is a direction-independent key for the {a, b} session pair, so A→B and
// B→A share one chain.
func pairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "\x00" + b
}

// normalizeBody collapses whitespace and case so trivially-different echoes
// compare equal.
func normalizeBody(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

// appendCapped appends v to xs, dropping the oldest entry when over cap.
func appendCapped(xs []string, v string, cap int) []string {
	xs = append(xs, v)
	if len(xs) > cap {
		xs = xs[len(xs)-cap:]
	}
	return xs
}
