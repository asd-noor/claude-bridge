package broker

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a controllable time source for deterministic TTL tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// --- test hooks (in-package, command-based) -------------------------------
//
// The broker is an actor: state lives in the run loop, so tests reach it via
// commands rather than touching fields. These mirror the operations the old
// white-box tests performed directly under the mutex.

// setClock installs a deterministic clock on the run loop.
func (b *Broker) setClock(now func() time.Time) {
	ask(b, func(st *state) struct{} { st.now = now; return struct{}{} })
}

// reapNow runs a single cleanup pass synchronously against the current clock.
func (b *Broker) reapNow() {
	ask(b, func(st *state) struct{} { st.reap(st.now()); return struct{}{} })
}

// injectSession registers a session under a caller-chosen ID, used by tests
// that need a stable, reused identifier.
func (b *Broker) injectSession(id, projectPath string) {
	ask(b, func(st *state) struct{} {
		if _, exists := st.sessions[id]; !exists {
			now := st.now()
			st.sessions[id] = &sessEntry{
				session: Session{
					ID:           id,
					ProjectPath:  projectPath,
					RegisteredAt: now,
					LastSeen:     now,
				},
				bucket: st.newBucket(now),
			}
		}
		return struct{}{}
	})
}

// newTestBroker builds a broker wired to a fake clock. The cleanup ticker uses
// real time and will not fire within a fast test; tests drive reapNow directly.
func newTestBroker(t *testing.T, cfg Config) (*Broker, *fakeClock) {
	t.Helper()
	clk := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := New(cfg)
	b.setClock(clk.Now)
	t.Cleanup(b.Close)
	return b, clk
}

func TestRegisterTouchUnregister(t *testing.T) {
	b, clk := newTestBroker(t, Config{SessionTTL: time.Hour})

	s, err := b.RegisterSession("/home/user/projects/api")
	if err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}
	if s.ID == "" {
		t.Fatal("expected non-empty session ID")
	}
	if s.ProjectName != "api" {
		t.Fatalf("ProjectName = %q, want %q", s.ProjectName, "api")
	}

	firstSeen := s.LastSeen
	clk.advance(time.Minute)
	b.Touch(s.ID)
	got, ok := b.Session(s.ID)
	if !ok {
		t.Fatal("session missing after Touch")
	}
	if !got.LastSeen.After(firstSeen) {
		t.Fatal("Touch did not advance LastSeen")
	}

	b.UnregisterSession(s.ID)
	if _, ok := b.Session(s.ID); ok {
		t.Fatal("session still present after unregister")
	}
}

func TestInboxCapEviction(t *testing.T) {
	b, _ := newTestBroker(t, Config{MaxInboxSize: 3})
	recv, _ := b.RegisterSession("/p/recv")
	sender, _ := b.RegisterSession("/p/send")

	var ids []string
	for range 5 {
		id, err := b.Send(Message{From: sender.ID, To: recv.ID, Content: "x"})
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
		ids = append(ids, id)
	}

	msgs, err := b.Poll(recv.ID)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("inbox len = %d, want 3 (cap)", len(msgs))
	}
	// Oldest two evicted; the surviving three are the last three sent.
	for i, m := range msgs {
		want := ids[i+2]
		if m.ID != want {
			t.Fatalf("msg[%d].ID = %q, want %q", i, m.ID, want)
		}
	}

	// Poll clears the inbox.
	again, _ := b.Poll(recv.ID)
	if len(again) != 0 {
		t.Fatalf("inbox not cleared, len = %d", len(again))
	}
}

func TestBroadcastRateLimitAndRefill(t *testing.T) {
	b, clk := newTestBroker(t, Config{
		BroadcastBurst:  2,
		BroadcastRefill: 10 * time.Second,
	})
	sender, _ := b.RegisterSession("/p/send")
	b.RegisterSession("/p/a")
	b.RegisterSession("/p/b")

	for i := range 2 {
		n, err := b.Broadcast(sender.ID, "hello")
		if err != nil {
			t.Fatalf("Broadcast %d: %v", i, err)
		}
		if n != 2 {
			t.Fatalf("Broadcast %d delivered %d, want 2", i, n)
		}
	}

	// Bucket exhausted.
	if _, err := b.Broadcast(sender.ID, "again"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited, got %v", err)
	}

	// Not enough time for a refill.
	clk.advance(9 * time.Second)
	if _, err := b.Broadcast(sender.ID, "still"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited before refill, got %v", err)
	}

	// One refill interval elapses → one token.
	clk.advance(2 * time.Second) // total 11s ≥ 10s
	if _, err := b.Broadcast(sender.ID, "refilled"); err != nil {
		t.Fatalf("expected success after refill, got %v", err)
	}
	if _, err := b.Broadcast(sender.ID, "drained"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited after single refill, got %v", err)
	}
}

func TestMessageTTLExpiry(t *testing.T) {
	b, clk := newTestBroker(t, Config{MessageTTL: time.Minute, SessionTTL: time.Hour})
	recv, _ := b.RegisterSession("/p/recv")
	sender, _ := b.RegisterSession("/p/send")

	if _, err := b.Send(Message{From: sender.ID, To: recv.ID, Content: "old"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Advance past message TTL but within session TTL.
	clk.advance(2 * time.Minute)
	b.reapNow()

	msgs, _ := b.Poll(recv.ID)
	if len(msgs) != 0 {
		t.Fatalf("expected expired message to be dropped, got %d", len(msgs))
	}

	// Session itself should survive (within SessionTTL).
	if len(b.ListPeers(sender.ID)) != 1 {
		t.Fatal("recv session should still be alive")
	}
}

func TestSessionStalenessCleanup(t *testing.T) {
	b, clk := newTestBroker(t, Config{SessionTTL: time.Minute})
	a, _ := b.RegisterSession("/p/a")
	bSess, _ := b.RegisterSession("/p/b")

	// b stays fresh, a goes stale.
	clk.advance(2 * time.Minute)
	b.Touch(bSess.ID)
	b.reapNow()

	if _, ok := b.Session(a.ID); ok {
		t.Fatal("stale session a should have been reaped")
	}
	if _, ok := b.Session(bSess.ID); !ok {
		t.Fatal("fresh session b should survive")
	}
}

func TestListPeersExcludesCallerAndStale(t *testing.T) {
	b, clk := newTestBroker(t, Config{SessionTTL: time.Minute})
	caller, _ := b.RegisterSession("/p/caller")
	fresh, _ := b.RegisterSession("/p/fresh")
	stale, _ := b.RegisterSession("/p/stale")

	// Age everyone, then refresh only caller and fresh.
	clk.advance(2 * time.Minute)
	b.Touch(caller.ID)
	b.Touch(fresh.ID)

	peers := b.ListPeers(caller.ID)
	if len(peers) != 1 {
		t.Fatalf("ListPeers returned %d, want 1", len(peers))
	}
	if peers[0].ID != fresh.ID {
		t.Fatalf("ListPeers returned %q, want fresh %q", peers[0].ID, fresh.ID)
	}
	for _, p := range peers {
		if p.ID == caller.ID {
			t.Fatal("caller must be excluded")
		}
		if p.ID == stale.ID {
			t.Fatal("stale peer must be excluded")
		}
	}
}

func TestSendBlockingPushDelivery(t *testing.T) {
	b, _ := newTestBroker(t, Config{})
	recv, _ := b.RegisterSession("/p/recv")
	sender, _ := b.RegisterSession("/p/send")

	ch, cancel := b.Subscribe(recv.ID)
	defer cancel()

	// expects_reply → blocking push path; event must arrive.
	if _, err := b.Send(Message{
		From:         sender.ID,
		To:           recv.ID,
		Content:      "answer me",
		ExpectsReply: true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Type != EventMessage {
			t.Fatalf("event type = %q, want %q", ev.Type, EventMessage)
		}
		msg, ok := ev.Payload.(Message)
		if !ok || !msg.ExpectsReply {
			t.Fatalf("unexpected payload: %#v", ev.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocking push event")
	}
}

func TestSendNonBlockingDropsWhenFull(t *testing.T) {
	b, _ := newTestBroker(t, Config{MaxInboxSize: 1000})
	recv, _ := b.RegisterSession("/p/recv")
	sender, _ := b.RegisterSession("/p/send")

	ch, cancel := b.Subscribe(recv.ID)
	defer cancel()

	// Fire-and-forget sends beyond the channel buffer must not block and must
	// still land in the inbox for Poll to recover.
	const n = subChanBuffer + 25
	for i := range n {
		if _, err := b.Send(Message{From: sender.ID, To: recv.ID, Content: "fyi"}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}

	// The outbound channel holds at most its buffer; the remainder is dropped
	// from the push path (the pusher's non-blocking try-send).
	if got := len(ch); got > subChanBuffer {
		t.Fatalf("channel len = %d, exceeds buffer %d", got, subChanBuffer)
	}

	// Every message is still pollable.
	msgs, _ := b.Poll(recv.ID)
	if len(msgs) != n {
		t.Fatalf("Poll returned %d, want %d", len(msgs), n)
	}
}

func TestSendUnknownRecipient(t *testing.T) {
	b, _ := newTestBroker(t, Config{})
	sender, _ := b.RegisterSession("/p/send")

	if _, err := b.Send(Message{From: sender.ID, To: "nope", Content: "hi"}); !errors.Is(err, ErrUnknownSession) {
		t.Fatalf("expected ErrUnknownSession, got %v", err)
	}
}

func TestSubscribeCancelAndUnregisterClose(t *testing.T) {
	b, _ := newTestBroker(t, Config{})
	s, _ := b.RegisterSession("/p/s")

	ch, cancel := b.Subscribe(s.ID)
	cancel()
	if _, open := <-ch; open {
		t.Fatal("channel should be closed after cancel")
	}

	// Unregister must close a live subscription.
	ch2, _ := b.Subscribe(s.ID)
	b.UnregisterSession(s.ID)
	if _, open := <-ch2; open {
		t.Fatal("channel should be closed after unregister")
	}
}

// drain keeps reading a subscription channel until it closes, so blocking
// pushes never stall on a full buffer during concurrency stress.
func drain(ch <-chan Event) {
	for range ch {
	}
}

// TestConcurrentSendCloseHammersSameSession is the core regression test for the
// send-on-closed-channel hazard the mutex broker had to guard against. Many
// goroutines Send to a single recipient while other goroutines repeatedly
// Subscribe (which stops the prior pusher), cancel, and UnregisterSession that
// same recipient. Under the actor model no goroutine but the pusher ever closes
// or sends on the output channel, so no panic and no race may occur. Run -race.
func TestConcurrentSendCloseHammersSameSession(t *testing.T) {
	b := New(Config{MaxInboxSize: 1000})
	t.Cleanup(b.Close)

	sender, _ := b.RegisterSession("/p/sender")
	const recvID = "recv-fixed"

	var wg sync.WaitGroup
	var stop atomic.Bool
	deadline := time.After(2 * time.Second)

	// Recipient lifecycle churn: register → subscribe → drain → cancel/unregister.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			b.injectSession(recvID, "/p/recv")
			ch, cancel := b.Subscribe(recvID)
			go drain(ch)
			// Re-subscribe to force stopping the prior pusher under load.
			ch2, cancel2 := b.Subscribe(recvID)
			go drain(ch2)
			cancel()
			cancel2()
			b.UnregisterSession(recvID)
		}
	}()

	// Senders blast the same recipient with blocking and non-blocking pushes.
	for i := range 16 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for !stop.Load() {
				b.Send(Message{
					From:         sender.ID,
					To:           recvID,
					Content:      "x",
					ExpectsReply: n%2 == 0, // mix blocking / non-blocking paths
				})
			}
		}(i)
	}

	<-deadline
	stop.Store(true)
	wg.Wait()
}

// TestConcurrentBroadcastSubscribeUnregister stresses the broadcast fanout push
// path against concurrent subscribe-replace and unregister across many distinct
// sessions.
func TestConcurrentBroadcastSubscribeUnregister(t *testing.T) {
	b := New(Config{
		MaxInboxSize:    1000,
		BroadcastBurst:  1 << 30, // effectively unlimited for the stress run
		BroadcastRefill: time.Hour,
	})
	t.Cleanup(b.Close)

	const nSessions = 24
	ids := make([]string, nSessions)
	for i := range ids {
		s, _ := b.RegisterSession(fmt.Sprintf("/p/%d", i))
		ids[i] = s.ID
	}
	broadcaster, _ := b.RegisterSession("/p/broadcaster")

	var wg sync.WaitGroup
	var stop atomic.Bool
	deadline := time.After(2 * time.Second)

	// Churn subscriptions and registrations on every session.
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for !stop.Load() {
				ch, cancel := b.Subscribe(id)
				go drain(ch)
				ch2, cancel2 := b.Subscribe(id) // replaces+stops the prior pusher
				go drain(ch2)
				cancel()
				cancel2()
				b.UnregisterSession(id)
				b.injectSession(id, "/p/re")
			}
		}(id)
	}

	// Broadcasters fan out to everyone concurrently.
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				b.Broadcast(broadcaster.ID, "hello all")
			}
		}()
	}

	<-deadline
	stop.Store(true)
	wg.Wait()
}

// TestConcurrentStaleReapVsSend races the cleanup reaper (which stops stale
// pushers) against senders and broadcasters on the sessions being reaped. Uses a
// real clock with a tiny SessionTTL so sessions go stale almost immediately and
// reapNow reaps them mid-flight.
func TestConcurrentStaleReapVsSend(t *testing.T) {
	b := New(Config{
		MaxInboxSize:    1000,
		SessionTTL:      time.Nanosecond, // everything is stale almost at once
		BroadcastBurst:  1 << 30,
		BroadcastRefill: time.Hour,
	})
	t.Cleanup(b.Close)

	sender, _ := b.RegisterSession("/p/sender")

	var wg sync.WaitGroup
	var stop atomic.Bool
	deadline := time.After(2 * time.Second)

	// Continuously (re)register + subscribe recipients so the reaper always has
	// fresh-then-stale targets to stop.
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("recv-%d", n)
			for !stop.Load() {
				b.injectSession(id, "/p/recv")
				ch, cancel := b.Subscribe(id)
				go drain(ch)
				b.Send(Message{From: sender.ID, To: id, Content: "x", ExpectsReply: true})
				cancel()
			}
		}(i)
	}

	// The reaper stops stale pushers concurrently with the sends.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			b.reapNow()
		}
	}()

	// Extra senders and broadcasters add pressure on the push paths.
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for !stop.Load() {
				b.Send(Message{From: sender.ID, To: fmt.Sprintf("recv-%d", n%8), Content: "y"})
				b.Broadcast(sender.ID, "all")
			}
		}(i)
	}

	<-deadline
	stop.Store(true)
	wg.Wait()
}

func TestConfigDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if c.MessageTTL != defaultMessageTTL ||
		c.SessionTTL != defaultSessionTTL ||
		c.MaxInboxSize != defaultMaxInboxSize ||
		c.CleanupTick != defaultCleanupTick ||
		c.BroadcastBurst != defaultBroadcastBurst ||
		c.BroadcastRefill != defaultBroadcastRefill {
		t.Fatalf("defaults not applied: %#v", c)
	}
}
