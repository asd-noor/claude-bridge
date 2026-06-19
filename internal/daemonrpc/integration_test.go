//go:build integration

package daemonrpc

import (
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/asd-noor/claude-bridge/internal/broker"
)

const (
	// eventWait bounds how long a test will block waiting for a single async
	// push event to arrive on a subscription channel.
	eventWait = time.Second
	// eventuallyWait bounds a poll-until-true retry loop.
	eventuallyWait = time.Second
	// eventuallyTick is the retry interval inside eventually.
	eventuallyTick = 5 * time.Millisecond
)

// harness wires a real broker behind a daemonrpc.Server listening on a temp
// Unix domain socket. Tests drive it exclusively through daemonrpc.Client.
type harness struct {
	t    *testing.T
	srv  *Server
	path string
}

// newHarness starts a server on a temp UDS with a short-but-usable broker
// config and registers cleanup of the listener and broker.
func newHarness(t *testing.T, cfg broker.Config) *harness {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen unix %q: %v", path, err)
	}

	b := broker.New(cfg)
	srv := NewServer(b)

	go func() {
		// Serve returns once the listener is closed in cleanup; that error is
		// expected and intentionally ignored.
		_ = srv.Serve(ln)
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		b.Close()
	})

	return &harness{t: t, srv: srv, path: path}
}

// dial opens a real client connection to the harness socket and registers it
// for cleanup.
func (h *harness) dial() *Client {
	h.t.Helper()
	c, err := Dial(h.path)
	if err != nil {
		h.t.Fatalf("dial %q: %v", h.path, err)
	}
	h.t.Cleanup(func() { _ = c.Close() })
	return c
}

// register performs a register_session RPC and returns the assigned session ID,
// mirroring how the shim obtains its identity.
func register(t *testing.T, c *Client, projectPath string) string {
	t.Helper()
	raw, err := c.Call(MethodRegister, RegisterParams{ProjectPath: projectPath})
	if err != nil {
		t.Fatalf("register_session: %v", err)
	}
	var res RegisterResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode register result: %v", err)
	}
	if res.SessionID == "" {
		t.Fatalf("register_session returned empty session id")
	}
	return res.SessionID
}

// sendMessage issues a send_message RPC as sender and returns the message ID.
func sendMessage(t *testing.T, c *Client, sender string, p SendParams) string {
	t.Helper()
	raw, err := c.CallAs(sender, MethodSend, p)
	if err != nil {
		t.Fatalf("send_message: %v", err)
	}
	var res SendResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode send result: %v", err)
	}
	return res.MessageID
}

// pollMessages issues a poll_messages RPC as sessionID and returns the inbox.
func pollMessages(t *testing.T, c *Client, sessionID string) []broker.Message {
	t.Helper()
	raw, err := c.CallAs(sessionID, MethodPoll, struct{}{})
	if err != nil {
		t.Fatalf("poll_messages: %v", err)
	}
	var res PollResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode poll result: %v", err)
	}
	return res.Messages
}

// eventually retries fn until it returns true or the deadline elapses,
// failing the test on timeout. It replaces fixed sleeps for async delivery.
func eventually(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventuallyWait)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(eventuallyTick)
	}
	t.Fatalf("condition never became true: %s", what)
}

// awaitMessageEvent waits for a "message" event on ch and decodes its payload
// into a broker.Message. Over the wire Event.Payload arrives as a generic map,
// so it is re-marshaled into the concrete type.
func awaitMessageEvent(t *testing.T, ch <-chan broker.Event) broker.Message {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatalf("event channel closed before a message arrived")
		}
		if ev.Type != broker.EventMessage {
			t.Fatalf("unexpected event type %q, want %q", ev.Type, broker.EventMessage)
		}
		return decodeMessagePayload(t, ev.Payload)
	case <-time.After(eventWait):
		t.Fatalf("timed out after %s waiting for a message event", eventWait)
		return broker.Message{}
	}
}

// awaitEventType waits for the next event on ch and asserts its type, used to
// drain presence events and confirm a subscription is live.
func awaitEventType(t *testing.T, ch <-chan broker.Event, want string) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatalf("event channel closed before a %q event", want)
		}
		if ev.Type != want {
			t.Fatalf("unexpected event type %q, want %q", ev.Type, want)
		}
	case <-time.After(eventWait):
		t.Fatalf("timed out after %s waiting for a %q event", eventWait, want)
	}
}

// decodeMessagePayload converts a decoded Event payload into a broker.Message.
func decodeMessagePayload(t *testing.T, payload any) broker.Message {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("re-marshal event payload: %v", err)
	}
	var msg broker.Message
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("decode message payload: %v", err)
	}
	return msg
}

// TestMessageExchangeViaPoll covers a directed send surfaced by the recipient's
// poll_messages, asserting from and content survive the round trip.
func TestMessageExchangeViaPoll(t *testing.T) {
	h := newHarness(t, broker.Config{})

	clientA := h.dial()
	clientB := h.dial()

	idA := register(t, clientA, "/work/project-a")
	idB := register(t, clientB, "/work/project-b")

	const content = "hello from A"
	sendMessage(t, clientA, idA, SendParams{To: idB, Content: content})

	var got []broker.Message
	eventually(t, "B's inbox holds the message", func() bool {
		got = pollMessages(t, clientB, idB)
		return len(got) == 1
	})

	if got[0].From != idA {
		t.Fatalf("message From = %q, want %q", got[0].From, idA)
	}
	if got[0].Content != content {
		t.Fatalf("message Content = %q, want %q", got[0].Content, content)
	}
}

// TestMessageExchangeViaSubscribe covers push delivery: B subscribes, A sends,
// and B's event channel surfaces the message event with correct from/content.
func TestMessageExchangeViaSubscribe(t *testing.T) {
	h := newHarness(t, broker.Config{})

	clientA := h.dial()
	clientB := h.dial()
	subB := h.dial()

	idB := register(t, clientB, "/work/project-b")

	events, err := subB.Subscribe(idB)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	idA := register(t, clientA, "/work/project-a")

	// subB.Subscribe returning does not guarantee the server has installed B's
	// channel yet (the subscribe frame is handled asynchronously). Rather than
	// race on a best-effort peer_joined event, re-send until a push lands: sends
	// that arrive before the channel is wired fall through to B's inbox
	// harmlessly, and once it is wired the event is delivered deterministically.
	const content = "pushed to B"
	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the pushed message event")
		}
		sendMessage(t, clientA, idA, SendParams{To: idB, Content: content, ExpectsReply: true})
		select {
		case ev := <-events:
			if ev.Type != broker.EventMessage {
				continue
			}
			msg := decodeMessagePayload(t, ev.Payload)
			if msg.From != idA {
				t.Fatalf("event From = %q, want %q", msg.From, idA)
			}
			if msg.Content != content {
				t.Fatalf("event Content = %q, want %q", msg.Content, content)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestBroadcastFanOutAndRateLimit covers broadcast delivery to all peers and
// rate-limit exhaustion once the sender's token bucket drains.
func TestBroadcastFanOutAndRateLimit(t *testing.T) {
	// Burst of 2 with a long refill so the third broadcast is guaranteed to be
	// rejected within the test window.
	h := newHarness(t, broker.Config{BroadcastBurst: 2, BroadcastRefill: time.Hour})

	sender := h.dial()
	peer1 := h.dial()
	peer2 := h.dial()

	idSender := register(t, sender, "/work/sender")
	register(t, peer1, "/work/peer-1")
	register(t, peer2, "/work/peer-2")

	n := broadcast(t, sender, idSender, "first")
	if n != 2 {
		t.Fatalf("broadcast recipients = %d, want 2", n)
	}

	// Second broadcast consumes the last token.
	if _, err := sender.CallAs(idSender, MethodBroadcast, BroadcastParams{Content: "second"}); err != nil {
		t.Fatalf("second broadcast unexpectedly failed: %v", err)
	}

	// Third broadcast must be rate limited.
	_, err := sender.CallAs(idSender, MethodBroadcast, BroadcastParams{Content: "third"})
	if err == nil {
		t.Fatalf("third broadcast succeeded, want rate-limit error")
	}
	if err.Error() != broker.ErrRateLimited.Error() {
		t.Fatalf("third broadcast error = %q, want %q", err.Error(), broker.ErrRateLimited.Error())
	}
}

// broadcast issues a broadcast RPC and returns the recipient count.
func broadcast(t *testing.T, c *Client, sender, content string) int {
	t.Helper()
	raw, err := c.CallAs(sender, MethodBroadcast, BroadcastParams{Content: content})
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	var res BroadcastResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("decode broadcast result: %v", err)
	}
	return res.Recipients
}

// TestThreadedReply covers a reply thread: A asks (ExpectsReply), B answers
// with InReplyTo set, and A's poll surfaces the reply with InReplyTo == the
// original message ID.
func TestThreadedReply(t *testing.T) {
	h := newHarness(t, broker.Config{})

	clientA := h.dial()
	clientB := h.dial()

	idA := register(t, clientA, "/work/project-a")
	idB := register(t, clientB, "/work/project-b")

	questionID := sendMessage(t, clientA, idA, SendParams{
		To:           idB,
		Content:      "does your code call getUser()?",
		ExpectsReply: true,
	})
	if questionID == "" {
		t.Fatalf("question message id is empty")
	}

	// B sees the question, then replies threading it back to A.
	eventually(t, "B receives the question", func() bool {
		return len(pollMessages(t, clientB, idB)) >= 1
	})
	sendMessage(t, clientB, idB, SendParams{
		To:        idA,
		Content:   "no, it's getCurrentUser() now",
		InReplyTo: questionID,
	})

	var reply []broker.Message
	eventually(t, "A receives the reply", func() bool {
		reply = pollMessages(t, clientA, idA)
		return len(reply) == 1
	})

	if reply[0].From != idB {
		t.Fatalf("reply From = %q, want %q", reply[0].From, idB)
	}
	if reply[0].InReplyTo != questionID {
		t.Fatalf("reply InReplyTo = %q, want %q", reply[0].InReplyTo, questionID)
	}
}

// TestIdleShutdownFires covers the idle hook firing once the last connection
// closes: ActiveConns returns to zero and the OnIdle callback is observed.
func TestIdleShutdownFires(t *testing.T) {
	h := newHarness(t, broker.Config{})

	idle := make(chan struct{}, 1)
	h.srv.OnIdle(func() {
		// Non-blocking so a later re-fire (other tests reuse the pattern) is
		// never able to deadlock the hook.
		select {
		case idle <- struct{}{}:
		default:
		}
	})

	// A single short-lived connection: open, register, then close.
	c, err := Dial(h.path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	register(t, c, "/work/solo")

	eventually(t, "server observes the active connection", func() bool {
		return h.srv.ActiveConns() >= 1
	})

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case <-idle:
	case <-time.After(eventWait):
		t.Fatalf("OnIdle hook did not fire within %s of the last disconnect", eventWait)
	}

	eventually(t, "active connection count returns to zero", func() bool {
		return h.srv.ActiveConns() == 0
	})
}

// TestIdleShutdownCanceledByNewConn covers the cancel path: a new connection
// arriving after the idle window opens keeps the server active, so the modeled
// shutdown action is not taken.
func TestIdleShutdownCanceledByNewConn(t *testing.T) {
	h := newHarness(t, broker.Config{})

	// shutdownFired models cmd-layer behaviour: OnIdle starts a timer; if a new
	// connection arrives first, the timer is canceled and shutdown never runs.
	idle := make(chan struct{}, 4)
	h.srv.OnIdle(func() { idle <- struct{}{} })

	first, err := Dial(h.path)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	register(t, first, "/work/first")
	eventually(t, "first connection is active", func() bool {
		return h.srv.ActiveConns() >= 1
	})

	// Last (only) connection closes → idle window opens.
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	select {
	case <-idle:
	case <-time.After(eventWait):
		t.Fatalf("OnIdle did not fire after the last disconnect")
	}

	// A new connection arrives during the modeled idle window. The cancel path
	// is observable as ActiveConns climbing back above zero.
	second := h.dial()
	register(t, second, "/work/second")

	eventually(t, "new connection cancels idle by re-activating the server", func() bool {
		return h.srv.ActiveConns() >= 1
	})

	// The shutdown action (gated on ActiveConns == 0 in the cmd layer) must not
	// proceed while a connection is live.
	if got := h.srv.ActiveConns(); got == 0 {
		t.Fatalf("ActiveConns = 0 with a live connection; shutdown would wrongly proceed")
	}

	// And no spurious extra idle fire occurred while the new conn is open.
	select {
	case <-idle:
		t.Fatalf("OnIdle fired again while a connection is still active")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestStaleSocketHandling documents that stale-socket recovery is a cmd/serve
// concern (flock dance + listen-after-remove) and is not drivable through the
// daemonrpc layer alone, which only consumes an already-bound net.Listener.
func TestStaleSocketHandling(t *testing.T) {
	t.Skip("stale-socket recovery lives in cmd/serve (pre-listen flock + os.Remove); " +
		"daemonrpc.Server only consumes a bound net.Listener and cannot exercise it")
}
