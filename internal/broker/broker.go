// Package broker is the single source of truth for the daemon's runtime
// state: connected sessions, their pending inboxes, and push subscriptions.
// It is process-internal and knows nothing about transport.
//
// Concurrency model: actor. All shared state lives in a single `state` value
// owned by one goroutine (the run loop). Every public method posts a closure
// onto the command channel; the loop executes them serially, so the state is
// accessed without locks. Slow push delivery is delegated to a per-session
// pusher goroutine so it never blocks the loop. Because each pusher is the sole
// owner of its output channel's lifecycle, the send-on-closed-channel hazard of
// a lock-based design cannot occur.
package broker

import (
	"errors"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrRateLimited is returned by Broadcast when the caller's token bucket is
// exhausted.
var ErrRateLimited = errors.New("broker: broadcast rate limited")

// ErrUnknownSession is returned when an operation references a session that
// the broker does not track.
var ErrUnknownSession = errors.New("broker: unknown session")

// ErrClosed is returned when an operation is attempted on a closed broker.
var ErrClosed = errors.New("broker: closed")

// Default configuration values.
const (
	defaultMessageTTL      = 5 * time.Minute
	defaultSessionTTL      = 30 * time.Minute
	defaultMaxInboxSize    = 100
	defaultCleanupTick     = time.Minute
	defaultBroadcastBurst  = 3
	defaultBroadcastRefill = 10 * time.Second

	defaultLivelockMaxChain  = 20
	defaultLivelockResetIdle = 60 * time.Second

	// blockingPushTimeout bounds a blocking push to a busy recipient.
	blockingPushTimeout = 100 * time.Millisecond
	// subChanBuffer sizes each subscription's outbound channel.
	subChanBuffer = 16
	// cmdBuffer smooths bursts of commands onto the run loop.
	cmdBuffer = 64
)

// Config tunes broker timing and capacity. Zero-valued fields fall back to
// package defaults.
type Config struct {
	MessageTTL      time.Duration // how long unread messages live
	SessionTTL      time.Duration // peer staleness threshold
	MaxInboxSize    int           // max messages queued per session
	CleanupTick     time.Duration // cleanup loop interval
	BroadcastBurst  int           // broadcast token bucket capacity
	BroadcastRefill time.Duration // broadcast token bucket refill interval

	LivelockEnabled   bool          // trip the no-progress reply-chain breaker
	LivelockMaxChain  int           // consecutive content-free exchanges before tripping
	LivelockResetIdle time.Duration // idle gap that resets a chain
}

// withDefaults returns a copy of cfg with zero fields replaced by defaults.
func (c Config) withDefaults() Config {
	if c.MessageTTL <= 0 {
		c.MessageTTL = defaultMessageTTL
	}
	if c.SessionTTL <= 0 {
		c.SessionTTL = defaultSessionTTL
	}
	if c.MaxInboxSize <= 0 {
		c.MaxInboxSize = defaultMaxInboxSize
	}
	if c.CleanupTick <= 0 {
		c.CleanupTick = defaultCleanupTick
	}
	if c.BroadcastBurst <= 0 {
		c.BroadcastBurst = defaultBroadcastBurst
	}
	if c.BroadcastRefill <= 0 {
		c.BroadcastRefill = defaultBroadcastRefill
	}
	if c.LivelockMaxChain <= 0 {
		c.LivelockMaxChain = defaultLivelockMaxChain
	}
	if c.LivelockResetIdle <= 0 {
		c.LivelockResetIdle = defaultLivelockResetIdle
	}
	return c
}

// tokenBucket is a per-sender broadcast limiter. Tokens refill one per refill
// interval up to burst. It is mutated only by the run loop, so it needs no
// synchronization of its own.
type tokenBucket struct {
	tokens     int
	burst      int
	refill     time.Duration
	lastRefill time.Time
}

// take refills lazily against now then consumes one token, reporting success.
func (tb *tokenBucket) take(now time.Time) bool {
	if elapsed := now.Sub(tb.lastRefill); elapsed >= tb.refill {
		gained := int(elapsed / tb.refill)
		tb.tokens = min(tb.burst, tb.tokens+gained)
		tb.lastRefill = tb.lastRefill.Add(time.Duration(gained) * tb.refill)
	}
	if tb.tokens <= 0 {
		return false
	}
	tb.tokens--
	return true
}

// Broker is the public handle. It owns no state directly; it forwards work to
// the run loop over cmds and is shut down via done.
type Broker struct {
	cmds      chan func(*state)
	done      chan struct{}
	closeOnce sync.Once
}

// state is the loop-private store. Only the run loop touches it.
type state struct {
	cfg      Config
	now      func() time.Time
	sessions map[string]*sessEntry // session_id → metadata + inbox + bucket
	pushers  map[string]*pusher    // session_id → active subscription pusher
	livelock *livelockTracker      // no-progress reply-chain breaker
}

// sessEntry is a session's metadata plus its pending inbox and broadcast limiter.
type sessEntry struct {
	session Session
	inbox   []Message
	bucket  *tokenBucket
}

// New constructs a Broker and starts its run loop.
func New(cfg Config) *Broker {
	b := &Broker{
		cmds: make(chan func(*state), cmdBuffer),
		done: make(chan struct{}),
	}
	resolved := cfg.withDefaults()
	st := &state{
		cfg:      resolved,
		now:      time.Now,
		sessions: make(map[string]*sessEntry),
		pushers:  make(map[string]*pusher),
		livelock: newLivelockTracker(resolved),
	}
	go b.run(st)
	return b
}

// run is the actor loop: it serially executes posted commands, reaps on the
// cleanup tick, and on Close stops every pusher before returning.
func (b *Broker) run(st *state) {
	ticker := time.NewTicker(st.cfg.CleanupTick)
	defer ticker.Stop()
	for {
		select {
		case fn := <-b.cmds:
			fn(st)
		case <-ticker.C:
			st.reap(st.now())
		case <-b.done:
			for _, p := range st.pushers {
				p.stopPush()
			}
			return
		}
	}
}

// Close stops the run loop and all pushers. It is idempotent.
func (b *Broker) Close() {
	b.closeOnce.Do(func() { close(b.done) })
}

// do posts fn to the run loop, reporting false if the broker is closed.
func (b *Broker) do(fn func(*state)) bool {
	select {
	case b.cmds <- fn:
		return true
	case <-b.done:
		return false
	}
}

// ask posts fn to the run loop and returns its result. The bool is false if the
// broker is closed before the command could run.
func ask[T any](b *Broker, fn func(*state) T) (T, bool) {
	reply := make(chan T, 1)
	if !b.do(func(st *state) { reply <- fn(st) }) {
		var zero T
		return zero, false
	}
	return <-reply, true
}

// newID returns a UUIDv7 string, or a UUIDv4 fallback if v7 generation fails.
func newID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

// RegisterSession creates a new session for projectPath and returns it.
func (b *Broker) RegisterSession(projectPath string) (*Session, error) {
	s, ok := ask(b, func(st *state) *Session {
		now := st.now()
		sess := Session{
			ID:           newID(),
			ProjectPath:  projectPath,
			ProjectName:  filepath.Base(projectPath),
			RegisteredAt: now,
			LastSeen:     now,
		}
		st.sessions[sess.ID] = &sessEntry{session: sess, bucket: st.newBucket(now)}
		cp := sess
		st.notifyPeers(sess.ID, Event{Type: EventPeerJoined, Payload: &cp})
		return &cp
	})
	if !ok {
		return nil, ErrClosed
	}
	return s, nil
}

// newBucket builds a full token bucket using the configured limits.
func (st *state) newBucket(now time.Time) *tokenBucket {
	return &tokenBucket{
		tokens:     st.cfg.BroadcastBurst,
		burst:      st.cfg.BroadcastBurst,
		refill:     st.cfg.BroadcastRefill,
		lastRefill: now,
	}
}

// Touch refreshes LastSeen for sessionID. Called on every inbound RPC. It is
// fire-and-forget; FIFO command ordering keeps it consistent with later calls.
func (b *Broker) Touch(sessionID string) {
	b.do(func(st *state) {
		if e, ok := st.sessions[sessionID]; ok {
			e.session.LastSeen = st.now()
		}
	})
}

// UnregisterSession removes a session, stops its subscription, and notifies
// peers. It is synchronous so a clean shim exit is fully applied on return.
func (b *Broker) UnregisterSession(sessionID string) {
	ask(b, func(st *state) struct{} {
		if _, ok := st.sessions[sessionID]; !ok {
			return struct{}{}
		}
		delete(st.sessions, sessionID)
		if p, ok := st.pushers[sessionID]; ok {
			p.stopPush()
			delete(st.pushers, sessionID)
		}
		st.notifyPeers(sessionID, Event{Type: EventPeerLeft, Payload: sessionID})
		return struct{}{}
	})
}

// ListPeers returns copies of all non-stale sessions except the caller.
func (b *Broker) ListPeers(callerID string) []*Session {
	peers, _ := ask(b, func(st *state) []*Session {
		return st.listPeers(callerID)
	})
	return peers
}

// listPeers snapshots non-stale peers excluding callerID. Caller is the loop.
func (st *state) listPeers(callerID string) []*Session {
	now := st.now()
	peers := make([]*Session, 0, len(st.sessions))
	for id, e := range st.sessions {
		if id == callerID || e.session.isStaleAt(now, st.cfg.SessionTTL) {
			continue
		}
		cp := e.session
		peers = append(peers, &cp)
	}
	return peers
}

// Session returns a copy of the session for id and whether it exists. Unlike
// ListPeers it does not filter by staleness, so a peer that is stale but not
// yet reaped is still resolvable — used by single-peer lookups.
func (b *Broker) Session(id string) (*Session, bool) {
	type result struct {
		s  *Session
		ok bool
	}
	r, alive := ask(b, func(st *state) result {
		e, ok := st.sessions[id]
		if !ok {
			return result{nil, false}
		}
		cp := e.session
		return result{&cp, true}
	})
	if !alive {
		return nil, false
	}
	return r.s, r.ok
}

// Send delivers msg to msg.To's inbox and pushes a message event to the
// recipient's pusher. It returns the assigned message ID.
//
// The caller is not blocked on delivery: the loop enqueues the message and
// hands the event to the recipient's pusher without blocking. The pusher then
// performs the tiered push — a bounded blocking send for replies and
// reply-expecting messages, a non-blocking try-send otherwise.
func (b *Broker) Send(msg Message) (string, error) {
	type result struct {
		id  string
		err error
	}
	r, ok := ask(b, func(st *state) result {
		m := st.stamp(msg)
		e, ok := st.sessions[m.To]
		if !ok {
			return result{"", ErrUnknownSession}
		}
		// Permission-relay control messages are push-only: they never persist in
		// the inbox and are exempt from the livelock breaker. Ordinary messages
		// are inboxed, and their wake is suppressed once the breaker trips.
		if m.Kind != KindMessage {
			st.deliver(m.To, Event{Type: EventMessage, Payload: m})
			return result{m.ID, nil}
		}
		st.enqueue(e, m)
		if !st.livelock.trip(m, st.now()) {
			st.deliver(m.To, Event{Type: EventMessage, Payload: m})
		}
		return result{m.ID, nil}
	})
	if !ok {
		return "", ErrClosed
	}
	return r.id, r.err
}

// stamp fills in the immutable fields of a message before delivery.
func (st *state) stamp(msg Message) Message {
	msg.ID = newID()
	msg.CreatedAt = st.now()
	if msg.TTL <= 0 {
		msg.TTL = st.cfg.MessageTTL
	}
	return msg
}

// enqueue appends msg to a session's inbox, evicting the oldest when at
// capacity. Caller is the loop.
func (st *state) enqueue(e *sessEntry, msg Message) {
	if len(e.inbox) >= st.cfg.MaxInboxSize {
		e.inbox = e.inbox[1:]
	}
	e.inbox = append(e.inbox, msg)
}

// deliver hands ev to id's pusher, if any, without blocking the loop.
func (st *state) deliver(id string, ev Event) {
	if p, ok := st.pushers[id]; ok {
		p.enqueue(ev)
	}
}

// Broadcast delivers content to every active session except the sender,
// subject to the sender's token bucket. It returns the number of recipients
// and ErrRateLimited when the bucket is exhausted.
func (b *Broker) Broadcast(from string, content string) (int, error) {
	type result struct {
		n   int
		err error
	}
	r, ok := ask(b, func(st *state) result {
		now := st.now()
		bucket := st.bucketFor(from, now)
		if !bucket.take(now) {
			return result{0, ErrRateLimited}
		}
		return result{st.fanout(from, content), nil}
	})
	if !ok {
		return 0, ErrClosed
	}
	return r.n, r.err
}

// bucketFor returns the sender's bucket, creating one for an unknown sender
// (e.g. a sender with no registered session). Caller is the loop.
func (st *state) bucketFor(from string, now time.Time) *tokenBucket {
	if e, ok := st.sessions[from]; ok {
		return e.bucket
	}
	// Unknown sender: hold a transient bucket on a synthetic entry so repeat
	// callers are still limited. Mirrors the prior buckets-map behaviour.
	e := &sessEntry{bucket: st.newBucket(now)}
	st.sessions[from] = e
	return e.bucket
}

// fanout enqueues a broadcast for every session except from and pushes it to
// each recipient's pusher, returning the recipient count. Caller is the loop.
func (st *state) fanout(from, content string) int {
	n := 0
	for id, e := range st.sessions {
		if id == from {
			continue
		}
		msg := st.stamp(Message{From: from, To: id, Content: content})
		st.enqueue(e, msg)
		st.deliver(id, Event{Type: EventMessage, Payload: msg})
		n++
	}
	return n
}

// Poll returns and clears the pending inbox for sessionID.
func (b *Broker) Poll(sessionID string) ([]Message, error) {
	type result struct {
		msgs []Message
		err  error
	}
	r, ok := ask(b, func(st *state) result {
		e, ok := st.sessions[sessionID]
		if !ok {
			return result{nil, ErrUnknownSession}
		}
		msgs := e.inbox
		e.inbox = nil
		return result{msgs, nil}
	})
	if !ok {
		return nil, ErrClosed
	}
	return r.msgs, r.err
}

// Subscribe returns an event channel for sessionID and a cancel func that
// unsubscribes. A second subscribe for the same session replaces and stops the
// prior pusher. The channel is closed when the pusher stops (cancel,
// UnregisterSession, stale reap, or Close).
func (b *Broker) Subscribe(sessionID string) (<-chan Event, func()) {
	p, ok := ask(b, func(st *state) *pusher {
		np := newPusher(st.cfg)
		if old, had := st.pushers[sessionID]; had {
			old.stopPush()
		}
		st.pushers[sessionID] = np
		go np.run()
		return np
	})
	if !ok {
		return closedEventChan(), func() {}
	}
	cancel := func() {
		b.do(func(st *state) {
			if cur, had := st.pushers[sessionID]; had && cur == p {
				cur.stopPush()
				delete(st.pushers, sessionID)
			}
		})
	}
	return p.out, cancel
}

// notifyPeers hands ev to every pusher except excludeID, without blocking.
func (st *state) notifyPeers(excludeID string, ev Event) {
	for id, p := range st.pushers {
		if id == excludeID {
			continue
		}
		p.enqueue(ev)
	}
}

// reap removes stale sessions (stopping pushers, notifying peers) and drops
// expired inbox messages. Caller is the loop.
func (st *state) reap(now time.Time) {
	for id, e := range st.sessions {
		if !e.session.isStaleAt(now, st.cfg.SessionTTL) {
			continue
		}
		delete(st.sessions, id)
		if p, ok := st.pushers[id]; ok {
			p.stopPush()
			delete(st.pushers, id)
		}
		st.notifyPeers(id, Event{Type: EventPeerLeft, Payload: id})
	}
	st.expireMessages(now)
	st.livelock.prune(now)
}

// expireMessages drops inbox messages whose TTL has elapsed. Caller is the loop.
func (st *state) expireMessages(now time.Time) {
	for _, e := range st.sessions {
		if len(e.inbox) == 0 {
			continue
		}
		kept := e.inbox[:0]
		for _, msg := range e.inbox {
			if now.Sub(msg.CreatedAt) <= msg.TTL {
				kept = append(kept, msg)
			}
		}
		if len(kept) == 0 {
			e.inbox = nil
		} else {
			e.inbox = kept
		}
	}
}

// closedEventChan returns an already-closed event channel for the closed-broker
// path so callers ranging over it terminate immediately.
func closedEventChan() <-chan Event {
	ch := make(chan Event)
	close(ch)
	return ch
}
