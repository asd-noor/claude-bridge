package broker

import "time"

// pusher owns a single session's outbound event delivery. The run loop hands it
// events on mailbox (non-blocking); the pusher forwards them to out, applying
// the tiered push policy. The pusher is the sole sender on out and the sole
// closer of out, so out can never be sent-on after close — the actor model's
// structural replacement for the lock-based send/close coordination.
type pusher struct {
	mailbox chan Event    // run loop is the sole sender; never closed
	out     chan Event    // returned to subscribers; closed only by run, once
	stop    chan struct{} // buffered(1) stop signal
}

// newPusher builds a pusher sized from cfg. The mailbox absorbs delivery bursts
// so the run loop's hand-off never blocks; out matches the subscription buffer.
func newPusher(cfg Config) *pusher {
	return &pusher{
		mailbox: make(chan Event, cfg.MaxInboxSize),
		out:     make(chan Event, subChanBuffer),
		stop:    make(chan struct{}, 1),
	}
}

// run drains the mailbox to out until stopped, then closes out exactly once.
func (p *pusher) run() {
	defer close(p.out)
	for {
		select {
		case <-p.stop:
			return
		case ev := <-p.mailbox:
			p.forward(ev)
		}
	}
}

// forward delivers ev to out. Replies and reply-expecting messages use a bounded
// blocking send so an attentive subscriber is not missed; everything else uses a
// non-blocking try-send and is dropped on a full channel, to be caught by Poll.
func (p *pusher) forward(ev Event) {
	if !blockingEvent(ev) {
		select {
		case p.out <- ev:
		default:
		}
		return
	}
	timer := time.NewTimer(blockingPushTimeout)
	defer timer.Stop()
	select {
	case p.out <- ev:
	case <-timer.C:
	}
}

// enqueue hands ev to the pusher without blocking the run loop, dropping it on a
// full mailbox (recoverable via Poll).
func (p *pusher) enqueue(ev Event) {
	select {
	case p.mailbox <- ev:
	default:
	}
}

// stopPush signals the pusher to drain-stop and close out. It never blocks, so
// the run loop can stop a pusher even while it is mid blocking-send.
func (p *pusher) stopPush() {
	select {
	case p.stop <- struct{}{}:
	default:
	}
}

// blockingEvent reports whether ev warrants a blocking push: a message event
// that answers a prior message or explicitly asks for a reply.
func blockingEvent(ev Event) bool {
	if ev.Type != EventMessage {
		return false
	}
	m, ok := ev.Payload.(Message)
	return ok && m.wantsBlockingPush()
}
