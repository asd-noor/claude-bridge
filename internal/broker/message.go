package broker

import "time"

// Event types pushed to subscribers.
const (
	EventMessage    = "message"
	EventPeerJoined = "peer_joined"
	EventPeerLeft   = "peer_left"
)

// Message is a unit of communication routed between sessions. A blank To
// is reserved for broadcasts; otherwise To names the recipient session.
type Message struct {
	ID           string        `json:"id"`                      // UUIDv7
	From         string        `json:"from"`                    // sender session_id
	To           string        `json:"to"`                      // recipient session_id, "" for broadcast
	Content      string        `json:"content"`                 //
	CreatedAt    time.Time     `json:"created_at"`              //
	TTL          time.Duration `json:"-"`                       // zero = use broker default
	InReplyTo    string        `json:"in_reply_to,omitempty"`   // id of the message this answers
	ExpectsReply bool          `json:"expects_reply,omitempty"` // sender wants an answer
}

// wantsBlockingPush reports whether the message warrants a blocking push:
// either it answers a prior message or it explicitly asks for a reply.
func (m Message) wantsBlockingPush() bool {
	return m.InReplyTo != "" || m.ExpectsReply
}

// Event is a push notification delivered on a session's subscription channel.
type Event struct {
	Type    string `json:"type"`    // "message" | "peer_joined" | "peer_left"
	Payload any    `json:"payload"` //
}
