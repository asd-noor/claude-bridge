package daemonrpc

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asd-noor/claude-bridge/internal/broker"
)

// Method names carried in a request frame's Method field. They form the shared
// RPC contract between the shim client and the daemon server.
const (
	MethodRegister    = "register_session"
	MethodUnregister  = "unregister_session"
	MethodListPeers   = "list_peers"
	MethodSend        = "send_message"
	MethodBroadcast   = "broadcast"
	MethodPoll        = "poll_messages"
	MethodGetPeerInfo = "get_peer_info"
	MethodSubscribe   = "subscribe"
)

// Request is the envelope for an RPC call. SessionID is injected by the shim;
// when non-empty the server refreshes the session's liveness before dispatch.
// Params carries the method-specific payload.
type Request struct {
	Method    string          `json:"method"`
	SessionID string          `json:"session_id,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
}

// Response is the envelope for an RPC reply. Exactly one of Result or Error is
// meaningful: a non-empty Error indicates failure.
type Response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// RegisterParams requests a new session for a project working directory.
type RegisterParams struct {
	ProjectPath string `json:"project_path"`
}

// RegisterResult returns the freshly assigned session identity.
type RegisterResult struct {
	SessionID   string `json:"session_id"`
	ProjectName string `json:"project_name"`
	ProjectPath string `json:"project_path"`
}

// ListPeersResult enumerates the active peers visible to the caller.
type ListPeersResult struct {
	Peers []*broker.Session `json:"peers"`
}

// SendParams addresses a directed message to a single peer. Kind and RequestID
// carry the permission-relay control flow; both are empty for ordinary messages.
type SendParams struct {
	To           string `json:"to"`
	Content      string `json:"content"`
	InReplyTo    string `json:"in_reply_to,omitempty"`
	ExpectsReply bool   `json:"expects_reply,omitempty"`
	Kind         string `json:"kind,omitempty"`
	RequestID    string `json:"request_id,omitempty"`
}

// SendResult returns the broker-assigned message identifier.
type SendResult struct {
	MessageID string `json:"message_id"`
}

// BroadcastParams fans content out to every peer.
type BroadcastParams struct {
	Content string `json:"content"`
}

// BroadcastResult reports how many peers received the broadcast.
type BroadcastResult struct {
	Recipients int `json:"recipients"`
}

// PollResult returns and clears the caller's pending inbox.
type PollResult struct {
	Messages []broker.Message `json:"messages"`
}

// GetPeerInfoParams identifies the peer to look up.
type GetPeerInfoParams struct {
	SessionID string `json:"session_id"`
}

// GetPeerInfoResult describes a single peer.
type GetPeerInfoResult struct {
	SessionID   string `json:"session_id"`
	ProjectName string `json:"project_name"`
	ProjectPath string `json:"project_path"`
	LastSeen    string `json:"last_seen"`
}

// ErrPeerNotFound is returned when get_peer_info names an unknown session.
var ErrPeerNotFound = errors.New("daemonrpc: peer not found")

// Server adapts a broker to the UDS frame transport. It owns no lifecycle
// policy: it merely counts active connections and fires an injected OnIdle
// hook when the count returns to zero, leaving timer and shutdown decisions to
// the caller.
type Server struct {
	broker *broker.Broker

	active atomic.Int64

	mu     sync.RWMutex
	onIdle func()
}

// NewServer wraps b in a transport server.
func NewServer(b *broker.Broker) *Server {
	return &Server{broker: b}
}

// OnIdle registers a hook fired whenever the active connection count drops to
// zero. The caller typically starts an idle-shutdown timer here.
func (s *Server) OnIdle(fn func()) {
	s.mu.Lock()
	s.onIdle = fn
	s.mu.Unlock()
}

// ActiveConns reports the number of currently open connections.
func (s *Server) ActiveConns() int64 {
	return s.active.Load()
}

// Serve accepts connections on ln until it returns an error (e.g. the listener
// is closed). Each connection is handled in its own goroutine.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// handleConn services one connection's frames until EOF or error, maintaining
// the active-connection count around its lifetime.
//
// A session's lifetime is bound to the connection that REGISTERED it (the
// shim's long-lived control connection): when that connection drops, the session
// is unregistered immediately rather than left for SessionTTL. This keeps the
// peer list reflecting live shims even on a dirty exit (e.g. a plugin reload
// kills the shim with no clean unregister). Only the registering connection
// binds — ephemeral connections that merely reference a session_id (the prompt
// hook's poll, `status`, the subscribe stream) reap nothing, so they cannot
// unregister a session that is still alive.
func (s *Server) handleConn(conn net.Conn) {
	s.active.Add(1)

	var boundSession string
	defer func() {
		if boundSession != "" {
			s.broker.UnregisterSession(boundSession)
		}
		s.connClosed(conn)
	}()

	for {
		raw, err := ReadFrame(conn)
		if err != nil {
			return
		}

		var req Request
		if err := json.Unmarshal(raw, &req); err != nil {
			_ = s.writeError(conn, err)
			continue
		}

		if req.SessionID != "" {
			s.broker.Touch(req.SessionID)
		}

		// Subscribe owns the connection for its lifetime; everything else is a
		// single request/response exchange.
		if req.Method == MethodSubscribe {
			s.streamSubscription(conn, req.SessionID)
			return
		}

		if req.Method == MethodRegister {
			id, err := s.handleRegister(conn, req.Params)
			if err != nil {
				return
			}
			if id != "" {
				boundSession = id
			}
			continue
		}

		if err := s.dispatch(conn, req); err != nil {
			return
		}
	}
}

// handleRegister registers a session, writes the response, and returns the new
// session_id so the connection can bind it for reap-on-disconnect. An empty id
// with a nil error means a logical failure was reported to the client and the
// connection should continue.
func (s *Server) handleRegister(conn net.Conn, params json.RawMessage) (string, error) {
	result, err := s.doRegister(params)
	if err != nil {
		return "", s.writeError(conn, err)
	}
	var res RegisterResult
	_ = json.Unmarshal(result, &res)
	return res.SessionID, WriteFrame(conn, Response{Result: result})
}

// connClosed decrements the active count and fires OnIdle when it reaches zero.
func (s *Server) connClosed(conn net.Conn) {
	_ = conn.Close()
	if s.active.Add(-1) == 0 {
		s.mu.RLock()
		fn := s.onIdle
		s.mu.RUnlock()
		if fn != nil {
			fn()
		}
	}
}

// dispatch routes a single request to the broker and writes the response
// frame. The returned error signals an unrecoverable write failure that should
// tear down the connection.
func (s *Server) dispatch(conn net.Conn, req Request) error {
	result, err := s.invoke(req)
	if err != nil {
		return s.writeError(conn, err)
	}
	return WriteFrame(conn, Response{Result: result})
}

// invoke executes the broker call for a request and returns the marshaled
// result payload.
func (s *Server) invoke(req Request) (json.RawMessage, error) {
	switch req.Method {
	case MethodRegister:
		return s.doRegister(req.Params)
	case MethodUnregister:
		s.broker.UnregisterSession(req.SessionID)
		return marshalResult(struct{}{})
	case MethodListPeers:
		return marshalResult(ListPeersResult{Peers: s.broker.ListPeers(req.SessionID)})
	case MethodSend:
		return s.doSend(req.SessionID, req.Params)
	case MethodBroadcast:
		return s.doBroadcast(req.SessionID, req.Params)
	case MethodPoll:
		msgs, err := s.broker.Poll(req.SessionID)
		if err != nil {
			return nil, err
		}
		return marshalResult(PollResult{Messages: msgs})
	case MethodGetPeerInfo:
		return s.doGetPeerInfo(req.Params)
	default:
		return nil, errors.New("daemonrpc: unknown method: " + req.Method)
	}
}

// doRegister handles MethodRegister.
func (s *Server) doRegister(params json.RawMessage) (json.RawMessage, error) {
	var p RegisterParams
	if err := unmarshalParams(params, &p); err != nil {
		return nil, err
	}
	sess, err := s.broker.RegisterSession(p.ProjectPath)
	if err != nil {
		return nil, err
	}
	return marshalResult(RegisterResult{
		SessionID:   sess.ID,
		ProjectName: sess.ProjectName,
		ProjectPath: sess.ProjectPath,
	})
}

// doSend handles MethodSend.
func (s *Server) doSend(from string, params json.RawMessage) (json.RawMessage, error) {
	var p SendParams
	if err := unmarshalParams(params, &p); err != nil {
		return nil, err
	}
	id, err := s.broker.Send(broker.Message{
		From:         from,
		To:           p.To,
		Content:      p.Content,
		InReplyTo:    p.InReplyTo,
		ExpectsReply: p.ExpectsReply,
		Kind:         p.Kind,
		RequestID:    p.RequestID,
	})
	if err != nil {
		return nil, err
	}
	return marshalResult(SendResult{MessageID: id})
}

// doBroadcast handles MethodBroadcast.
func (s *Server) doBroadcast(from string, params json.RawMessage) (json.RawMessage, error) {
	var p BroadcastParams
	if err := unmarshalParams(params, &p); err != nil {
		return nil, err
	}
	n, err := s.broker.Broadcast(from, p.Content)
	if err != nil {
		return nil, err
	}
	return marshalResult(BroadcastResult{Recipients: n})
}

// doGetPeerInfo handles MethodGetPeerInfo via the broker's single-session
// accessor, which resolves a peer even if it is stale-but-not-yet-reaped.
func (s *Server) doGetPeerInfo(params json.RawMessage) (json.RawMessage, error) {
	var p GetPeerInfoParams
	if err := unmarshalParams(params, &p); err != nil {
		return nil, err
	}
	peer, ok := s.broker.Session(p.SessionID)
	if !ok {
		return nil, ErrPeerNotFound
	}
	return marshalResult(GetPeerInfoResult{
		SessionID:   peer.ID,
		ProjectName: peer.ProjectName,
		ProjectPath: peer.ProjectPath,
		LastSeen:    peer.LastSeen.Format(time.RFC3339),
	})
}

// streamSubscription forwards broker events for sessionID out as frames until
// the channel closes or the connection drops, then cancels the subscription.
func (s *Server) streamSubscription(conn net.Conn, sessionID string) {
	events, cancel := s.broker.Subscribe(sessionID)
	defer cancel()

	for ev := range events {
		if err := WriteFrame(conn, ev); err != nil {
			return
		}
	}
}

// writeError frames an error response. It returns any write failure so the
// caller can decide whether to tear down the connection.
func (s *Server) writeError(conn net.Conn, cause error) error {
	return WriteFrame(conn, Response{Error: cause.Error()})
}

// marshalResult marshals a result payload into raw JSON for a Response.
func marshalResult(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}

// unmarshalParams decodes request params into dst, tolerating an empty body.
func unmarshalParams(params json.RawMessage, dst any) error {
	if len(params) == 0 {
		return nil
	}
	return json.Unmarshal(params, dst)
}
