package mcp

import (
	"encoding/json"
	"time"

	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// daemonClient is the subset of *daemonrpc.Client the tool handlers depend on.
// Depending on the interface (not the concrete type) keeps the server testable
// with a fake client.
type daemonClient interface {
	CallAs(sessionID, method string, params any) (json.RawMessage, error)
}

// toolHandler executes one tool: it parses Claude-supplied arguments, performs
// the daemon RPC as the given session, and returns the JSON value to surface as
// the tool result content.
type toolHandler func(c daemonClient, sessionID string, args json.RawMessage) (any, error)

// toolRegistry maps each MCP tool name to its handler.
func toolRegistry() map[string]toolHandler {
	return map[string]toolHandler{
		ToolListPeers:    handleListPeers,
		ToolSendMessage:  handleSendMessage,
		ToolBroadcast:    handleBroadcast,
		ToolPollMessages: handlePollMessages,
		ToolGetPeerInfo:  handleGetPeerInfo,
	}
}

// peerView is the Claude-facing shape of a peer.
type peerView struct {
	SessionID   string `json:"session_id"`
	ProjectName string `json:"project_name"`
	ProjectPath string `json:"project_path"`
	LastSeen    string `json:"last_seen"`
}

// messageView is the Claude-facing shape of an inbox message.
type messageView struct {
	ID           string `json:"id"`
	From         string `json:"from"`
	Content      string `json:"content"`
	CreatedAt    string `json:"created_at"`
	InReplyTo    string `json:"in_reply_to,omitempty"`
	ExpectsReply bool   `json:"expects_reply,omitempty"`
}

// handleListPeers maps MethodListPeers to a peer array.
func handleListPeers(c daemonClient, sessionID string, _ json.RawMessage) (any, error) {
	raw, err := c.CallAs(sessionID, daemonrpc.MethodListPeers, struct{}{})
	if err != nil {
		return nil, err
	}
	var res daemonrpc.ListPeersResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	peers := make([]peerView, 0, len(res.Peers))
	for _, p := range res.Peers {
		peers = append(peers, peerView{
			SessionID:   p.ID,
			ProjectName: p.ProjectName,
			ProjectPath: p.ProjectPath,
			LastSeen:    p.LastSeen.Format(time.RFC3339),
		})
	}
	return peers, nil
}

// sendArgs are the arguments accepted by send_message.
type sendArgs struct {
	To           string `json:"to"`
	Content      string `json:"content"`
	InReplyTo    string `json:"in_reply_to"`
	ExpectsReply bool   `json:"expects_reply"`
}

// handleSendMessage maps MethodSend to {message_id}.
func handleSendMessage(c daemonClient, sessionID string, args json.RawMessage) (any, error) {
	a, err := decodeArgs[sendArgs](args)
	if err != nil {
		return nil, errInvalidParams(err)
	}
	raw, err := c.CallAs(sessionID, daemonrpc.MethodSend, daemonrpc.SendParams{
		To:           a.To,
		Content:      a.Content,
		InReplyTo:    a.InReplyTo,
		ExpectsReply: a.ExpectsReply,
	})
	if err != nil {
		return nil, err
	}
	var res daemonrpc.SendResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return map[string]any{"message_id": res.MessageID}, nil
}

// broadcastArgs are the arguments accepted by broadcast.
type broadcastArgs struct {
	Content string `json:"content"`
}

// handleBroadcast maps MethodBroadcast to {sent_to}.
func handleBroadcast(c daemonClient, sessionID string, args json.RawMessage) (any, error) {
	a, err := decodeArgs[broadcastArgs](args)
	if err != nil {
		return nil, errInvalidParams(err)
	}
	raw, err := c.CallAs(sessionID, daemonrpc.MethodBroadcast, daemonrpc.BroadcastParams{Content: a.Content})
	if err != nil {
		return nil, err
	}
	var res daemonrpc.BroadcastResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return map[string]any{"sent_to": res.Recipients}, nil
}

// handlePollMessages maps MethodPoll to a message array.
func handlePollMessages(c daemonClient, sessionID string, _ json.RawMessage) (any, error) {
	raw, err := c.CallAs(sessionID, daemonrpc.MethodPoll, struct{}{})
	if err != nil {
		return nil, err
	}
	var res daemonrpc.PollResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	msgs := make([]messageView, 0, len(res.Messages))
	for _, m := range res.Messages {
		msgs = append(msgs, messageView{
			ID:           m.ID,
			From:         m.From,
			Content:      m.Content,
			CreatedAt:    m.CreatedAt.Format(time.RFC3339),
			InReplyTo:    m.InReplyTo,
			ExpectsReply: m.ExpectsReply,
		})
	}
	return msgs, nil
}

// getPeerInfoArgs are the arguments accepted by get_peer_info.
type getPeerInfoArgs struct {
	SessionID string `json:"session_id"`
}

// handleGetPeerInfo maps MethodGetPeerInfo to a single peer object.
func handleGetPeerInfo(c daemonClient, sessionID string, args json.RawMessage) (any, error) {
	a, err := decodeArgs[getPeerInfoArgs](args)
	if err != nil {
		return nil, errInvalidParams(err)
	}
	raw, err := c.CallAs(sessionID, daemonrpc.MethodGetPeerInfo, daemonrpc.GetPeerInfoParams{SessionID: a.SessionID})
	if err != nil {
		return nil, err
	}
	var res daemonrpc.GetPeerInfoResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	return peerView{
		SessionID:   res.SessionID,
		ProjectName: res.ProjectName,
		ProjectPath: res.ProjectPath,
		LastSeen:    res.LastSeen,
	}, nil
}

// decodeArgs unmarshals tool arguments into T, tolerating an empty body.
func decodeArgs[T any](args json.RawMessage) (T, error) {
	var v T
	if len(args) == 0 {
		return v, nil
	}
	err := json.Unmarshal(args, &v)
	return v, err
}
