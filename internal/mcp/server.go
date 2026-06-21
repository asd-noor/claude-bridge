// Package mcp implements the stdio MCP JSON-RPC server that runs inside the
// claude-bridge shim. It speaks the Model Context Protocol on stdin/stdout to
// Claude and delegates every tool call to a daemonrpc.Client, injecting the
// shim's own session_id so Claude never handles it. Subscription events from the
// daemon are pushed to Claude as notifications/claude/channel frames, which wake
// an idle session. The shim also relays Claude Code permission prompts to peers
// (notifications/claude/channel/permission_request → a peer's verdict →
// notifications/claude/channel/permission).
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/asd-noor/claude-bridge/internal/broker"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// Protocol constants advertised during initialize.
const (
	jsonRPCVersion  = "2.0"
	protocolVersion = "2024-11-05"
	serverName      = "claude-bridge"
	serverVersion   = "0.1.0"
)

// MCP method names handled by the dispatch loop.
const (
	methodInitialize = "initialize"
	methodToolsList  = "tools/list"
	methodToolsCall  = "tools/call"
	methodPing       = "ping"

	// methodPermissionRequest is the inbound notification Claude Code sends when a
	// tool-approval dialog opens, for the shim to relay to a peer.
	methodPermissionRequest = "notifications/claude/channel/permission_request"
)

// JSON-RPC 2.0 error codes used in responses.
const (
	codeInvalidParams  = -32602
	codeMethodNotFound = -32601
	codeInternalError  = -32603
)

// channelMethod is the MCP frame used to push peer messages to Claude; it wakes
// an idle session rather than being logged.
const channelMethod = "notifications/claude/channel"

// permissionVerdictMethod is the frame the shim emits to its host to answer a
// relayed tool-approval prompt.
const permissionVerdictMethod = "notifications/claude/channel/permission"

// Capabilities.experimental keys declared in the initialize result.
const (
	experimentalChannelCapability    = "claude/channel"
	experimentalPermissionCapability = "claude/channel/permission"
)

// Permission verdict values.
const (
	behaviorAllow = "allow"
	behaviorDeny  = "deny"
)

// channelMeta keys carried on a channel notification. Values are strings only.
const (
	metaFrom         = "from"
	metaID           = "id"
	metaInReplyTo    = "in_reply_to"
	metaExpectsReply = "expects_reply"
	metaRequestID    = "request_id"
	metaKind         = "kind"
	metaTrue         = "true"
)

// Fields parsed from an inbound permission_request notification.
type permissionRequestParams struct {
	RequestID    string `json:"request_id"`
	ToolName     string `json:"tool_name"`
	Description  string `json:"description"`
	InputPreview string `json:"input_preview"`
}

// channelInstructions is the MCP initialize instructions string injected into
// Claude's system prompt for an opted-in session. It carries both the reactive
// protocol (how to handle inbound channel messages) and the proactive guidance
// (when to reach for the bridge) that the retired bridge-awareness skill held.
const channelInstructions = "You are connected to claude-bridge, a local bus linking Claude Code sessions " +
	"across projects on this machine. Peer messages arrive as " +
	"<channel source=\"claude-bridge\" from=\"...\" ...> blocks; source is always " +
	"\"claude-bridge\", so identify the sender by the from attribute (the peer's session_id). " +
	"REACTIVELY: when a message has expects_reply=\"true\" or an in_reply_to attribute, answer " +
	"by calling send_message with to=the from value and in_reply_to=the message id; otherwise " +
	"act on the content directly. A message with kind=\"permission_request\" is a peer asking you " +
	"to approve a tool call — decide, then call respond_permission with to=the from value, " +
	"request_id=the request_id attribute, and behavior=\"allow\" or \"deny\". " +
	"PROACTIVELY coordinate when your work affects others: before a breaking API/type/interface " +
	"change, when upgrading a shared dependency, when you import from a sibling repo, or when asked " +
	"to notify/coordinate. Use list_peers to see who is connected, broadcast (rate-limited — don't " +
	"loop) to announce a breaking change, and send_message with expects_reply=true to ask a specific " +
	"peer; poll_messages is manual catch-up. Be specific: name the symbol, file, and new signature."

// MCPRequest is an inbound JSON-RPC request. A nil ID marks a notification,
// which receives no response.
type MCPRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// MCPResponse is an outbound JSON-RPC reply. Exactly one of Result or Error is
// populated.
type MCPResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id"`
	Result  any       `json:"result,omitempty"`
	Error   *MCPError `json:"error,omitempty"`
}

// MCPError is a JSON-RPC error object.
type MCPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// paramError wraps a cause as an invalid-params error so dispatch can map it to
// the JSON-RPC -32602 code.
type paramError struct{ cause error }

func (e paramError) Error() string { return e.cause.Error() }

// errInvalidParams marks err as an invalid-params failure.
func errInvalidParams(err error) error { return paramError{cause: err} }

// Server is the stdio MCP JSON-RPC server. It owns no connection lifecycle: the
// cmd layer dials the daemon, registers to obtain sessionID, and constructs the
// server with a ready client.
type Server struct {
	client    daemonClient
	sessionID string
	tools     map[string]toolHandler
	inert     bool // true for a session that did not opt into the bridge

	mu  sync.Mutex // guards all writes to the stdout writer
	out io.Writer
}

// NewServer constructs an MCP server bound to a daemon client and the shim's
// session identity. The same sessionID is injected into every daemon RPC.
func NewServer(client *daemonrpc.Client, sessionID string) *Server {
	return &Server{
		client:    client,
		sessionID: sessionID,
		tools:     toolRegistry(),
	}
}

// NewInertServer constructs a Server for a session that did not opt into the
// bridge: it advertises no tools, declares no channel capability, and never
// touches the daemon. It exists only so Claude Code sees a connected MCP server
// rather than a failed one.
func NewInertServer() *Server {
	return &Server{inert: true}
}

// Serve runs the MCP read/dispatch loop, reading newline-delimited JSON
// requests from in and writing newline-delimited JSON responses to out. It
// returns when in reaches EOF, ctx is cancelled, or an unrecoverable write
// error occurs.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	s.mu.Lock()
	s.out = out
	s.mu.Unlock()

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		if err := s.handleLine(line, out); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

// handleLine parses one request line and dispatches it. Parse failures and
// handler errors are surfaced as JSON-RPC error responses; only write failures
// propagate up.
func (s *Server) handleLine(line []byte, out io.Writer) error {
	var req MCPRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return s.writeResponse(out, errorResponse(nil, codeInvalidParams, err.Error()))
	}

	resp, isNotification := s.dispatch(req)
	if isNotification {
		return nil
	}
	return s.writeResponse(out, resp)
}

// dispatch routes a request to its handler and returns the response to write.
// The second return is true for notifications (requests with no ID), which get
// no response.
func (s *Server) dispatch(req MCPRequest) (MCPResponse, bool) {
	// A relayed permission prompt arrives as a notification (no id); handle it
	// before the generic notification short-circuit below. An inert server never
	// declares the capability, so it ignores these.
	if !s.inert && req.Method == methodPermissionRequest {
		s.handlePermissionRequest(req.Params)
		return MCPResponse{}, true
	}
	if req.ID == nil {
		return MCPResponse{}, true
	}

	switch req.Method {
	case methodInitialize:
		return successResponse(req.ID, s.initializeResult()), false
	case methodToolsList:
		return successResponse(req.ID, s.toolsListResult()), false
	case methodToolsCall:
		return s.dispatchToolCall(req), false
	case methodPing:
		return successResponse(req.ID, map[string]any{}), false
	default:
		return errorResponse(req.ID, codeMethodNotFound, "method not found: "+req.Method), false
	}
}

// toolCallParams is the argument shape of a tools/call request.
type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// dispatchToolCall parses a tools/call request, runs the matching handler, and
// maps its result (or error) into an MCP response.
func (s *Server) dispatchToolCall(req MCPRequest) MCPResponse {
	var p toolCallParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return errorResponse(req.ID, codeInvalidParams, err.Error())
	}

	handler, ok := s.tools[p.Name]
	if !ok {
		return errorResponse(req.ID, codeMethodNotFound, "unknown tool: "+p.Name)
	}

	result, err := handler(s.client, s.sessionID, p.Arguments)
	if err != nil {
		return errorResponse(req.ID, errorCode(err), err.Error())
	}
	return successResponse(req.ID, toolResult(result))
}

// errorCode maps a handler error to a JSON-RPC error code. Invalid params get
// -32602; everything else — including rate-limit exhaustion, which arrives as a
// plain string over the RPC wire — is reported as an internal error (-32603).
func errorCode(err error) int {
	var pErr paramError
	if errors.As(err, &pErr) {
		return codeInvalidParams
	}
	return codeInternalError
}

// ForwardEvents pumps subscription events to stdout, in channel order with no
// buffering. It returns when events closes or ctx is cancelled. Writes share the
// server's output mutex so a pushed frame never interleaves with a tool response.
// A nil frame (e.g. a non-message event in channel mode) is dropped.
func (s *Server) ForwardEvents(ctx context.Context, events <-chan broker.Event, out io.Writer) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			frame := s.eventNotification(ev)
			if frame == nil {
				continue
			}
			if err := s.writeRaw(out, frame); err != nil {
				return
			}
		}
	}
}

// eventNotification builds the push frame for a broker event. Message events
// become notifications/claude/channel frames; a permission-verdict message
// instead becomes a notifications/claude/channel/permission frame for this shim's
// host. Non-message events (peer_joined/peer_left) produce nil and are dropped.
func (s *Server) eventNotification(ev broker.Event) map[string]any {
	msg, ok := messagePayload(ev)
	if !ok {
		return nil
	}
	if msg.Kind == broker.KindPermissionVerdict {
		return permissionVerdictFrame(msg)
	}
	return channelFrame(msg)
}

// channelFrame builds a notifications/claude/channel frame. The params carry the
// message body as content plus a string→string meta map identifying the sender,
// threading, and (for a relayed prompt) the permission request to answer.
func channelFrame(msg broker.Message) map[string]any {
	return map[string]any{
		"jsonrpc": jsonRPCVersion,
		"method":  channelMethod,
		"params": map[string]any{
			"content": msg.Content,
			"meta":    channelMeta(msg),
		},
	}
}

// permissionVerdictFrame builds the notifications/claude/channel/permission frame
// from a peer's verdict message, echoing its request_id and allow/deny content.
func permissionVerdictFrame(msg broker.Message) map[string]any {
	return map[string]any{
		"jsonrpc": jsonRPCVersion,
		"method":  permissionVerdictMethod,
		"params": map[string]any{
			"request_id": msg.RequestID,
			"behavior":   msg.Content,
		},
	}
}

// channelMeta builds the string→string meta map for a channel notification.
// in_reply_to is omitted when empty; expects_reply is included only when true;
// a relayed permission prompt carries its kind and request_id so Claude can
// answer it via respond_permission.
func channelMeta(msg broker.Message) map[string]string {
	meta := map[string]string{
		metaFrom: msg.From,
		metaID:   msg.ID,
	}
	if msg.InReplyTo != "" {
		meta[metaInReplyTo] = msg.InReplyTo
	}
	if msg.ExpectsReply {
		meta[metaExpectsReply] = metaTrue
	}
	if msg.Kind == broker.KindPermissionRequest {
		meta[metaKind] = broker.KindPermissionRequest
		meta[metaRequestID] = msg.RequestID
	}
	return meta
}

// handlePermissionRequest relays an inbound tool-approval prompt to every peer as
// a permission_request message. A peer answers with respond_permission, whose
// verdict the broker routes back here as a permission-verdict message. Relay is
// best-effort: peers that fail to receive are skipped, and the local terminal
// dialog still works.
func (s *Server) handlePermissionRequest(params json.RawMessage) {
	var p permissionRequestParams
	if err := json.Unmarshal(params, &p); err != nil || p.RequestID == "" {
		return
	}
	peers, err := s.listPeerIDs()
	if err != nil {
		return
	}
	content := permissionPrompt(p)
	for _, id := range peers {
		_, _ = s.client.CallAs(s.sessionID, daemonrpc.MethodSend, daemonrpc.SendParams{
			To:        id,
			Content:   content,
			Kind:      broker.KindPermissionRequest,
			RequestID: p.RequestID,
		})
	}
}

// listPeerIDs fetches the session_ids of the shim's current peers.
func (s *Server) listPeerIDs() ([]string, error) {
	raw, err := s.client.CallAs(s.sessionID, daemonrpc.MethodListPeers, struct{}{})
	if err != nil {
		return nil, err
	}
	var res daemonrpc.ListPeersResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(res.Peers))
	for _, p := range res.Peers {
		ids = append(ids, p.ID)
	}
	return ids, nil
}

// permissionPrompt renders a relayed tool-approval request into a peer-readable
// prompt. The shim's instructions tell the peer to answer via respond_permission.
func permissionPrompt(p permissionRequestParams) string {
	prompt := "A peer Claude session is asking you to approve a tool call: " + p.ToolName
	if p.Description != "" {
		prompt += " — " + p.Description
	}
	if p.InputPreview != "" {
		prompt += "\ninput: " + p.InputPreview
	}
	return prompt
}

// messagePayload extracts a broker.Message from an event payload. Events that
// arrive over the subscription socket are JSON-decoded, so the payload is a
// generic map rather than a typed Message; in-process events carry the value
// directly. Both shapes are normalized here.
func messagePayload(ev broker.Event) (broker.Message, bool) {
	if ev.Type != broker.EventMessage {
		return broker.Message{}, false
	}
	switch p := ev.Payload.(type) {
	case broker.Message:
		return p, true
	case map[string]any:
		var msg broker.Message
		raw, err := json.Marshal(p)
		if err != nil {
			return broker.Message{}, false
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			return broker.Message{}, false
		}
		return msg, true
	default:
		return broker.Message{}, false
	}
}

// writeResponse writes a JSON-RPC response as a newline-delimited frame.
func (s *Server) writeResponse(out io.Writer, resp MCPResponse) error {
	return s.writeRaw(out, resp)
}

// writeRaw marshals v to compact JSON and writes it followed by a newline,
// under the output mutex so concurrent writers never interleave a line.
func (s *Server) writeRaw(out io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = out.Write(data)
	return err
}

// initializeResult builds the initialize response: protocol version, server
// capabilities (tools plus the experimental channel + permission-relay
// capabilities), server info, and the instructions string injected into Claude's
// system prompt.
func (s *Server) initializeResult() map[string]any {
	serverInfo := map[string]any{"name": serverName, "version": serverVersion}
	if s.inert {
		return map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]any{},
			"serverInfo":      serverInfo,
		}
	}
	return map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities": map[string]any{
			"tools": map[string]any{},
			"experimental": map[string]any{
				experimentalChannelCapability:    map[string]any{},
				experimentalPermissionCapability: map[string]any{},
			},
		},
		"serverInfo":   serverInfo,
		"instructions": channelInstructions,
	}
}

// toolsListResult builds the tools/list response. An inert server exposes no
// tools.
func (s *Server) toolsListResult() map[string]any {
	if s.inert {
		return map[string]any{"tools": []Tool{}}
	}
	return map[string]any{"tools": Tools()}
}

// toolResult wraps a tool's JSON value as MCP tool-call content. The value is
// serialized into a single text content block.
func toolResult(value any) map[string]any {
	text, err := json.Marshal(value)
	if err != nil {
		text = []byte(`null`)
	}
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": string(text)},
		},
	}
}

// successResponse builds a result response for the given id.
func successResponse(id any, result any) MCPResponse {
	return MCPResponse{JSONRPC: jsonRPCVersion, ID: id, Result: result}
}

// errorResponse builds an error response for the given id.
func errorResponse(id any, code int, message string) MCPResponse {
	return MCPResponse{JSONRPC: jsonRPCVersion, ID: id, Error: &MCPError{Code: code, Message: message}}
}
