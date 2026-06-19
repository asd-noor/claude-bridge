// Package mcp implements the stdio MCP JSON-RPC server that runs inside the
// claude-bridge shim. It speaks the Model Context Protocol on stdin/stdout to
// Claude and delegates every tool call to a daemonrpc.Client, injecting the
// shim's own session_id so Claude never handles it. Subscription events from
// the daemon are pushed back to Claude as notifications/message frames.
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
	loggerName      = "claude-bridge"
)

// MCP method names handled by the dispatch loop.
const (
	methodInitialize = "initialize"
	methodToolsList  = "tools/list"
	methodToolsCall  = "tools/call"
	methodPing       = "ping"
)

// JSON-RPC 2.0 error codes used in responses.
const (
	codeInvalidParams  = -32602
	codeMethodNotFound = -32601
	codeInternalError  = -32603
)

// notificationMethod is the MCP frame used to push daemon events to Claude.
const notificationMethod = "notifications/message"

// Notification levels per push intent.
const (
	levelInfo    = "info"
	levelWarning = "warning"
)

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
	if req.ID == nil {
		return MCPResponse{}, true
	}

	switch req.Method {
	case methodInitialize:
		return successResponse(req.ID, initializeResult()), false
	case methodToolsList:
		return successResponse(req.ID, toolsListResult()), false
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

// ForwardEvents pumps subscription events to stdout as notifications/message
// frames, in channel order with no buffering. It returns when events closes or
// ctx is cancelled. Writes share the server's output mutex so a pushed frame
// never interleaves with a tool response.
func (s *Server) ForwardEvents(ctx context.Context, events <-chan broker.Event, out io.Writer) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			frame := eventNotification(ev)
			if err := s.writeRaw(out, frame); err != nil {
				return
			}
		}
	}
}

// eventNotification builds a notifications/message frame for a broker event.
// The level is "warning" when the message threads a reply or asks for one.
func eventNotification(ev broker.Event) map[string]any {
	return map[string]any{
		"jsonrpc": jsonRPCVersion,
		"method":  notificationMethod,
		"params": map[string]any{
			"level":  eventLevel(ev),
			"logger": loggerName,
			"data":   eventData(ev),
		},
	}
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

// eventLevel chooses the notification level by message intent.
func eventLevel(ev broker.Event) string {
	if msg, ok := messagePayload(ev); ok {
		if msg.InReplyTo != "" || msg.ExpectsReply {
			return levelWarning
		}
	}
	return levelInfo
}

// eventData builds the message envelope surfaced to Claude. For message events
// it emits the full envelope; other event types pass their payload through.
func eventData(ev broker.Event) any {
	msg, ok := messagePayload(ev)
	if !ok {
		return map[string]any{"type": ev.Type, "payload": ev.Payload}
	}
	data := map[string]any{
		"type":    ev.Type,
		"id":      msg.ID,
		"from":    msg.From,
		"content": msg.Content,
	}
	if msg.InReplyTo != "" {
		data["in_reply_to"] = msg.InReplyTo
	}
	if msg.ExpectsReply {
		data["expects_reply"] = true
	}
	return data
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
// capabilities (advertising tools), and server info.
func initializeResult() map[string]any {
	return map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    serverName,
			"version": serverVersion,
		},
	}
}

// toolsListResult builds the tools/list response.
func toolsListResult() map[string]any {
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
