package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/asd-noor/claude-bridge/internal/broker"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// fakeClient records the last CallAs invocation and returns a canned result.
type fakeClient struct {
	lastSession string
	lastMethod  string
	lastParams  any
	result      json.RawMessage
	err         error
}

func (f *fakeClient) CallAs(sessionID, method string, params any) (json.RawMessage, error) {
	f.lastSession = sessionID
	f.lastMethod = method
	f.lastParams = params
	return f.result, f.err
}

// newTestServer builds a Server backed by a fake daemon client.
func newTestServer(c daemonClient) *Server {
	return &Server{client: c, sessionID: "sess-1", tools: toolRegistry()}
}

// runOne dispatches a single non-notification request and returns its response.
func runOne(t *testing.T, s *Server, req MCPRequest) MCPResponse {
	t.Helper()
	resp, isNotif := s.dispatch(req)
	if isNotif {
		t.Fatalf("unexpected notification for method %q", req.Method)
	}
	return resp
}

func TestInitialize(t *testing.T) {
	s := newTestServer(&fakeClient{})
	resp := runOne(t, s, MCPRequest{ID: 1, Method: methodInitialize})
	if resp.Error != nil {
		t.Fatalf("initialize error: %+v", resp.Error)
	}
	res := resp.Result.(map[string]any)
	if res["protocolVersion"] != protocolVersion {
		t.Fatalf("protocolVersion = %v", res["protocolVersion"])
	}
	caps := res["capabilities"].(map[string]any)
	if _, ok := caps["tools"]; !ok {
		t.Fatalf("tools capability not advertised: %v", caps)
	}
	exp, ok := caps["experimental"].(map[string]any)
	if !ok {
		t.Fatalf("experimental capability missing: %v", caps)
	}
	if _, ok := exp[experimentalChannelCapability]; !ok {
		t.Fatalf("claude/channel capability missing: %v", exp)
	}
	if _, ok := exp[experimentalPermissionCapability]; !ok {
		t.Fatalf("claude/channel/permission capability missing: %v", exp)
	}
	if res["instructions"] != channelInstructions {
		t.Fatalf("instructions = %v, want channel instructions", res["instructions"])
	}
}

func TestToolsList(t *testing.T) {
	s := newTestServer(&fakeClient{})
	resp := runOne(t, s, MCPRequest{ID: 2, Method: methodToolsList})
	res := resp.Result.(map[string]any)
	tools := res["tools"].([]Tool)
	if len(tools) != 6 {
		t.Fatalf("expected 6 tools, got %d", len(tools))
	}
}

func TestPing(t *testing.T) {
	s := newTestServer(&fakeClient{})
	resp := runOne(t, s, MCPRequest{ID: 3, Method: methodPing})
	if resp.Error != nil {
		t.Fatalf("ping error: %+v", resp.Error)
	}
}

func TestUnknownMethod(t *testing.T) {
	s := newTestServer(&fakeClient{})
	resp := runOne(t, s, MCPRequest{ID: 4, Method: "does/not/exist"})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("expected method-not-found, got %+v", resp.Error)
	}
}

func TestNotificationGetsNoResponse(t *testing.T) {
	s := newTestServer(&fakeClient{})
	_, isNotif := s.dispatch(MCPRequest{ID: nil, Method: methodPing})
	if !isNotif {
		t.Fatal("request with nil ID should be treated as a notification")
	}
}

func TestToolsCallSendMessage(t *testing.T) {
	fc := &fakeClient{result: json.RawMessage(`{"message_id":"m-9"}`)}
	s := newTestServer(fc)

	params, _ := json.Marshal(toolCallParams{
		Name:      ToolSendMessage,
		Arguments: json.RawMessage(`{"to":"peer-2","content":"hi","expects_reply":true}`),
	})
	resp := runOne(t, s, MCPRequest{ID: 5, Method: methodToolsCall, Params: params})
	if resp.Error != nil {
		t.Fatalf("tools/call error: %+v", resp.Error)
	}

	if fc.lastMethod != daemonrpc.MethodSend {
		t.Fatalf("daemon method = %q", fc.lastMethod)
	}
	if fc.lastSession != "sess-1" {
		t.Fatalf("session injection failed: %q", fc.lastSession)
	}
	sp := fc.lastParams.(daemonrpc.SendParams)
	if sp.To != "peer-2" || sp.Content != "hi" || !sp.ExpectsReply {
		t.Fatalf("bad params: %+v", sp)
	}

	content := resp.Result.(map[string]any)["content"].([]map[string]any)
	if !strings.Contains(content[0]["text"].(string), "m-9") {
		t.Fatalf("result content missing message_id: %v", content)
	}
}

func TestToolsCallUnknownTool(t *testing.T) {
	s := newTestServer(&fakeClient{})
	params, _ := json.Marshal(toolCallParams{Name: "nope"})
	resp := runOne(t, s, MCPRequest{ID: 6, Method: methodToolsCall, Params: params})
	if resp.Error == nil || resp.Error.Code != codeMethodNotFound {
		t.Fatalf("expected method-not-found for unknown tool, got %+v", resp.Error)
	}
}

func TestServeRoundTrip(t *testing.T) {
	fc := &fakeClient{result: json.RawMessage(`{"recipients":3}`)}
	s := newTestServer(fc)

	in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"broadcast","arguments":{"content":"hello"}}}` + "\n")
	var out bytes.Buffer

	if err := s.Serve(context.Background(), in, &out); err != nil {
		t.Fatalf("Serve: %v", err)
	}

	var resp MCPResponse
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &resp); err != nil {
		t.Fatalf("decode response: %v (%q)", err, out.String())
	}
	if resp.Error != nil {
		t.Fatalf("response error: %+v", resp.Error)
	}
	if fc.lastMethod != daemonrpc.MethodBroadcast {
		t.Fatalf("expected broadcast, got %q", fc.lastMethod)
	}
}

func TestForwardEvents(t *testing.T) {
	s := newTestServer(&fakeClient{})
	events := make(chan broker.Event, 3)
	events <- broker.Event{Type: broker.EventMessage, Payload: broker.Message{
		ID: "m1", From: "peer-a", Content: "hello",
	}}
	events <- broker.Event{Type: broker.EventMessage, Payload: broker.Message{
		ID: "m2", From: "peer-b", Content: "answer me", InReplyTo: "m1", ExpectsReply: true,
	}}
	// Non-message events (peer_joined/left) are dropped.
	events <- broker.Event{Type: broker.EventPeerJoined, Payload: map[string]any{"session": "peer-c"}}
	close(events)

	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ForwardEvents(ctx, events, &out)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 channel frames (peer_joined dropped), got %d: %q", len(lines), out.String())
	}

	type wireFrame struct {
		Method string `json:"method"`
		Params struct {
			Content string            `json:"content"`
			Meta    map[string]string `json:"meta"`
		} `json:"params"`
	}

	var f0 wireFrame
	if err := json.Unmarshal([]byte(lines[0]), &f0); err != nil {
		t.Fatalf("decode frame 0: %v", err)
	}
	if f0.Method != channelMethod {
		t.Fatalf("frame 0 method = %q, want %q", f0.Method, channelMethod)
	}
	if f0.Params.Content != "hello" {
		t.Fatalf("frame 0 content = %q", f0.Params.Content)
	}
	if f0.Params.Meta[metaFrom] != "peer-a" || f0.Params.Meta[metaID] != "m1" {
		t.Fatalf("frame 0 meta = %v", f0.Params.Meta)
	}
	if _, ok := f0.Params.Meta[metaInReplyTo]; ok {
		t.Fatalf("frame 0 in_reply_to should be omitted: %v", f0.Params.Meta)
	}
	if _, ok := f0.Params.Meta[metaExpectsReply]; ok {
		t.Fatalf("frame 0 expects_reply should be omitted: %v", f0.Params.Meta)
	}

	var f1 wireFrame
	if err := json.Unmarshal([]byte(lines[1]), &f1); err != nil {
		t.Fatalf("decode frame 1: %v", err)
	}
	if f1.Params.Meta[metaFrom] != "peer-b" || f1.Params.Meta[metaID] != "m2" {
		t.Fatalf("frame 1 meta = %v", f1.Params.Meta)
	}
	if f1.Params.Meta[metaInReplyTo] != "m1" {
		t.Fatalf("frame 1 in_reply_to = %q, want m1", f1.Params.Meta[metaInReplyTo])
	}
	if f1.Params.Meta[metaExpectsReply] != metaTrue {
		t.Fatalf("frame 1 expects_reply = %q, want %q", f1.Params.Meta[metaExpectsReply], metaTrue)
	}
}

func TestForwardPermissionRequestFrame(t *testing.T) {
	s := newTestServer(&fakeClient{})
	events := make(chan broker.Event, 1)
	events <- broker.Event{Type: broker.EventMessage, Payload: broker.Message{
		ID: "m1", From: "peer-a", Content: "approve Bash?",
		Kind: broker.KindPermissionRequest, RequestID: "abcde",
	}}
	close(events)

	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ForwardEvents(ctx, events, &out)

	var f struct {
		Method string `json:"method"`
		Params struct {
			Meta map[string]string `json:"meta"`
		} `json:"params"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &f); err != nil {
		t.Fatalf("decode frame: %v (%q)", err, out.String())
	}
	if f.Method != channelMethod {
		t.Fatalf("method = %q, want a channel frame", f.Method)
	}
	if f.Params.Meta[metaKind] != broker.KindPermissionRequest || f.Params.Meta[metaRequestID] != "abcde" {
		t.Fatalf("permission_request meta = %v", f.Params.Meta)
	}
}

func TestForwardPermissionVerdictFrame(t *testing.T) {
	s := newTestServer(&fakeClient{})
	events := make(chan broker.Event, 1)
	events <- broker.Event{Type: broker.EventMessage, Payload: broker.Message{
		ID: "v1", From: "peer-b", Content: behaviorAllow,
		Kind: broker.KindPermissionVerdict, RequestID: "abcde",
	}}
	close(events)

	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ForwardEvents(ctx, events, &out)

	var f struct {
		Method string `json:"method"`
		Params struct {
			RequestID string `json:"request_id"`
			Behavior  string `json:"behavior"`
		} `json:"params"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &f); err != nil {
		t.Fatalf("decode frame: %v (%q)", err, out.String())
	}
	if f.Method != permissionVerdictMethod {
		t.Fatalf("method = %q, want %q", f.Method, permissionVerdictMethod)
	}
	if f.Params.RequestID != "abcde" || f.Params.Behavior != behaviorAllow {
		t.Fatalf("verdict frame params = %+v", f.Params)
	}
}

func TestHandlePermissionRequestFansOutToPeers(t *testing.T) {
	fc := &fakeClient{result: json.RawMessage(`{"peers":[{"id":"peer-9"}]}`)}
	s := newTestServer(fc)

	params := json.RawMessage(`{"request_id":"abcde","tool_name":"Bash","description":"run ls","input_preview":"ls -la"}`)
	_, isNotif := s.dispatch(MCPRequest{ID: nil, Method: methodPermissionRequest, Params: params})
	if !isNotif {
		t.Fatal("permission_request must be handled as a notification")
	}

	// The last daemon call is the per-peer send carrying the relayed request.
	if fc.lastMethod != daemonrpc.MethodSend {
		t.Fatalf("last method = %q, want send", fc.lastMethod)
	}
	sp := fc.lastParams.(daemonrpc.SendParams)
	if sp.To != "peer-9" || sp.Kind != broker.KindPermissionRequest || sp.RequestID != "abcde" {
		t.Fatalf("relayed send params = %+v", sp)
	}
}
