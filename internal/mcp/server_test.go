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
}

func TestToolsList(t *testing.T) {
	s := newTestServer(&fakeClient{})
	resp := runOne(t, s, MCPRequest{ID: 2, Method: methodToolsList})
	res := resp.Result.(map[string]any)
	tools := res["tools"].([]Tool)
	if len(tools) != 5 {
		t.Fatalf("expected 5 tools, got %d", len(tools))
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

func TestForwardEventsLevels(t *testing.T) {
	s := newTestServer(&fakeClient{})
	events := make(chan broker.Event, 2)
	events <- broker.Event{Type: broker.EventMessage, Payload: broker.Message{
		ID: "1", From: "a", Content: "fyi",
	}}
	events <- broker.Event{Type: broker.EventMessage, Payload: broker.Message{
		ID: "2", From: "a", Content: "answer me", ExpectsReply: true,
	}}
	close(events)

	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ForwardEvents(ctx, events, &out)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 notification frames, got %d: %q", len(lines), out.String())
	}

	levels := make([]string, 0, 2)
	for _, ln := range lines {
		var frame struct {
			Method string `json:"method"`
			Params struct {
				Level string `json:"level"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(ln), &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		if frame.Method != notificationMethod {
			t.Fatalf("frame method = %q", frame.Method)
		}
		levels = append(levels, frame.Params.Level)
	}
	if levels[0] != levelInfo || levels[1] != levelWarning {
		t.Fatalf("levels = %v, want [info warning]", levels)
	}
}
