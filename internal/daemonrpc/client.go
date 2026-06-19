package daemonrpc

import (
	"encoding/json"
	"errors"
	"net"
	"sync"

	"github.com/asd-noor/claude-bridge/internal/broker"
)

// Client is the shim-side handle to a daemon connection. One connection
// serializes calls; Call is guarded by a mutex so concurrent callers do not
// interleave request and response frames on the same socket.
type Client struct {
	conn net.Conn
	mu   sync.Mutex
}

// Dial connects to the daemon's Unix domain socket.
func Dial(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// Call sends a request frame and returns the single response result. A
// non-empty Error in the response is surfaced as a Go error.
func (c *Client) Call(method string, params any) (json.RawMessage, error) {
	return c.callWithSession(method, "", params)
}

// CallAs is Call with an explicit session identity injected into the request.
func (c *Client) CallAs(sessionID, method string, params any) (json.RawMessage, error) {
	return c.callWithSession(method, sessionID, params)
}

// callWithSession performs the framed request/response exchange under the
// connection mutex.
func (c *Client) callWithSession(method, sessionID string, params any) (json.RawMessage, error) {
	raw, err := encodeParams(params)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := WriteFrame(c.conn, Request{Method: method, SessionID: sessionID, Params: raw}); err != nil {
		return nil, err
	}

	respBytes, err := ReadFrame(c.conn)
	if err != nil {
		return nil, err
	}

	var resp Response
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp.Result, nil
}

// Subscribe issues a subscribe request on this connection then streams broker
// events into the returned channel until the connection closes. This connection
// is dedicated to the subscription; callers Dial a separate Client for RPC.
func (c *Client) Subscribe(sessionID string) (<-chan broker.Event, error) {
	raw, err := encodeParams(struct{}{})
	if err != nil {
		return nil, err
	}
	if err := WriteFrame(c.conn, Request{Method: MethodSubscribe, SessionID: sessionID, Params: raw}); err != nil {
		return nil, err
	}

	events := make(chan broker.Event)
	go c.readEvents(events)
	return events, nil
}

// readEvents decodes event frames off the connection into out, closing it when
// the stream ends.
func (c *Client) readEvents(out chan<- broker.Event) {
	defer close(out)
	for {
		raw, err := ReadFrame(c.conn)
		if err != nil {
			return
		}
		var ev broker.Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			return
		}
		out <- ev
	}
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// encodeParams marshals call params, treating nil as an empty body.
func encodeParams(params any) (json.RawMessage, error) {
	if params == nil {
		return nil, nil
	}
	return json.Marshal(params)
}
