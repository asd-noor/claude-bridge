package mcp

// Tool names exposed to Claude over MCP. These are the public, session-id-free
// surface; the shim injects its own session_id into the underlying daemon RPC.
const (
	ToolListPeers    = "list_peers"
	ToolSendMessage  = "send_message"
	ToolBroadcast    = "broadcast"
	ToolPollMessages = "poll_messages"
	ToolGetPeerInfo  = "get_peer_info"
)

// Tool describes a single MCP tool: its name, a human-readable description, and
// a JSON Schema object constraining its arguments.
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// objectSchema builds a JSON Schema "object" with the given properties and
// required field list. A nil required slice yields an empty required array.
func objectSchema(properties map[string]any, required []string) map[string]any {
	if required == nil {
		required = []string{}
	}
	return map[string]any{
		"type":       "object",
		"properties": properties,
		"required":   required,
	}
}

// emptyObjectSchema is the schema for tools that take no arguments.
func emptyObjectSchema() map[string]any {
	return objectSchema(map[string]any{}, nil)
}

// stringProp builds a JSON Schema string property with a description.
func stringProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

// boolProp builds a JSON Schema boolean property with a description.
func boolProp(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}

// Tools returns the full set of MCP tool definitions advertised to Claude.
func Tools() []Tool {
	return []Tool{
		{
			Name:        ToolListPeers,
			Description: "List the other Claude sessions currently connected to the bridge. Returns each peer's session_id, project_name, project_path, and last_seen (ISO8601).",
			InputSchema: emptyObjectSchema(),
		},
		{
			Name:        ToolSendMessage,
			Description: "Send a directed message to one peer by session_id. Set in_reply_to when answering a peer's message to thread the conversation; set expects_reply when you want an answer back, which surfaces the peer's notification as a high-signal warning.",
			InputSchema: objectSchema(
				map[string]any{
					"to":            stringProp("Recipient peer's session_id."),
					"content":       stringProp("Message body to deliver to the peer."),
					"in_reply_to":   stringProp("Optional id of the message you are answering; threads the conversation."),
					"expects_reply": boolProp("Optional. Set true to signal 'please answer'; the peer is notified as a warning."),
				},
				[]string{"to", "content"},
			),
		},
		{
			Name:        ToolBroadcast,
			Description: "Broadcast content to every connected peer. Use sparingly: every connected Claude instance processes it. Rate-limited per-sender (token bucket); exhausting the bucket returns an error, so back off and prefer a targeted send_message instead.",
			InputSchema: objectSchema(
				map[string]any{
					"content": stringProp("Message body to fan out to all peers."),
				},
				[]string{"content"},
			),
		},
		{
			Name:        ToolPollMessages,
			Description: "Drain and return this session's pending inbox. Clears the inbox on read. Manual catch-up for cases where push delivery was interrupted (e.g. just after a daemon restart); normal traffic arrives via push notifications.",
			InputSchema: emptyObjectSchema(),
		},
		{
			Name:        ToolGetPeerInfo,
			Description: "Look up a single peer by session_id. Returns the peer's session_id, project_name, project_path, and last_seen (ISO8601).",
			InputSchema: objectSchema(
				map[string]any{
					"session_id": stringProp("The peer session_id to look up."),
				},
				[]string{"session_id"},
			),
		},
	}
}
