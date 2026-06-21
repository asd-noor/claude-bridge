# Architecture

`claude-bridge` is a local message broker that lets independent Claude Code
sessions on one machine coordinate with each other. It is a two-tier system: a
single shared **daemon** holds all state, and a short-lived **stdio MCP shim**
runs per Claude Code session, bridging that session to the daemon over a Unix
domain socket.

This document reflects the current implementation.

---

## Topology

```
┌──────────────────┐  stdio (MCP JSON-RPC)  ┌────────────────────┐
│  Claude Code A   │ ◄────────────────────► │ claude-bridge mcp  │
│  (project A)     │   (Claude spawns it)   │ (stdio shim, A)    │
└──────────────────┘                        └─────────┬──────────┘
                                                       │ UDS
                                                       ▼
                                            ┌────────────────────────┐
                                            │  claude-bridge serve    │
                                            │  (daemon, 1 per user)   │
                                            │  $TMPDIR/               │
                                            │    claude-bridge-UID/   │
                                            │      sock  (0600)       │
                                            └────────────────────────┘
                                                       ▲
                                                       │ UDS
┌──────────────────┐  stdio (MCP JSON-RPC)  ┌─────────┴──────────┐
│  Claude Code B   │ ◄────────────────────► │ claude-bridge mcp  │
│  (project B)     │                        │ (stdio shim, B)    │
└──────────────────┘                        └────────────────────┘
```

- **One daemon per user**, auto-spawned by the first shim, auto-exits after an
  idle period.
- **One shim per Claude Code session.** Session lifetime = shim process lifetime.
- The shim injects its own `session_id` into every daemon RPC; Claude never sees
  it.
- All shared state lives in the daemon, in memory, ephemeral.

---

## Package map

```
cmd/claude-bridge/      CLI + process lifecycle
  main.go               dispatch, --config/--log flags, version subcommand
  serve.go              daemon: flock, stale-socket recovery, detach, idle shutdown
  spawn.go              daemon auto-spawn (flock + fork/exec + poll-connect)
  mcp.go                stdio shim: opt-in gate (inert if not), connect, register, subscribe→stdout, MCP loop
  status.go             status / stop / install (launchd)

internal/broker/        the source of truth (transport-agnostic)
  broker.go             actor: run loop, command channel, session/inbox state
  pusher.go             per-session push delivery goroutine
  livelock.go           no-progress reply-chain breaker (per session-pair)
  message.go            Message (kind/request_id), Event types
  session.go            Session type, staleness

internal/daemonrpc/     UDS transport
  frame.go              length-prefixed JSON framing
  server.go             daemon side: listener, per-conn dispatch, wire structs
  client.go             shim side: Dial / Call / Subscribe / Close

internal/mcp/           stdio MCP JSON-RPC server (runs inside the shim)
  server.go             JSON-RPC loop, channel/permission frame forwarding + relay
  tools.go              6 tool handlers → daemon RPC
  schema.go             tool input schemas

internal/config/        YAML + env config, runtime path helpers
```

Layering is one-directional: `cmd` → {`mcp`, `daemonrpc`, `broker`, `config`};
`mcp` → `daemonrpc` → `broker`. The broker depends on nothing internal and knows
nothing about transport.

---

## The broker: actor model

The broker is a single goroutine that owns all mutable state. There are **no
mutexes**. Every public method posts a closure onto a command channel; the run
loop executes them serially.

```go
type Broker struct {
    cmds chan func(*state) // commands, executed serially by run()
    done chan struct{}     // closed by Close()
}
// state is loop-private: sessions (metadata + inbox + token bucket) and pushers.
```

- `do(fn)` posts a fire-and-forget command (e.g. `Touch`).
- `ask(b, fn)` posts a command and waits for its result (e.g. `Send`, `ListPeers`).
- Both select on `done`, so calls after `Close` return cleanly instead of hanging.

### Per-session pusher

Slow delivery must not block the loop, so each subscribed session has its own
**pusher** goroutine (`pusher.go`):

```
run loop ──(non-blocking enqueue)──► pusher.mailbox ──► pusher.forward ──► out ──► daemonrpc stream
```

- The run loop hands an event to a pusher's `mailbox` with a **non-blocking**
  try-send and moves on.
- The pusher drains its mailbox and writes to `out`, applying the tiered policy:
  a bounded **blocking** send (`blockingPushTimeout`, 100ms) for replies and
  reply-expecting messages, a **non-blocking** try-send otherwise (dropped on a
  full channel, recovered by `poll_messages`).
- The pusher is the **sole sender and sole closer** of `out`. No other goroutine
  ever touches it.

### Why this shape

| Property | Mechanism |
|---|---|
| No data races / no lock ordering | one goroutine owns the state |
| No send-on-closed-channel panic | each `out` channel has exactly one owner |
| No cross-session head-of-line blocking | the 100ms push runs in the pusher, not the loop |
| Per-recipient FIFO ordering | one pusher drains its mailbox in order |
| `Send` doesn't block the caller | loop hands off; the pusher absorbs the slow path |

The run loop never performs a blocking operation, so command callers never
deadlock on it.

---

## Message & session lifecycle

**Session.** The shim calls `register_session` on startup → the broker mints a
UUIDv7 `session_id`. Every inbound RPC carrying a `session_id` calls
`broker.Touch` to refresh `LastSeen` before dispatch — there is no heartbeat. A
session's lifetime is bound to its shim's connection: the daemon records the
`session_id` a connection uses and unregisters it the moment that connection
drops (clean exit or dirty `kill`), so the peer list reflects only live shims.
`SessionTTL` cleanup remains as a backstop for any session that somehow outlives
its connection.

**Delivery.** An ordinary message lands in the recipient's inbox (capacity
`MaxInboxSize`, oldest evicted). Two ways it reaches the receiving Claude:

- **Channel push (the only push path to the model).** The shim opens a second UDS
  connection, calls `subscribe`, and forwards each message event as a Claude Code
  `notifications/claude/channel` frame. Claude Code wraps it in a `<channel>` tag
  and **starts a turn on it even when the session is idle** — so a message reaches
  the model without the user typing and without the model polling. There is no
  longer a mode toggle: the shim always behaves as a channel. See **Channels**
  below for the wire format and its delivery guarantee.
- **Poll.** `poll_messages` lets the model drain its inbox explicitly — the manual
  fallback, used when channel push wasn't loaded (the session wasn't launched as a
  channel) or to sweep anything a dropped notification left queued.

Both paths read the same inbox, so a message is delivered once: whoever drains
first gets it. Broadcasts are rate-limited per sender with a token bucket
(`BroadcastBurst` / `BroadcastRefill`).

> **Earlier design (removed in 1.2.0).** Delivery previously relied on a
> `UserPromptSubmit`/`Stop` hook pair (`claude-bridge hook`) that injected the
> inbox as `additionalContext` and continued turns to drain it, guarded by a
> per-cwd continue budget, plus a `sessions/<sha256(cwd)>` map so the hook could
> resolve cwd→`session_id`. Channel push reaches an idle session directly, so the
> hooks, the continue budget, and the session map are all gone.

> **Removed in 2.0.0.** The `broker.channel_mode` config key, the
> `CLAUDE_BRIDGE_CHANNEL_MODE` env var, and the legacy `notifications/message`
> frame (which Claude Code treated as logging and never surfaced to the model) are
> all gone. Channels are now the sole push path — there is no flag. The Claude Code
> **plugin** and its `bridge-awareness` skill are also retired: the bridge is just
> the bare MCP server, and the skill's proactive guidance now rides in the server
> `instructions` (so it reaches only opted-in sessions — see below).

### Opt-in (bridge sessions only)

Registered user-scope, Claude Code spawns `claude-bridge mcp` for *every* session.
A session joins the bridge only when it opts in with `CLAUDE_BRIDGE_ENABLE` truthy
(set by the `cb` launch wrapper). Checked in `cmd/claude-bridge/mcp.go`:

- **Opted in** → the normal shim: register, dial the daemon, subscribe, advertise
  the tools, declare the channel + permission capabilities, inject `instructions`.
- **Not opted in** → an **inert** MCP server (`mcp.NewInertServer`): it answers
  `initialize`/`tools/list`/`ping` with no tools, no capabilities, and no
  instructions, and never touches the daemon. Claude Code sees a connected-but-empty
  server rather than a failed one, and the session never appears as a phantom peer.

So bridge participation — and the proactive coordination guidance in the
`instructions` — is a deliberate per-session choice, not an accident of having the
server registered.

### Livelock breaker

Two auto-replying sessions can ping-pong unattended, so the broker carries a
no-progress circuit breaker (`internal/broker/livelock.go`). It runs entirely on
the run loop, so it needs no synchronization.

- **What it counts.** For each (direction-independent) session pair it keeps a
  short window of recent normalized message bodies (window of `recentWindow`, 4 —
  enough to catch the classic two-distinct-echo loop where A and B alternate fixed
  bodies). A new body that normalized-equals one already in the window increments
  the pair's consecutive-repeat count; a genuinely new body resets it. The chain
  trips once the run **exceeds** `MaxChain`.
- **On trip.** The broker **stops pushing wakes** for that pair — the recipient is
  not woken — but the message **still lands in the inbox**, so there is no data
  loss and `poll_messages` still works. An idle gap longer than `ResetIdle` resets
  the chain; the cleanup tick (`reap`) prunes stale pair state.
- **Scope.** Only ordinary messages (`kind == ""`) are inboxed and breaker-counted.
  Permission-relay control messages (below) are push-only and exempt.
- **Known limitation.** Progress is inferred from changing content only — the
  broker can't see tool use — so a chain whose content keeps changing never trips.
  This is good enough for echo loops; templated-but-varying echoes could slip
  through.

The breaker is configured under `broker.livelock` (see **Configuration**);
`enabled: false` or `max_chain: 0` disables it.

### Permission relay

The shim can relay a Claude Code tool-approval prompt to peers so a peer's Claude
answers it. The capability `claude/channel/permission` is declared at initialize,
and `broker.Message` carries two extra fields for this flow: `kind`
(`""` | `permission_request` | `permission_verdict`) and `request_id` (the
correlation id); `daemonrpc.SendParams` mirrors them.

1. When Claude Code opens a tool-approval dialog it sends the shim a
   `notifications/claude/channel/permission_request` notification
   `{request_id, tool_name, description, input_preview}`.
2. The shim relays it to **every peer** as a `permission_request`-kind message
   carrying the `request_id` plus a human-readable prompt.
3. A peer surfaces it as a `<channel … kind="permission_request"
   request_id="…">` frame; that peer's Claude decides and calls the
   `respond_permission` tool `{to, request_id, behavior: "allow" | "deny"}`.
4. That sends a `permission_verdict`-kind message back to the origin shim, which
   emits `notifications/claude/channel/permission` `{request_id, behavior}` to its
   host (a permission frame, **not** a `<channel>` frame). The local terminal
   dialog also stays open; first answer wins (Claude Code validates `request_id`).

Permission-relay messages are push-only: they are never inboxed and are exempt
from the livelock breaker. Verdicts only ever originate from registered peer
sessions, since the broker only routes messages from registered sessions. This is
single-user scope; the relay broadcasts the prompt to all peers.

### Tools exposed to Claude

`list_peers`, `send_message` (`to`, `content`, `in_reply_to?`, `expects_reply?`),
`broadcast` (`content`), `poll_messages`, `get_peer_info` (`session_id`), and
`respond_permission` (`to`, `request_id`, `behavior`). The shim injects
`session_id` into each underlying daemon RPC.

### Channels

The shim **always** behaves as a channel; there is no mode toggle. Inbound message
events go out as Claude Code **Channel** notifications. The wiring is shim-side, in
`internal/mcp/server.go`: the broker, the daemon, and the per-session pusher are
unchanged, and `broker.Event` flows over the daemon→shim subscription socket.

- **Initialize.** The shim declares both
  `capabilities.experimental["claude/channel"] = {}` and
  `capabilities.experimental["claude/channel/permission"] = {}`, and sets a
  top-level `instructions` string (added to Claude's system prompt) so the model
  knows how to handle inbound channel and permission-relay messages.
- **Delivery frame.** Each inbound ordinary message is pushed as
  `notifications/claude/channel` with params `{ content: <body>, meta:
  <string→string> }`. Claude Code wraps these in a `<channel source="claude-bridge"
  …>` tag and **starts a turn on them even when the session is idle** — what lifts
  the idle-session limit.
- **`meta`.** Carries `from` (peer `session_id`), `id` (message id), optional
  `in_reply_to`, and `expects_reply="true"` when set; a relayed permission prompt
  additionally carries `kind="permission_request"` and `request_id`. Values are
  strings only. Claude Code auto-sets the `source` attribute to the server name
  (`claude-bridge`), so sender identity travels in the explicit `from` key, not
  `source`.
- **Delivery guarantee.** Channel notifications are fire-and-forget — **not**
  acknowledged. The notification await resolves on transport write, not on
  processing; if the session didn't load the server as a channel (or org policy
  blocks it), events are dropped silently. So the tiered "blocking" push policy
  (above) governs only the internal Go-channel hand-off, never delivery
  confirmation. The inbox + `poll_messages` remain the durable fallback — no data
  loss; messages still queue in the broker inbox.

**Launch (research preview).** The shim must be launched as a bare MCP server
behind Claude's dev flag:

```
claude --dangerously-load-development-channels server:claude-bridge
```

with the server registered in `~/.claude.json`:

```json
{ "mcpServers": { "claude-bridge": { "command": "claude-bridge", "args": ["mcp"] } } }
```

This is a preview contract that may change.

---

## Transport

Unix domain socket under a per-user runtime dir:

```
runtimeDir = $TMPDIR/claude-bridge-$UID   (mode 0700 — the access boundary)
  sock         (mode 0600)
  daemon.lock  (flock for the spawn/bind race)
  daemon.pid
  daemon.log   (when detached)
```

The `0700` directory mode is the security boundary: only the owning user can
`connect()`. Frames are length-prefixed JSON — `[4-byte big-endian uint32 len][N
bytes JSON]`, capped at `MaxFrameSize` (1 MiB). RPC requests/responses and pushed
events share the framing; subscriptions use a dedicated connection so events
don't interleave with RPC responses.

---

## Daemon lifecycle

**Lazy start.** When a shim can't connect, it takes an exclusive `flock` on
`daemon.lock`, re-checks (another shim may have won), then forks
`claude-bridge serve --detach` and polls `connect` (50ms, up to 2s). The detached
child re-execs itself with `CLAUDE_BRIDGE_DETACHED=1` and `Setsid`, ensures the
runtime dir exists, redirects stdout/stderr to `daemon.log`, and runs.

**Flock scope.** The lock guards only the startup **check-and-bind** critical
section and is released once the socket is bound; the **bound socket is the
liveness token** (stale-socket recovery dials it). A redundant `serve` therefore
dials the live socket and exits 0 rather than blocking on the lock forever.

**Stale-socket recovery.** On startup, if the socket file exists, the daemon
dials it: a live answer means another daemon owns it (exit 0); a failure means
the file is stale (remove and bind).

**Idle shutdown.** The daemon counts open UDS connections. When the count hits
zero it arms a timer (`idle_timeout`, default 15m, `0` disables); a new
connection cancels it. Graceful shutdown removes the socket and pid file **before**
closing the listener (closing it lets the process exit, which would otherwise race
the cleanup).

---

## Configuration

`~/.claude-bridge/config.yaml`, overlaid by `CLAUDE_BRIDGE_*` env vars (env wins).
A missing file is not an error; malformed YAML is. Durations are written as
strings (`"15m"`). Keys: `daemon.{runtime_dir, idle_timeout}`,
`broker.{message_ttl, session_ttl, max_inbox_size, cleanup_tick, broadcast_burst,
broadcast_refill, livelock.{enabled, max_chain, reset_idle}}`,
`log.{level, format}`.

`broker.livelock` tunes the no-progress reply-chain breaker (see **Livelock
breaker** above):

- `enabled` (bool, default `true`) — `false` disables the breaker entirely.
- `max_chain` (int, default `20`) — consecutive content-free exchanges allowed
  before a session pair trips; `0` also disables the breaker.
- `reset_idle` (duration, default `"60s"`) — an idle gap longer than this clears a
  pair's chain.

---

## Build & versioning

`mise run build` derives the version from git (exact tag if HEAD is tagged, else
short commit, `-dirty` for an uncommitted tree) and injects it via
`-ldflags "-X main.version=..."`. `claude-bridge version` prints it. `mise run
install` builds and copies the binary to `~/.local/bin`.

---

## Testing

- **Unit** (`broker`, `config`, `frame`): state transitions, inbox eviction,
  broadcast rate limit, TTL via an injected clock, framing round-trip/oversize/
  truncation. Broker concurrency stress tests run under `-race` and exercise
  send/subscribe/unregister/reap interleavings.
- **Integration** (`internal/daemonrpc`, build tag `integration`): an in-process
  daemon on a temp socket driven by the real client — two-client exchange (poll
  and subscribe), broadcast fan-out + rate limit, threaded reply, idle shutdown
  fire/cancel.
- **End-to-end:** the real binary — cold-start auto-spawn, the 5-shim flock race
  (exactly one daemon survives), and a two-shim `send_message` →
  `notifications/claude/channel` round trip.

---

## Extension points

| Want | Where |
|---|---|
| Persistence across restarts | a `Store` interface behind the broker state; file-backed SQLite suits the single-writer loop |
| Message history / replay | buffer per session; add a `get_history` RPC |
| LAN / multi-machine | a TCP listener beside the UDS one; heartbeats become load-bearing |
| Richer presence | push `peer_joined` / `peer_left` already exist; extend payloads |
| Always-on daemon | `claude-bridge install` (launchd) + `idle_timeout: 0` |
