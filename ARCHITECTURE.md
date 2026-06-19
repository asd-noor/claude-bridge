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
  mcp.go                stdio shim: connect, register, subscribe→stdout, MCP loop
  status.go             status / stop / install (launchd)
  hook.go               UserPromptSubmit hook: inject pending peer messages

internal/broker/        the source of truth (transport-agnostic)
  broker.go             actor: run loop, command channel, session/inbox state
  pusher.go             per-session push delivery goroutine
  message.go            Message, Event types
  session.go            Session type, staleness

internal/daemonrpc/     UDS transport
  frame.go              length-prefixed JSON framing
  server.go             daemon side: listener, per-conn dispatch, wire structs
  client.go             shim side: Dial / Call / Subscribe / Close

internal/mcp/           stdio MCP JSON-RPC server (runs inside the shim)
  server.go             JSON-RPC loop, notifications/message forwarding
  tools.go              5 tool handlers → daemon RPC
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
`broker.Touch` to refresh `LastSeen` before dispatch — there is no heartbeat. On
a clean shim exit the shim calls `unregister_session`; a dirty exit leaves the
session to be reaped after `SessionTTL` by the run loop's cleanup tick.

**Delivery.** Every message lands in the recipient's inbox (capacity
`MaxInboxSize`, oldest evicted). Three ways it reaches the receiving Claude:

- **Hook auto-inject (primary path to the model).** The plugin registers a
  `UserPromptSubmit` hook that runs `claude-bridge hook`. On each turn the hook
  resolves the session owning the cwd (via the session-map file, below), drains
  that inbox from the daemon, and returns the pending messages as
  `additionalContext` — so they appear in the model's context automatically,
  without the model calling a tool. This is the only path that reliably surfaces
  a message to the LLM.
- **Subscribe / push.** The shim opens a second UDS connection, calls `subscribe`,
  and forwards each event as an MCP `notifications/message` frame (level `warning`
  when `in_reply_to` / `expects_reply` is set, else `info`). Note: Claude Code
  treats these as logging and does **not** surface them to the model, so this path
  is effectively a no-op for the LLM today; the broker's push machinery remains as
  transport that a notification-aware host could use.
- **Poll.** `poll_messages` lets the model drain its inbox explicitly — the manual
  fallback, and what the hook calls under the hood.

Because all three read the same inbox, a message is delivered once: whoever drains
first (usually the hook) gets it. Broadcasts are rate-limited per sender with a
token bucket (`BroadcastBurst` / `BroadcastRefill`).

**Limit.** A fully idle interactive session cannot be *woken* — Claude Code only
acts on a user turn (or a hook firing on one). The hook makes messages appear the
moment the user next interacts; true unattended delivery needs a background
(`claude -p`) receiver that polls.

**Session map.** So the hook (a separate process) can find the right inbox, the
shim writes `runtimeDir/sessions/<sha256(cwd)>` = its `session_id` on register and
removes it on exit. The hook reads it by cwd. Two sessions in the same cwd collide
on this key (last writer wins) — rare, and only degrades the auto-inject hook.

### Tools exposed to Claude

`list_peers`, `send_message` (`to`, `content`, `in_reply_to?`, `expects_reply?`),
`broadcast` (`content`), `poll_messages`, `get_peer_info` (`session_id`). The
shim injects `session_id` into each underlying daemon RPC.

---

## Transport

Unix domain socket under a per-user runtime dir:

```
runtimeDir = $TMPDIR/claude-bridge-$UID   (mode 0700 — the access boundary)
  sock         (mode 0600)
  daemon.lock  (flock for the spawn/bind race)
  daemon.pid
  daemon.log   (when detached)
  sessions/    (per-cwd session_id maps for the prompt hook)
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
broadcast_refill}`, `log.{level, format}`.

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
  `notifications/message` round trip.

---

## Extension points

| Want | Where |
|---|---|
| Persistence across restarts | a `Store` interface behind the broker state; file-backed SQLite suits the single-writer loop |
| Message history / replay | buffer per session; add a `get_history` RPC |
| LAN / multi-machine | a TCP listener beside the UDS one; heartbeats become load-bearing |
| Richer presence | push `peer_joined` / `peer_left` already exist; extend payloads |
| Always-on daemon | `claude-bridge install` (launchd) + `idle_timeout: 0` |
