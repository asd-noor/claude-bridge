# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v2.0.0] - 2026-06-21

Channels are still a Claude Code **research preview**; the launch contract may
change.

### Added

- **Opt-in bridge sessions.** A session joins the bridge only when launched with
  `CLAUDE_BRIDGE_ENABLE=1` (set by the `cb` wrapper). Otherwise the shim serves an
  inert MCP server — no daemon, no registration, no peer entry, no tools — so
  ordinary sessions no longer pollute the peer list or auto-spawn the daemon.
- **Livelock breaker (in the broker).** A no-progress reply-chain circuit breaker
  trips when two sessions exchange more than `max_chain` consecutive content-free
  messages (a new body that normalized-equals one of the last few seen for that
  pair, catching alternating echoes). On trip the broker **stops waking** the
  recipient for that pair, but the message still lands in the inbox — no data loss,
  `poll_messages` still works. An idle gap longer than `reset_idle` resets the
  chain. Configured under `broker.livelock.{enabled, max_chain, reset_idle}`
  (defaults `true` / `20` / `60s`); `enabled: false` or `max_chain: 0` disables it.
  Progress is inferred from changing content only (the broker can't see tool use).
- **Permission relay.** When Claude Code opens a tool-approval dialog, the shim
  relays the prompt to peers (capability `claude/channel/permission`). A peer's
  Claude answers, and the verdict flows back to the requesting session, which emits
  it to its host as a `notifications/claude/channel/permission` frame. The local
  terminal dialog stays open; first answer wins. `broker.Message` and
  `daemonrpc.SendParams` gained `kind` + `request_id` fields to carry the flow.
- **`respond_permission` tool** (`to`, `request_id`, `behavior: "allow" | "deny"`)
  for answering a relayed permission prompt — the MCP surface is now six tools.

### Changed

- **BREAKING — channels are the only push path.** The shim always behaves as a
  channel; inbound messages always push as `notifications/claude/channel` frames.
  There is no longer a flag to turn this off.

### Removed

- **BREAKING — the `broker.channel_mode` config key and the
  `CLAUDE_BRIDGE_CHANNEL_MODE` env var.** There is no channel-mode toggle anymore.
- **BREAKING — the legacy `notifications/message` frame** (and its info/warning
  levels), which Claude Code treated as logging and never surfaced to the model.
- **BREAKING — the Claude Code plugin and the `bridge-awareness` skill** (and the
  `plugin/` + `.claude-plugin/marketplace.json` packaging). The bridge is now just
  the bare MCP server registered in `~/.claude.json`; the skill's proactive
  coordination guidance moved into the server `instructions`, which reach only
  opted-in sessions. Uninstall the old plugin: `/plugin uninstall claude-bridge@claude-bridge`
  then `/plugin marketplace remove claude-bridge`.

## [v1.2.0] - 2026-06-21

### Added

- **Automatic delivery via Claude Code channels.** The shim pushes inbound peer
  messages as `notifications/claude/channel` events, which start a turn even in a
  **fully idle** session — lifting the long-standing "an idle session cannot be
  woken" limitation. Launch a session as a channel with
  `claude --dangerously-load-development-channels server:claude-bridge`. Controlled
  by `broker.channel_mode` (env `CLAUDE_BRIDGE_CHANNEL_MODE`), now default `true`.

### Removed

- **The `UserPromptSubmit`/`Stop` hooks and the `claude-bridge hook` subcommand.**
  Channel push reaches the model directly, so the hooks, the per-cwd continue
  budget (`sessions/<hash>.cont`), and the `runtimeDir/sessions/` session map are
  all gone. `poll_messages` remains as the manual fallback. The plugin is now
  skill-only (the MCP server is registered separately); its bundled `.mcp.json`
  and `hooks/` were removed.

### Known gaps

- Channel-mode reply chains have **no livelock guard** yet (the continue budget
  only governed the removed hook path). A no-progress circuit breaker in the broker
  is planned. Avoid unattended two-session auto-reply loops until then.

## [v1.1.3] - 2026-06-19

### Fixed

- Reap a session only when the connection that **registered** it drops, not when
  any connection referencing the `session_id` closes. The v1.1.1 reap bound to
  every session-bearing frame, so the prompt hook's short-lived poll connection
  unregistered its own still-live session on close — peers vanished mid-use
  ("unknown session" on send). Ephemeral connections (hook poll, `status`,
  subscribe stream) now reap nothing.

## [v1.1.2] - 2026-06-19

### Fixed

- `claude-bridge stop` (and SIGTERM/SIGINT) now shut the daemon down even when
  shims are connected. The graceful-shutdown active-connection guard was meant
  only for the idle timer but also blocked the signal path, so an explicit stop
  was a no-op whenever any session was attached. Signals now force shutdown; only
  the idle timer keeps the guard.

## [v1.1.1] - 2026-06-19

### Fixed

- Reap a session as soon as its shim's connection drops, instead of waiting for
  `SessionTTL`. Dirty shim exits (e.g. a plugin reload killing the process with no
  clean unregister) no longer leave stale "zombie" peers in `list_peers`; the peer
  list now reflects only live shims.

## [v1.1.0] - 2026-06-19

### Added

- **Automatic message delivery via a `UserPromptSubmit` hook.** The plugin now
  ships a hook running `claude-bridge hook`, which injects pending peer messages
  into the receiving session's context on each turn — no manual `poll_messages`.
  This works around Claude Code not surfacing MCP `notifications/message` to the
  model.
- `claude-bridge hook` subcommand: resolves the session for the current
  directory and emits queued messages as hook context.
- The shim now records a `runtimeDir/sessions/<hash(cwd)>` → `session_id` map so
  the hook can find the right inbox; removed on shim exit.
- **`Stop` hook continue-on-pending.** The same `claude-bridge hook` is also wired
  to `Stop`: when a turn ends with peer messages pending, it continues the turn so
  an active session keeps processing without a new prompt — making an active
  session near-autonomous. A per-session continue budget (default 5, reset on each
  user turn) caps consecutive auto-continues to break reply loops between two
  auto-replying agents.

### Notes

- Once a session is active, arriving messages are handled autonomously at each turn
  boundary (via the `Stop` hook) up to the continue budget. A fully idle session
  still can't be *woken* — Claude Code only acts on a turn; the hooks surface
  messages the moment the session next takes one. Fully unattended delivery needs a
  background (`claude -p`) receiver.

## [v1.0.1] - 2026-06-19

### Added

- Plugin marketplace manifest (`.claude-plugin/marketplace.json`) so the bundled
  plugin — the MCP shim plus the `bridge-awareness` skill — installs via
  `/plugin marketplace add <repo>` then `/plugin install claude-bridge@claude-bridge`.
- `go install` support: the `version` subcommand falls back to the module build
  info (`runtime/debug`), so `go install github.com/asd-noor/claude-bridge/cmd/claude-bridge@vX.Y.Z`
  reports the module version instead of `dev`.

### Documentation

- README: document the plugin-marketplace and `go install` installation paths.

## [v1.0.0] - 2026-06-19

First stable release.

### Added

- **Daemon** (`claude-bridge serve`): a single per-user message broker holding
  all session and inbox state in memory. Built on an actor-model broker — one
  goroutine owns the state, with a per-session pusher for push delivery, so it
  is lock-free and free of channel-close races.
- **Stdio MCP shim** (`claude-bridge mcp`): one per Claude Code session, exposing
  five tools — `list_peers`, `send_message`, `broadcast`, `poll_messages`,
  `get_peer_info`. The shim injects the `session_id` into every daemon call.
- **Push delivery** via MCP `notifications/message`, with the notification level
  chosen by intent (`warning` for replies / reply-expecting messages, `info`
  otherwise); `poll_messages` is the catch-up fallback.
- **UDS transport** with length-prefixed JSON framing, under a per-user runtime
  directory (`$TMPDIR/claude-bridge-$UID`, mode 0700).
- **Daemon lifecycle**: lazy auto-spawn behind an flock (the bound socket is the
  liveness token), idle shutdown, stale-socket recovery, and graceful shutdown
  that removes the socket and pid file before closing the listener.
- **CLI**: `status`, `stop`, `install` (launchd agent on macOS), and `version`
  (derived from the git tag, injected at build time).
- **Claude Code plugin**: the `bridge-awareness` skill and bundled `.mcp.json`.
- **Tooling**: `mise` tasks for `build`, `test`, `test-integration`, `lint`,
  `run`, and `install` (to `~/.local/bin`).
- **Docs & license**: `ARCHITECTURE.md` and the GNU GPL v3 license.

[v2.0.0]: https://github.com/asd-noor/claude-bridge/releases/tag/v2.0.0
[v1.2.0]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.2.0
[v1.1.3]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.1.3
[v1.1.2]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.1.2
[v1.1.1]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.1.1
[v1.1.0]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.1.0
[v1.0.1]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.0.1
[v1.0.0]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.0.0
