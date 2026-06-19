# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.2.0] - 2026-06-19

### Added

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

## [v1.1.0] - 2026-06-19

### Added

- **Automatic message delivery via a `UserPromptSubmit` hook.** The plugin now
  ships a hook running `claude-bridge hook`, which injects pending peer messages
  into the receiving session's context on each turn — no manual `poll_messages`.
  This works around Claude Code not surfacing MCP `notifications/message` to the
  model.
- `claude-bridge hook` subcommand: resolves the session for the current
  directory and emits queued messages as hook `additionalContext`.
- The shim now records a `runtimeDir/sessions/<hash(cwd)>` → `session_id` map so
  the hook can find the right inbox; removed on shim exit.

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

[v1.2.0]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.2.0
[v1.1.0]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.1.0
[v1.0.1]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.0.1
[v1.0.0]: https://github.com/asd-noor/claude-bridge/releases/tag/v1.0.0
