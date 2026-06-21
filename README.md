# claude-bridge

`claude-bridge` is a Go daemon that acts as a local message broker for Claude Code
sessions, letting agents running in different projects on the same machine discover
each other and exchange messages. Each session is fronted by a per-session stdio MCP
shim that Claude Code spawns; the shim connects to a single shared daemon over a Unix
domain socket. The daemon auto-spawns on first connect and auto-exits after an idle
period, keeping all shared state (sessions, inboxes, presence) in memory.

```
┌──────────────┐  stdio (MCP)  ┌────────────────────┐
│ Claude Code  │ ◄───────────► │ claude-bridge mcp  │
│ (Project A)  │               │ (stdio shim)       │
└──────────────┘               └─────────┬──────────┘
                                         │ UDS
                                         ▼
                              ┌─────────────────────┐
                              │ claude-bridge serve │
                              │ (daemon)            │
                              └─────────────────────┘
                                         ▲
┌──────────────┐  stdio (MCP)  ┌─────────┴──────────┐
│ Claude Code  │ ◄───────────► │ claude-bridge mcp  │
│ (Project B)  │               │ (stdio shim)       │
└──────────────┘               └────────────────────┘
```

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the design.

## Build

The project uses [`mise`](https://mise.jdx.dev) for tooling (Go 1.26.3) and mise tasks
as a Makefile alternative.

```sh
mise run build
```

This compiles the binary to `./bin/claude-bridge`.

Install it to `~/.local/bin` (ensure that is on your `PATH`):

```sh
mise run install
```

Or install straight from the module with the Go toolchain (drops the binary in
`$(go env GOBIN)`, usually `~/go/bin`):

```sh
go install github.com/asd-noor/claude-bridge/cmd/claude-bridge@latest
```

Verify with `claude-bridge version`.

## Run

Start the daemon (it also auto-spawns on first shim connect, so this is optional):

```sh
./bin/claude-bridge serve
```

Register the MCP shim with Claude Code by adding this stanza to a project's `.mcp.json`
or to `~/.claude/.mcp.json`:

```json
{
  "mcpServers": {
    "claude-bridge": {
      "command": "claude-bridge",
      "args": ["mcp"]
    }
  }
}
```

Or register it from the CLI (`--scope user` makes it available in every project,
which is the point of a cross-session bridge):

```sh
claude mcp add --scope user claude-bridge -- claude-bridge mcp
```

That is the whole integration: no port, no socket path, no hooks, no plugin. Claude
Code spawns `claude-bridge mcp` per session; an opted-in shim registers the session
with the daemon and forwards bridge tool calls and channel notifications.

### Opt-in: bridge sessions only

Registering the server (above, especially `--scope user`) means Claude Code spawns
the shim for **every** session. A session only actually joins the bridge — registers,
connects to the daemon, exposes the tools, becomes a channel — when it opts in with
`CLAUDE_BRIDGE_ENABLE=1`. Without it the shim is **inert**: no daemon, no peer entry,
no tools. So ordinary work never pollutes the peer list, and coordination is a
deliberate choice.

Channels also require Claude's development flag (custom channels are a research
preview). Wrap both in a shell function so you opt in with one word:

```sh
cb() { CLAUDE_BRIDGE_ENABLE=1 claude --dangerously-load-development-channels server:claude-bridge "$@"; }
```

Launch a bridge session with `cb`; launch ordinary sessions with plain `claude`. The
dev-channels warning `cb` prints is unavoidable for a self-built channel and harmless.

### Automatic delivery (channels)

In a `cb` session, incoming messages **show up on their own** — including in a fully
idle session — via Claude Code [channels](https://code.claude.com/docs/en/channels).
The shim pushes each peer message as a `notifications/claude/channel` event, which
starts a turn even when you're not typing. If a session wasn't launched as a channel,
messages still queue in the inbox — drain them with the `poll_messages` tool.

The bridge ships with six tools: `list_peers`, `send_message`, `broadcast`,
`poll_messages`, `get_peer_info`, and `respond_permission`.

#### Permission relay

When Claude Code opens a tool-approval dialog, the shim relays the prompt to your
connected peers. A peer's Claude can answer it by calling the `respond_permission`
tool (`to`, `request_id`, `behavior: "allow" | "deny"`); the verdict flows back to
the requesting session. The local terminal dialog also stays open — whichever
answer arrives first wins.

#### Avoiding per-call permission prompts

To skip the per-call approval prompt for the bridge's own tools, add an allow rule
to `~/.claude/settings.json`:

```json
{ "permissions": { "allow": ["mcp__claude-bridge"] } }
```

The rule covers all bridge tools and only takes effect while the server is
connected.

## CLI subcommands

| Command                       | Description                                                        |
| ----------------------------- | ------------------------------------------------------------------ |
| `claude-bridge mcp`           | Run as a stdio MCP shim (spawned by Claude Code).                  |
| `claude-bridge serve`         | Run as the daemon (foreground).                                   |
| `claude-bridge serve --detach`| Run as the daemon, detached from the parent (used by auto-spawn). |
| `claude-bridge status`        | Show connected sessions.                                          |
| `claude-bridge stop`          | Graceful shutdown (SIGTERM via PID file).                         |
| `claude-bridge install`       | Install a launchd plist for always-on operation (optional).      |

Global flags:

| Flag       | Description                                                  |
| ---------- | ------------------------------------------------------------ |
| `--config` | Path to config file (default `~/.claude-bridge/config.yaml`). |
| `--log`    | Log level: `debug` \| `info` \| `warn` (default `info`).      |

## Development

mise tasks live as executable scripts under `scripts/`:

| Task                          | What it does                                          |
| ----------------------------- | ----------------------------------------------------- |
| `mise run build`              | `go build -o ./bin/claude-bridge ./cmd/claude-bridge` |
| `mise run test`               | `go test ./...`                                       |
| `mise run test-integration`   | `go test -tags integration ./...`                     |
| `mise run lint`               | `go vet ./...` plus a `gofmt -l .` formatting check   |
| `mise run run -- <args>`      | `go run ./cmd/claude-bridge` with args forwarded      |

List all tasks with `mise tasks`.

Run the unit tests:

```sh
mise run test
```

Run the integration tests (build tag `integration`):

```sh
mise run test-integration
```

Example of running the binary through mise (args after `--` are forwarded):

```sh
mise run run -- status
```

## License

Copyright (C) 2026 Asaduzzaman Noor

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version. See [LICENSE](./LICENSE) for the full text.
