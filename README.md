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

That is the whole integration: no port, no socket path, no hooks. Claude Code spawns
`claude-bridge mcp` per session; the shim registers the session with the daemon and
forwards bridge tool calls and push notifications.

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
