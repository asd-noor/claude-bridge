// Command claude-bridge is the CLI entrypoint for the claude-bridge daemon and
// its per-session stdio MCP shim. It dispatches to one of the subcommands:
//
//	claude-bridge mcp              Run as a stdio MCP shim (spawned by Claude Code)
//	claude-bridge serve            Run as the daemon (foreground)
//	claude-bridge serve --detach   Run as the daemon, detached (used by auto-spawn)
//	claude-bridge status           Show connected sessions
//	claude-bridge stop             Graceful shutdown via the PID file
//	claude-bridge install          Install an always-on launchd agent (macOS)
//	claude-bridge version          Print the build version
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"

	"github.com/asd-noor/claude-bridge/internal/config"
)

// version is the build version, injected at link time via
// -ldflags "-X main.version=...". It defaults to "dev" for `go run` builds.
var version = "dev"

// Subcommand names dispatched from the command line.
const (
	cmdMCP     = "mcp"
	cmdServe   = "serve"
	cmdStatus  = "status"
	cmdStop    = "stop"
	cmdInstall = "install"
	cmdVersion = "version"
	cmdHook    = "hook"
)

// Environment variable names shared across the cmd layer.
const (
	// envDetached marks the re-exec'd daemon child so it skips the detach
	// branch and runs in the foreground of its new session.
	envDetached = "CLAUDE_BRIDGE_DETACHED"
)

// defaultConfigRelPath is the config file location relative to the user's home.
const defaultConfigRelPath = ".claude-bridge/config.yaml"

// logLevels maps the --log flag values to slog levels.
var logLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func main() {
	os.Exit(run(os.Args[1:]))
}

// run parses global flags and the subcommand, loads config, builds the logger,
// and dispatches. It returns a process exit code.
func run(args []string) int {
	fs := flag.NewFlagSet("claude-bridge", flag.ContinueOnError)
	configPath := fs.String("config", defaultConfigPath(), "Path to config file")
	logLevel := fs.String("log", "", "Log level: debug|info|warn")
	fs.Usage = usage()

	if err := fs.Parse(args); err != nil {
		return 2
	}

	sub := fs.Arg(0)
	if sub == "" {
		fs.Usage()
		return 2
	}
	// version is a pure info command — answer it before loading config so it
	// works regardless of config state.
	if sub == cmdVersion {
		return runVersion()
	}
	rest := fs.Args()[1:]

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		return 1
	}
	if *logLevel != "" {
		cfg.Log.Level = *logLevel
	}
	logger := newLogger(cfg)

	return dispatch(sub, rest, cfg, logger)
}

// dispatch routes a subcommand to its handler.
func dispatch(sub string, args []string, cfg config.Config, logger *slog.Logger) int {
	switch sub {
	case cmdMCP:
		return runMCP(cfg, logger)
	case cmdServe:
		return runServe(args, cfg, logger)
	case cmdStatus:
		return runStatus(cfg, logger)
	case cmdStop:
		return runStop(cfg, logger)
	case cmdInstall:
		return runInstall(cfg, logger)
	case cmdHook:
		return runHook(cfg, logger)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", sub)
		usage()()
		return 2
	}
}

// runVersion prints the build version.
func runVersion() int {
	fmt.Printf("claude-bridge %s\n", buildVersion())
	return 0
}

// buildVersion resolves the version to report. A linker-injected value (from
// `mise run build`) wins; otherwise it falls back to the module version
// embedded by `go install module@vX.Y.Z`, and finally the "dev" default.
func buildVersion() string {
	if version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok &&
		info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}

// newLogger builds a slog.Logger writing to stderr, honoring config format and
// level. An unrecognized level falls back to info.
func newLogger(cfg config.Config) *slog.Logger {
	level, ok := logLevels[cfg.Log.Level]
	if !ok {
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.Log.Format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(handler)
}

// defaultConfigPath returns ~/.claude-bridge/config.yaml, falling back to a
// bare relative path when the home directory cannot be resolved.
func defaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return defaultConfigRelPath
	}
	return filepath.Join(home, defaultConfigRelPath)
}

// usage prints the CLI usage banner.
func usage() func() {
	return func() {
		fmt.Fprint(os.Stderr, `claude-bridge — local message broker for Claude Code sessions

Usage:
  claude-bridge mcp              Run as a stdio MCP shim (spawned by Claude Code)
  claude-bridge serve            Run as the daemon (foreground)
  claude-bridge serve --detach   Run as the daemon, detach from parent
  claude-bridge status           Show connected sessions
  claude-bridge stop             Graceful shutdown via the PID file
  claude-bridge install          Install an always-on launchd agent (macOS)
  claude-bridge version          Print the build version
  claude-bridge hook             UserPromptSubmit hook: inject pending peer messages

Flags:
  --config string  Path to config file (default ~/.claude-bridge/config.yaml)
  --log    string  Log level: debug|info|warn
`)
	}
}
