package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/asd-noor/claude-bridge/internal/config"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// launchd integration constants for the install subcommand on macOS.
const (
	launchdLabel       = "com.claude-bridge"
	launchAgentsRelDir = "Library/LaunchAgents"
	plistFileMode      = 0o644
	// envIdleTimeoutZero disables daemon auto-exit when run under launchd.
	envIdleTimeoutZero = "CLAUDE_BRIDGE_IDLE_TIMEOUT"
)

// runStatus dials the daemon and prints the connected sessions, or reports that
// the daemon is not running.
func runStatus(cfg config.Config, _ *slog.Logger) int {
	client, err := daemonrpc.Dial(config.SockPath(cfg))
	if err != nil {
		fmt.Println("claude-bridge: not running")
		return 0
	}
	defer func() { _ = client.Close() }()

	peers, err := listPeers(client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list peers: %v\n", err)
		return 1
	}
	printPeers(peers)
	return 0
}

// listPeers queries the daemon for the connected sessions as a neutral caller.
func listPeers(client *daemonrpc.Client) ([]peerRow, error) {
	raw, err := client.Call(daemonrpc.MethodListPeers, struct{}{})
	if err != nil {
		return nil, err
	}
	var res daemonrpc.ListPeersResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}

	rows := make([]peerRow, 0, len(res.Peers))
	for _, p := range res.Peers {
		rows = append(rows, peerRow{
			projectName: p.ProjectName,
			projectPath: p.ProjectPath,
			lastSeen:    p.LastSeen.Format(time.RFC3339),
		})
	}
	return rows, nil
}

// peerRow is a printable row of the status table.
type peerRow struct {
	projectName string
	projectPath string
	lastSeen    string
}

// printPeers renders the connected sessions as an aligned table.
func printPeers(rows []peerRow) {
	if len(rows) == 0 {
		fmt.Println("claude-bridge: running, no connected sessions")
		return
	}
	fmt.Printf("claude-bridge: running, %d session(s)\n\n", len(rows))

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PROJECT\tPATH\tLAST SEEN")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\n", r.projectName, r.projectPath, r.lastSeen)
	}
	_ = w.Flush()
}

// runStop reads the PID file and sends SIGTERM to the daemon.
func runStop(cfg config.Config, _ *slog.Logger) int {
	pid, err := readPID(cfg)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("claude-bridge: not running (no pid file)")
			return 0
		}
		fmt.Fprintf(os.Stderr, "read pid: %v\n", err)
		return 1
	}

	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		fmt.Fprintf(os.Stderr, "signal pid %d: %v\n", pid, err)
		return 1
	}
	fmt.Printf("claude-bridge: sent SIGTERM to pid %d\n", pid)
	return 0
}

// readPID parses the daemon PID file.
func readPID(cfg config.Config) (int, error) {
	data, err := os.ReadFile(config.PidPath(cfg))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

// runInstall writes a launchd agent on macOS so the daemon runs always-on with
// auto-exit disabled. On other platforms it prints guidance and does not fail.
func runInstall(_ config.Config, _ *slog.Logger) int {
	if runtime.GOOS != "darwin" {
		fmt.Println("claude-bridge: install is supported on macOS only")
		fmt.Println("On Linux, run `claude-bridge serve` under systemd with CLAUDE_BRIDGE_IDLE_TIMEOUT=0")
		return 0
	}

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve executable: %v\n", err)
		return 1
	}
	path, err := writePlist(exe)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write plist: %v\n", err)
		return 1
	}

	fmt.Printf("claude-bridge: wrote launchd agent to %s\n", path)
	fmt.Printf("Load it with: launchctl load %s\n", path)
	return 0
}

// writePlist renders and writes the launchd plist, returning its path.
func writePlist(exe string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, launchAgentsRelDir)
	if err := os.MkdirAll(dir, runtimeDirMode); err != nil {
		return "", err
	}
	path := filepath.Join(dir, launchdLabel+".plist")
	return path, os.WriteFile(path, []byte(plistContent(exe)), plistFileMode)
}

// plistContent builds a minimal launchd plist that runs the daemon with
// idle-exit disabled.
func plistContent(exe string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>%s</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>%s</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>%s</key>
        <string>0</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
`, launchdLabel, exe, cmdServe, envIdleTimeoutZero)
}
