package main

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/asd-noor/claude-bridge/internal/config"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// Auto-spawn timing: how often to retry dialing a freshly spawned daemon and
// how long to keep trying before giving up.
const (
	spawnPollInterval = 50 * time.Millisecond
	spawnTimeout      = 2 * time.Second
)

// ensureDaemon returns a client connected to the daemon, spawning one if none
// is running. It guards the spawn with the same flock the daemon uses so two
// shims starting together do not both fork a daemon.
func ensureDaemon(cfg config.Config) (*daemonrpc.Client, error) {
	sock := config.SockPath(cfg)

	if client, err := daemonrpc.Dial(sock); err == nil {
		return client, nil
	}

	if err := os.MkdirAll(config.RuntimeDir(cfg), runtimeDirMode); err != nil {
		return nil, fmt.Errorf("create runtime dir: %w", err)
	}

	if err := spawnUnderLock(cfg, sock); err != nil {
		return nil, err
	}
	return dialUntilReady(sock)
}

// spawnUnderLock holds the spawn flock only long enough to re-check for a
// winner and fork the daemon, then releases it so the forked daemon can take
// the same lock for its own startup sequence. Polling for readiness happens
// after the lock is released.
func spawnUnderLock(cfg config.Config, sock string) error {
	lock, err := acquireLock(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	// Another shim may have won the race while we waited on the lock.
	if client, err := daemonrpc.Dial(sock); err == nil {
		_ = client.Close()
		return nil
	}
	return spawnDaemon()
}

// spawnDaemon forks `claude-bridge serve --detach` in a new session.
func spawnDaemon() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	cmd := detachedCommand(exe)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	_ = cmd.Process.Release()
	return nil
}

// dialUntilReady polls the socket until a daemon answers or the spawn timeout
// elapses.
func dialUntilReady(sock string) (*daemonrpc.Client, error) {
	deadline := time.Now().Add(spawnTimeout)
	for time.Now().Before(deadline) {
		if client, err := daemonrpc.Dial(sock); err == nil {
			return client, nil
		}
		time.Sleep(spawnPollInterval)
	}
	return nil, fmt.Errorf("daemon did not start within %s", spawnTimeout)
}

// detachedCommand builds the `serve --detach` command with the detached marker
// in its environment and a fresh session via Setsid. The grandchild re-exec is
// what fully detaches; this first hop just sets up the marker and session.
func detachedCommand(exe string) *exec.Cmd {
	cmd := exec.Command(exe, cmdServe, detachFlag)
	cmd.Env = append(os.Environ(), envDetached+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd
}
