package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/asd-noor/claude-bridge/internal/broker"
	"github.com/asd-noor/claude-bridge/internal/config"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
)

// Filesystem permission modes for runtime artifacts.
const (
	runtimeDirMode = 0o700
	sockMode       = 0o600
	pidFileMode    = 0o600
)

// detachFlag is the serve flag that triggers the detach-and-re-exec dance.
const detachFlag = "--detach"

// daemon bundles the live state of a running serve process so its lifecycle
// helpers can share the listener, server, broker, and config without globals.
type daemon struct {
	cfg    config.Config
	logger *slog.Logger
	ln     net.Listener
	server *daemonrpc.Server
	broker *broker.Broker

	mu        sync.Mutex
	idleTimer *time.Timer
	shutdown  sync.Once
}

// runServe runs the daemon. With --detach it re-execs itself into a new session
// (unless already the detached child) and the parent exits; otherwise it runs
// the daemon in the foreground until a signal or the idle timer fires.
func runServe(args []string, cfg config.Config, logger *slog.Logger) int {
	detach := slices.Contains(args, detachFlag)

	if detach && !isDetachedChild() {
		return reExecDetached(logger)
	}

	if isDetachedChild() {
		// The log file lives inside the runtime dir, so it must exist before
		// stdout/stderr are redirected into it. startDaemon re-runs this
		// idempotently.
		if err := os.MkdirAll(config.RuntimeDir(cfg), runtimeDirMode); err != nil {
			logger.Error("create runtime dir", "err", err)
			return 1
		}
		if err := redirectToLog(cfg); err != nil {
			logger.Error("redirect to log", "err", err)
			return 1
		}
	}

	d, code, done := startDaemon(cfg, logger)
	if !done {
		return code
	}
	defer d.broker.Close()

	d.installSignalHandlers()
	d.wireIdleShutdown()

	if err := d.serve(); err != nil {
		d.logger.Debug("listener stopped", "err", err)
	}
	return 0
}

// startDaemon performs the lock/recovery/listen/pid sequence. The boolean is
// false when the daemon should not proceed (another instance is alive, or
// setup failed); code is the exit code to use in that case.
func startDaemon(cfg config.Config, logger *slog.Logger) (*daemon, int, bool) {
	if err := os.MkdirAll(config.RuntimeDir(cfg), runtimeDirMode); err != nil {
		logger.Error("create runtime dir", "err", err)
		return nil, 1, false
	}

	// The flock guards only the check-and-bind startup critical section: it
	// serializes concurrent daemons racing to recover a stale socket and bind.
	// Once the socket is bound it becomes the liveness token (stale-socket
	// recovery dials it), so the lock is released here. Holding it for the
	// process lifetime would instead make every redundant `serve` block on
	// LOCK_EX forever instead of dialing the live socket and exiting.
	lock, err := acquireLock(cfg)
	if err != nil {
		logger.Error("acquire lock", "err", err)
		return nil, 1, false
	}
	defer func() { _ = lock.Close() }()

	if alive := recoverStaleSocket(cfg, logger); alive {
		logger.Info("daemon already running, exiting")
		return nil, 0, false
	}

	ln, err := listen(cfg)
	if err != nil {
		logger.Error("listen", "err", err)
		return nil, 1, false
	}

	if err := writePID(cfg); err != nil {
		logger.Error("write pid", "err", err)
		_ = ln.Close()
		return nil, 1, false
	}

	b := broker.New(brokerConfig(cfg))
	d := &daemon{
		cfg:    cfg,
		logger: logger,
		ln:     ln,
		server: daemonrpc.NewServer(b),
		broker: b,
	}
	logger.Info("daemon listening", "sock", config.SockPath(cfg))
	return d, 0, true
}

// serve runs the accept loop until the listener is closed by shutdown.
func (d *daemon) serve() error {
	return d.server.Serve(d.ln)
}

// brokerConfig maps the config broker section onto a broker.Config.
func brokerConfig(cfg config.Config) broker.Config {
	return broker.Config{
		MessageTTL:      cfg.Broker.MessageTTL.Std(),
		SessionTTL:      cfg.Broker.SessionTTL.Std(),
		MaxInboxSize:    cfg.Broker.MaxInboxSize,
		CleanupTick:     cfg.Broker.CleanupTick.Std(),
		BroadcastBurst:  cfg.Broker.BroadcastBurst,
		BroadcastRefill: cfg.Broker.BroadcastRefill.Std(),
	}
}

// acquireLock opens the lock file and takes an exclusive advisory flock. The
// caller holds it across the startup check-and-bind critical section and closes
// it once the socket is bound; the bound socket is the runtime liveness token.
func acquireLock(cfg config.Config) (*os.File, error) {
	f, err := os.OpenFile(config.LockPath(cfg), os.O_CREATE|os.O_RDWR, sockMode)
	if err != nil {
		return nil, fmt.Errorf("open lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("flock: %w", err)
	}
	return f, nil
}

// recoverStaleSocket checks for an existing socket file: if a daemon answers a
// dial it reports alive=true; otherwise the stale file is removed.
func recoverStaleSocket(cfg config.Config, logger *slog.Logger) (alive bool) {
	sock := config.SockPath(cfg)
	if _, err := os.Stat(sock); err != nil {
		return false
	}
	if client, err := daemonrpc.Dial(sock); err == nil {
		_ = client.Close()
		return true
	}
	logger.Info("removing stale socket", "sock", sock)
	_ = os.Remove(sock)
	return false
}

// listen binds the UDS listener and tightens the socket permissions.
func listen(cfg config.Config) (net.Listener, error) {
	sock := config.SockPath(cfg)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sock, sockMode); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("chmod sock: %w", err)
	}
	return ln, nil
}

// writePID records the current process ID in the daemon PID file.
func writePID(cfg config.Config) error {
	pid := strconv.Itoa(os.Getpid())
	return os.WriteFile(config.PidPath(cfg), []byte(pid+"\n"), pidFileMode)
}

// wireIdleShutdown arms an idle-shutdown timer whenever the active connection
// count returns to zero, cancelling any prior timer. A zero IdleTimeout
// disables auto-exit (e.g. under launchd).
func (d *daemon) wireIdleShutdown() {
	timeout := d.cfg.Daemon.IdleTimeout.Std()
	if timeout <= 0 {
		return
	}
	d.server.OnIdle(func() {
		d.mu.Lock()
		if d.idleTimer != nil {
			d.idleTimer.Stop()
		}
		d.idleTimer = time.AfterFunc(timeout, d.idleShutdown)
		d.mu.Unlock()
	})
}

// installSignalHandlers forces shutdown on SIGTERM or SIGINT. `claude-bridge
// stop` relies on this, so it must tear down regardless of active connections.
func (d *daemon) installSignalHandlers() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigs
		d.logger.Info("signal received", "signal", sig.String())
		d.shutdownNow()
	}()
}

// idleShutdown fires from the idle timer; it aborts if a connection arrived
// while the timer was pending. Explicit stops do not go through this guard.
func (d *daemon) idleShutdown() {
	if d.server.ActiveConns() > 0 {
		d.logger.Debug("idle shutdown aborted, connections active")
		return
	}
	d.shutdownNow()
}

// shutdownNow stops accepting connections, removes runtime files, and ends the
// accept loop. It is idempotent.
func (d *daemon) shutdownNow() {
	d.shutdown.Do(func() {
		d.logger.Info("shutting down")
		// Remove the runtime files before closing the listener: closing it
		// unblocks the accept loop and lets the process exit, which would
		// otherwise race this cleanup goroutine.
		_ = os.Remove(config.SockPath(d.cfg))
		_ = os.Remove(config.PidPath(d.cfg))
		_ = d.ln.Close()
	})
}

// redirectToLog points stdout and stderr at the daemon log file. Used by the
// detached child after re-exec.
func redirectToLog(cfg config.Config) error {
	f, err := os.OpenFile(config.LogPath(cfg), os.O_CREATE|os.O_WRONLY|os.O_APPEND, pidFileMode)
	if err != nil {
		return err
	}
	// dup2 duplicates the descriptor onto the std streams, so the original
	// can be closed once both redirects are wired.
	defer f.Close()
	if err := dup2(f, os.Stdout); err != nil {
		return err
	}
	return dup2(f, os.Stderr)
}

// dup2 redirects the file descriptor of target to that of src.
func dup2(src, target *os.File) error {
	return syscall.Dup2(int(src.Fd()), int(target.Fd()))
}

// reExecDetached starts a copy of this process in a new session (setsid) with
// the detached marker set, then returns so the parent exits. The child binds
// the socket and runs the daemon.
func reExecDetached(logger *slog.Logger) int {
	exe, err := os.Executable()
	if err != nil {
		logger.Error("resolve executable", "err", err)
		return 1
	}

	cmd := detachedCommand(exe)
	if err := cmd.Start(); err != nil {
		logger.Error("start detached daemon", "err", err)
		return 1
	}
	logger.Info("daemon detached", "pid", cmd.Process.Pid)
	_ = cmd.Process.Release()
	return 0
}

// isDetachedChild reports whether this process is the re-exec'd daemon child.
func isDetachedChild() bool {
	return os.Getenv(envDetached) == "1"
}
