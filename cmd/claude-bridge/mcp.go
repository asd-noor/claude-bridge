package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/asd-noor/claude-bridge/internal/config"
	"github.com/asd-noor/claude-bridge/internal/daemonrpc"
	"github.com/asd-noor/claude-bridge/internal/mcp"
)

// unregisterTimeout bounds the best-effort unregister on shim exit.
const unregisterTimeout = 200 * time.Millisecond

// shim holds the two daemon connections and the session identity for one MCP
// shim process: a control client for RPC and a second client dedicated to the
// event subscription.
type shim struct {
	control   *daemonrpc.Client
	subscribe *daemonrpc.Client
	sessionID string
	logger    *slog.Logger
}

// runMCP runs the stdio MCP shim: it ensures a daemon, registers a session,
// subscribes for push events, and serves the MCP loop on stdin/stdout until
// EOF or a termination signal, then unregisters and exits.
func runMCP(cfg config.Config, logger *slog.Logger) int {
	sh, err := startShim(cfg, logger)
	if err != nil {
		logger.Error("shim startup", "err", err)
		return 1
	}
	defer sh.close()

	server := mcp.NewServer(sh.control, sh.sessionID, cfg.Broker.ChannelMode)

	ctx, cancel := signalContext()
	defer cancel()

	events, err := sh.subscribe.Subscribe(sh.sessionID)
	if err != nil {
		logger.Error("subscribe", "err", err)
		return 1
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		server.ForwardEvents(ctx, events, os.Stdout)
	}()

	if err := server.Serve(ctx, os.Stdin, os.Stdout); err != nil && ctx.Err() == nil {
		logger.Error("mcp serve", "err", err)
	}

	cancel()
	wg.Wait()
	sh.unregister()
	return 0
}

// startShim wires the daemon connections and registers a session.
func startShim(cfg config.Config, logger *slog.Logger) (*shim, error) {
	projectPath, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	control, err := ensureDaemon(cfg)
	if err != nil {
		return nil, err
	}

	sessionID, err := registerSession(control, projectPath)
	if err != nil {
		_ = control.Close()
		return nil, err
	}

	subscribe, err := daemonrpc.Dial(config.SockPath(cfg))
	if err != nil {
		_ = control.Close()
		return nil, err
	}

	return &shim{
		control:   control,
		subscribe: subscribe,
		sessionID: sessionID,
		logger:    logger,
	}, nil
}

// registerSession registers projectPath with the daemon and returns the
// assigned session ID.
func registerSession(client *daemonrpc.Client, projectPath string) (string, error) {
	raw, err := client.Call(daemonrpc.MethodRegister, daemonrpc.RegisterParams{ProjectPath: projectPath})
	if err != nil {
		return "", err
	}
	var res daemonrpc.RegisterResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return "", err
	}
	return res.SessionID, nil
}

// unregister tells the daemon to drop the session, best-effort within a short
// deadline. Network failures are ignored — a dirty exit is reaped by SessionTTL.
func (s *shim) unregister() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.control.CallAs(s.sessionID, daemonrpc.MethodUnregister, struct{}{})
	}()
	select {
	case <-done:
	case <-time.After(unregisterTimeout):
		s.logger.Debug("unregister timed out")
	}
}

// close tears down both daemon connections.
func (s *shim) close() {
	_ = s.control.Close()
	_ = s.subscribe.Close()
}

// signalContext returns a context cancelled on SIGTERM or SIGINT.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
