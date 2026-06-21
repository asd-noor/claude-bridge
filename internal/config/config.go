// Package config loads claude-bridge configuration from a YAML file and
// environment variable overrides, and derives runtime artifact paths.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Default configuration values mirroring the blueprint config.yaml.
const (
	defaultIdleTimeout     = 15 * time.Minute
	defaultMessageTTL      = 5 * time.Minute
	defaultSessionTTL      = 30 * time.Minute
	defaultMaxInboxSize    = 100
	defaultCleanupTick     = time.Minute
	defaultBroadcastBurst  = 3
	defaultBroadcastRefill = 10 * time.Second
	defaultLogLevel        = "info"
	defaultLogFormat       = "text"
	defaultRuntimeDir      = ""

	defaultLivelockEnabled   = true
	defaultLivelockMaxChain  = 20
	defaultLivelockResetIdle = 60 * time.Second
)

// Environment variable names for overrides (highest precedence).
const (
	envRuntimeDir  = "CLAUDE_BRIDGE_RUNTIME_DIR"
	envIdleTimeout = "CLAUDE_BRIDGE_IDLE_TIMEOUT"
	envMessageTTL  = "CLAUDE_BRIDGE_MESSAGE_TTL"
	envSessionTTL  = "CLAUDE_BRIDGE_SESSION_TTL"
	envLogLevel    = "CLAUDE_BRIDGE_LOG_LEVEL"
)

// Runtime artifact filenames living inside the runtime directory.
const (
	sockFilename = "sock"
	lockFilename = "daemon.lock"
	pidFilename  = "daemon.pid"
	logFilename  = "daemon.log"
)

// runtimeDirPattern formats the derived per-user runtime directory name.
const runtimeDirPattern = "claude-bridge-%d"

// Duration wraps time.Duration so it can be unmarshalled from a YAML string
// such as "15m" via time.ParseDuration.
type Duration time.Duration

// UnmarshalYAML parses a YAML scalar string into a Duration.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Std returns the underlying time.Duration.
func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

// Daemon holds daemon lifecycle configuration.
type Daemon struct {
	RuntimeDir  string   `yaml:"runtime_dir"`
	IdleTimeout Duration `yaml:"idle_timeout"`
}

// Broker holds message broker configuration.
type Broker struct {
	MessageTTL      Duration `yaml:"message_ttl"`
	SessionTTL      Duration `yaml:"session_ttl"`
	MaxInboxSize    int      `yaml:"max_inbox_size"`
	CleanupTick     Duration `yaml:"cleanup_tick"`
	BroadcastBurst  int      `yaml:"broadcast_burst"`
	BroadcastRefill Duration `yaml:"broadcast_refill"`
	Livelock        Livelock `yaml:"livelock"`
}

// Livelock configures the no-progress reply-chain breaker: once two sessions
// exchange more than MaxChain consecutive content-free messages, the broker stops
// waking the recipient for that pair (inboxes stay intact). A ResetIdle gap with
// no traffic clears the chain. Enabled=false (or MaxChain=0) disables it entirely.
type Livelock struct {
	Enabled   bool     `yaml:"enabled"`
	MaxChain  int      `yaml:"max_chain"`
	ResetIdle Duration `yaml:"reset_idle"`
}

// Log holds logging configuration.
type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Config is the fully resolved claude-bridge configuration.
type Config struct {
	Daemon Daemon `yaml:"daemon"`
	Broker Broker `yaml:"broker"`
	Log    Log    `yaml:"log"`
}

// Defaults returns a Config populated with the blueprint default values.
func Defaults() Config {
	return Config{
		Daemon: Daemon{
			RuntimeDir:  defaultRuntimeDir,
			IdleTimeout: Duration(defaultIdleTimeout),
		},
		Broker: Broker{
			MessageTTL:      Duration(defaultMessageTTL),
			SessionTTL:      Duration(defaultSessionTTL),
			MaxInboxSize:    defaultMaxInboxSize,
			CleanupTick:     Duration(defaultCleanupTick),
			BroadcastBurst:  defaultBroadcastBurst,
			BroadcastRefill: Duration(defaultBroadcastRefill),
			Livelock: Livelock{
				Enabled:   defaultLivelockEnabled,
				MaxChain:  defaultLivelockMaxChain,
				ResetIdle: Duration(defaultLivelockResetIdle),
			},
		},
		Log: Log{
			Level:  defaultLogLevel,
			Format: defaultLogFormat,
		},
	}
}

// Load builds a Config starting from Defaults(), overlaying the YAML file at
// path when it exists, then applying environment overrides. A missing file is
// not an error; malformed YAML or invalid override values are.
func Load(path string) (Config, error) {
	cfg := Defaults()

	if err := overlayFile(&cfg, path); err != nil {
		return Config{}, err
	}
	if err := applyEnv(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// overlayFile decodes the YAML file at path into cfg, leaving cfg untouched
// when the file is absent.
func overlayFile(cfg *Config, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse config %q: %w", path, err)
	}
	return nil
}

// applyEnv overlays environment variable overrides onto cfg.
func applyEnv(cfg *Config) error {
	if v, ok := os.LookupEnv(envRuntimeDir); ok {
		cfg.Daemon.RuntimeDir = v
	}
	if err := envDuration(envIdleTimeout, &cfg.Daemon.IdleTimeout); err != nil {
		return err
	}
	if err := envDuration(envMessageTTL, &cfg.Broker.MessageTTL); err != nil {
		return err
	}
	if err := envDuration(envSessionTTL, &cfg.Broker.SessionTTL); err != nil {
		return err
	}
	if v, ok := os.LookupEnv(envLogLevel); ok {
		cfg.Log.Level = v
	}
	return nil
}

// envDuration parses the named env var as a duration into dst when set.
func envDuration(name string, dst *Duration) error {
	v, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("invalid duration for %s=%q: %w", name, v, err)
	}
	*dst = Duration(parsed)
	return nil
}

// RuntimeDir returns the configured runtime directory, or a derived per-user
// path under os.TempDir() when unset.
func RuntimeDir(cfg Config) string {
	if cfg.Daemon.RuntimeDir != "" {
		return cfg.Daemon.RuntimeDir
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf(runtimeDirPattern, os.Getuid()))
}

// SockPath returns the UDS socket path within the runtime directory.
func SockPath(cfg Config) string {
	return filepath.Join(RuntimeDir(cfg), sockFilename)
}

// LockPath returns the spawn flock path within the runtime directory.
func LockPath(cfg Config) string {
	return filepath.Join(RuntimeDir(cfg), lockFilename)
}

// PidPath returns the daemon PID file path within the runtime directory.
func PidPath(cfg Config) string {
	return filepath.Join(RuntimeDir(cfg), pidFilename)
}

// LogPath returns the daemon log file path within the runtime directory.
func LogPath(cfg Config) string {
	return filepath.Join(RuntimeDir(cfg), logFilename)
}
