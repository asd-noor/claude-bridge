package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const sampleYAML = `
daemon:
  runtime_dir: "/var/run/cb"
  idle_timeout: "30m"
broker:
  message_ttl: "2m"
  session_ttl: "20m"
  max_inbox_size: 50
  cleanup_tick: "30s"
  broadcast_burst: 5
  broadcast_refill: "15s"
log:
  level: "debug"
  format: "json"
`

// writeConfig writes content to a config.yaml inside a fresh temp dir and
// returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadDefaultsWhenNoFile(t *testing.T) {
	clearEnv(t)
	missing := filepath.Join(t.TempDir(), "absent.yaml")

	got, err := Load(missing)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != Defaults() {
		t.Fatalf("Load(missing) = %+v, want defaults %+v", got, Defaults())
	}
}

func TestLoadYAMLOverlay(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, sampleYAML)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := Config{
		Daemon: Daemon{
			RuntimeDir:  "/var/run/cb",
			IdleTimeout: Duration(30 * time.Minute),
		},
		Broker: Broker{
			MessageTTL:      Duration(2 * time.Minute),
			SessionTTL:      Duration(20 * time.Minute),
			MaxInboxSize:    50,
			CleanupTick:     Duration(30 * time.Second),
			BroadcastBurst:  5,
			BroadcastRefill: Duration(15 * time.Second),
		},
		Log: Log{Level: "debug", Format: "json"},
	}
	if got != want {
		t.Fatalf("Load() = %+v, want %+v", got, want)
	}
}

func TestLoadPartialYAMLKeepsDefaults(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, "log:\n  level: \"warn\"\n")

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Log.Level != "warn" {
		t.Fatalf("Log.Level = %q, want warn", got.Log.Level)
	}
	if got.Log.Format != defaultLogFormat {
		t.Fatalf("Log.Format = %q, want default %q", got.Log.Format, defaultLogFormat)
	}
	if got.Broker.MaxInboxSize != defaultMaxInboxSize {
		t.Fatalf("MaxInboxSize = %d, want default %d", got.Broker.MaxInboxSize, defaultMaxInboxSize)
	}
}

func TestEnvOverridesWinOverFile(t *testing.T) {
	clearEnv(t)
	path := writeConfig(t, sampleYAML)

	t.Setenv(envRuntimeDir, "/tmp/override")
	t.Setenv(envIdleTimeout, "1h")
	t.Setenv(envMessageTTL, "45s")
	t.Setenv(envSessionTTL, "10m")
	t.Setenv(envLogLevel, "error")

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got.Daemon.RuntimeDir != "/tmp/override" {
		t.Errorf("RuntimeDir = %q, want /tmp/override", got.Daemon.RuntimeDir)
	}
	if got.Daemon.IdleTimeout.Std() != time.Hour {
		t.Errorf("IdleTimeout = %v, want 1h", got.Daemon.IdleTimeout.Std())
	}
	if got.Broker.MessageTTL.Std() != 45*time.Second {
		t.Errorf("MessageTTL = %v, want 45s", got.Broker.MessageTTL.Std())
	}
	if got.Broker.SessionTTL.Std() != 10*time.Minute {
		t.Errorf("SessionTTL = %v, want 10m", got.Broker.SessionTTL.Std())
	}
	if got.Log.Level != "error" {
		t.Errorf("Log.Level = %q, want error", got.Log.Level)
	}
	// Unaffected field retains the file value.
	if got.Log.Format != "json" {
		t.Errorf("Log.Format = %q, want json", got.Log.Format)
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		env     map[string]string
		wantErr bool
	}{
		{
			name:    "malformed yaml",
			yaml:    "daemon: [this is not: valid",
			wantErr: true,
		},
		{
			name:    "bad duration in yaml",
			yaml:    "daemon:\n  idle_timeout: \"not-a-duration\"\n",
			wantErr: true,
		},
		{
			name:    "invalid duration env",
			yaml:    "",
			env:     map[string]string{envIdleTimeout: "fortnight"},
			wantErr: true,
		},
		{
			name:    "valid",
			yaml:    sampleYAML,
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			path := writeConfig(t, tc.yaml)

			_, err := Load(path)
			if tc.wantErr && err == nil {
				t.Fatalf("Load() = nil error, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Load() = %v, want no error", err)
			}
		})
	}
}

func TestRuntimeDirDerivation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "explicit",
			cfg:  Config{Daemon: Daemon{RuntimeDir: "/custom/dir"}},
			want: "/custom/dir",
		},
		{
			name: "derived",
			cfg:  Config{},
			want: filepath.Join(os.TempDir(), fmt.Sprintf(runtimeDirPattern, os.Getuid())),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := RuntimeDir(tc.cfg); got != tc.want {
				t.Fatalf("RuntimeDir = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestArtifactPaths(t *testing.T) {
	cfg := Config{Daemon: Daemon{RuntimeDir: "/run/cb"}}

	cases := map[string]struct {
		got  string
		want string
	}{
		"sock": {SockPath(cfg), "/run/cb/sock"},
		"lock": {LockPath(cfg), "/run/cb/daemon.lock"},
		"pid":  {PidPath(cfg), "/run/cb/daemon.pid"},
		"log":  {LogPath(cfg), "/run/cb/daemon.log"},
	}
	for name, c := range cases {
		if c.got != c.want {
			t.Errorf("%s path = %q, want %q", name, c.got, c.want)
		}
	}
}

// clearEnv unsets all override env vars for the duration of the test.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		envRuntimeDir, envIdleTimeout, envMessageTTL, envSessionTTL, envLogLevel,
	} {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
}
