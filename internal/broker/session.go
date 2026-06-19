package broker

import "time"

// Session represents a connected Claude Code shim. ProjectPath and
// ProjectName are display labels only — they tell peers "this agent is
// running over here." Nothing in the broker keys off them; two sessions
// with identical labels are still distinct peers.
type Session struct {
	ID           string    `json:"id"` // UUIDv7
	ProjectPath  string    `json:"project_path"`
	ProjectName  string    `json:"project_name"` // basename of ProjectPath
	RegisteredAt time.Time `json:"registered_at"`
	LastSeen     time.Time `json:"last_seen"`
}

// IsStale reports whether the session has not been seen within d.
func (s *Session) IsStale(d time.Duration) bool {
	return time.Since(s.LastSeen) > d
}

// isStaleAt reports staleness relative to an injected clock, enabling
// deterministic tests of cleanup behaviour.
func (s *Session) isStaleAt(now time.Time, d time.Duration) bool {
	return now.Sub(s.LastSeen) > d
}
