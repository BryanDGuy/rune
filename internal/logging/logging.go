// Package logging builds the structured slog logger used across Rune.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON logger writing to stderr at the given level, tagged with
// the node's id. Recognized levels: debug, info, warn, error (anything else
// falls back to info).
func New(level, nodeID string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(handler).With("node_id", nodeID)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
