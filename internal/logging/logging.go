// Package logging is Rune's single logging surface. Everything logs through
// *Logger; no other package imports log or log/slog directly.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// Logger is a thin structured logger over slog. Constructed once at startup and
// injected explicitly into the components that need it.
type Logger struct {
	sl *slog.Logger
}

// New returns a JSON logger writing to stderr at the given level.
func New(level Level) *Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level.toSlog()})
	return &Logger{sl: slog.New(handler)}
}

// Discard returns a Logger that drops all output. Useful as a default and in tests.
func Discard() *Logger {
	return &Logger{sl: slog.New(slog.DiscardHandler)}
}

func (l *Logger) Debug(msg string, args ...any) { l.sl.Debug(msg, args...) }
func (l *Logger) Info(msg string, args ...any)  { l.sl.Info(msg, args...) }
func (l *Logger) Warn(msg string, args ...any)  { l.sl.Warn(msg, args...) }
func (l *Logger) Error(msg string, args ...any) { l.sl.Error(msg, args...) }

// DebugEnabled reports whether debug records are emitted, so callers can skip
// building log payloads that would be discarded.
func (l *Logger) DebugEnabled(ctx context.Context) bool {
	return l.sl.Enabled(ctx, slog.LevelDebug)
}

// Level is Rune's log level. It maps to slog internally so callers never need
// to import log/slog.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) toSlog() slog.Level {
	switch l {
	case LevelDebug:
		return slog.LevelDebug
	case LevelInfo:
		return slog.LevelInfo
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ParseLevel converts a level string (debug, info, warn/warning, error) to a
// Level, falling back to LevelInfo for anything unrecognized.
func ParseLevel(s string) Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}
