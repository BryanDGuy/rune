// Package logging is Rune's single logging surface. Everything logs through
// *Logger; no other package imports log or log/slog directly.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Logger is a thin structured logger over slog. Constructed once at startup and
// injected explicitly into the components that need it.
type Logger struct {
	sl *slog.Logger
}

// New returns a JSON logger writing to stderr at the given level. Recognized
// levels: debug, info, warn, error (anything else falls back to info).
func New(level string) *Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: parseLevel(level)})
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

// With returns a child logger that adds the given key/value attributes to every
// record. The returned logger stays within Rune's logging surface.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{sl: l.sl.With(args...)}
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
