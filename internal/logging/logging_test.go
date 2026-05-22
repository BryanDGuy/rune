package logging

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want Level
	}{
		{"debug", LevelDebug},
		{"info", LevelInfo},
		{"WARN", LevelWarn},
		{"warning", LevelWarn},
		{"error", LevelError},
		{"", LevelInfo},
		{"nonsense", LevelInfo},
		{" Debug   ", LevelDebug},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, ParseLevel(c.in), "ParseLevel(%q)", c.in)
	}
}

func TestLevelToSlog(t *testing.T) {
	assert.Equal(t, slog.LevelDebug, LevelDebug.toSlog())
	assert.Equal(t, slog.LevelInfo, LevelInfo.toSlog())
	assert.Equal(t, slog.LevelWarn, LevelWarn.toSlog())
	assert.Equal(t, slog.LevelError, LevelError.toSlog())
}
