package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 7946, cfg.Port)
	assert.Equal(t, "/var/rune/data", cfg.DataDir)
	assert.Equal(t, int64(107374182400), cfg.MaxStorageBytes) // 100GB
	assert.InDelta(t, 0.8, cfg.EvictionThreshold, 1e-9)
	assert.InDelta(t, 1.0, cfg.EvictionSizeWeight, 1e-9)
	assert.InDelta(t, 1.0, cfg.EvictionAgeWeight, 1e-9)
	assert.Equal(t, 1024*1024, cfg.StreamChunkSize) // 1MB
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 9090, cfg.MetricsPort)
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("RUNE_PORT", "9000")
	t.Setenv("RUNE_DATA_DIR", "/tmp/rune")
	t.Setenv("RUNE_LOG_LEVEL", "debug")
	t.Setenv("RUNE_MAX_STORAGE", "500MB")
	t.Setenv("RUNE_EVICTION_THRESHOLD", "0.9")
	t.Setenv("RUNE_METRICS_PORT", "9100")
	t.Setenv("RUNE_GC_INTERVAL", "5m")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 9000, cfg.Port)
	assert.Equal(t, "/tmp/rune", cfg.DataDir)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, int64(524288000), cfg.MaxStorageBytes) // 500MB
	assert.InDelta(t, 0.9, cfg.EvictionThreshold, 1e-9)
	assert.Equal(t, 9100, cfg.MetricsPort)
	assert.Equal(t, 5*60*1e9, float64(cfg.GCInterval))
}

func TestParseBytes(t *testing.T) {
	cases := []struct {
		input string
		want  int64
	}{
		{"100GB", 107374182400},
		{"500MB", 524288000},
		{"1KB", 1024},
		{"1024", 1024},
	}
	for _, c := range cases {
		got, err := parseBytes(c.input)
		require.NoError(t, err, "input: %s", c.input)
		assert.Equal(t, c.want, got, "input: %s", c.input)
	}
}
