package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	cfg, err := LoadConfig("")
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

func TestEnvOverride(t *testing.T) {
	t.Setenv("RUNE_PORT", "9000")
	cfg, err := LoadConfig("")
	require.NoError(t, err)
	assert.Equal(t, 9000, cfg.Port)
}

func TestYAMLFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "rune-config-*.yaml")
	require.NoError(t, err)

	_, err = f.WriteString("port: 8888\nlog-level: debug\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := LoadConfig(f.Name())
	require.NoError(t, err)
	assert.Equal(t, 8888, cfg.Port)
	assert.Equal(t, "debug", cfg.LogLevel)
}
