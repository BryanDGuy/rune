package config

import (
	"os"
	"testing"
	"time"

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
}

func TestEnvOverrides(t *testing.T) {
	t.Setenv("RUNE_PORT", "9000")
	t.Setenv("RUNE_DATA_DIR", "/tmp/rune")
	t.Setenv("RUNE_LOG_LEVEL", "debug")
	t.Setenv("RUNE_MAX_STORAGE", "500MB")
	t.Setenv("RUNE_EVICTION_THRESHOLD", "0.9")
	t.Setenv("RUNE_GC_INTERVAL", "5m")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 9000, cfg.Port)
	assert.Equal(t, "/tmp/rune", cfg.DataDir)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, int64(524288000), cfg.MaxStorageBytes) // 500MB
	assert.InDelta(t, 0.9, cfg.EvictionThreshold, 1e-9)
	assert.InDelta(t, float64(5*time.Minute), float64(cfg.GCInterval), 0)
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

func TestConfigEtcdEndpoints(t *testing.T) {
	t.Setenv("RUNE_ETCD_ENDPOINTS", "etcd1:2379,etcd2:2379")
	t.Setenv("RUNE_NODE_ADDR", "rune-0:7946")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, []string{"etcd1:2379", "etcd2:2379"}, cfg.EtcdEndpoints)
}

func TestConfigEtcdEndpointsEmpty(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Empty(t, cfg.EtcdEndpoints, "no env var = single-node mode")
}

func TestConfigNodeID(t *testing.T) {
	t.Setenv("RUNE_NODE_ID", "node-abc")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "node-abc", cfg.NodeID)
}

func TestConfigNodeIDDefaultsToHostname(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	hostname, _ := os.Hostname()
	assert.Equal(t, hostname, cfg.NodeID)
}

func TestConfigNodeAddr(t *testing.T) {
	t.Setenv("RUNE_NODE_ADDR", "rune-0.rune.svc.cluster.local:7946")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "rune-0.rune.svc.cluster.local:7946", cfg.NodeAddr)
}

func TestConfigNodeAddrDefaultsToLocalhostPort(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "localhost:7946", cfg.NodeAddr)
}

func TestBlockCacheSizeDefaultsToZero(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, int64(0), cfg.BlockCacheSize)
}

func TestBlockCacheSizeEnvOverride(t *testing.T) {
	t.Setenv("RUNE_BLOCK_CACHE_SIZE", "256MB")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, int64(256*1024*1024), cfg.BlockCacheSize)
}

func TestMetricsPortDefault(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 9090, cfg.MetricsPort)
}

func TestMetricsPortEnvOverride(t *testing.T) {
	t.Setenv("RUNE_METRICS_PORT", "8080")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 8080, cfg.MetricsPort)
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	cases := map[string]map[string]string{
		"cluster without node addr":   {"RUNE_ETCD_ENDPOINTS": "etcd:2379"},
		"cluster with localhost addr": {"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "localhost:7946"},
		"cluster with 127.0.0.1 addr": {"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "127.0.0.1:7946"},
		"cluster with ::1 addr":       {"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "[::1]:7946"},
		"cluster with empty host":     {"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": ":7946"},
		"cluster with no port":        {"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "rune-0"},
		"empty etcd endpoint":         {"RUNE_ETCD_ENDPOINTS": "etcd:2379,", "RUNE_NODE_ADDR": "rune-0:7946"},
		"chunk size zero":             {"RUNE_STREAM_CHUNK_SIZE": "0"},
		"chunk size negative":         {"RUNE_STREAM_CHUNK_SIZE": "-1"},
		"chunk size 4MiB":             {"RUNE_STREAM_CHUNK_SIZE": "4194304"},
		"gc interval zero":            {"RUNE_GC_INTERVAL": "0s"},
		"gc interval sub-second":      {"RUNE_GC_INTERVAL": "500ms"},
		"discard ratio zero":          {"RUNE_GC_DISCARD_RATIO": "0"},
		"discard ratio one":           {"RUNE_GC_DISCARD_RATIO": "1"},
		"threshold zero":              {"RUNE_EVICTION_THRESHOLD": "0"},
		"threshold above one":         {"RUNE_EVICTION_THRESHOLD": "7"},
		"max storage zero":            {"RUNE_MAX_STORAGE": "0"},
		"max storage negative":        {"RUNE_MAX_STORAGE": "-5GB"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			for k, v := range env {
				t.Setenv(k, v)
			}
			_, err := LoadConfig()
			assert.Error(t, err)
		})
	}
}

func TestLoadConfigAcceptsBoundaryValues(t *testing.T) {
	t.Setenv("RUNE_ETCD_ENDPOINTS", " etcd1:2379 , etcd2:2379")
	t.Setenv("RUNE_NODE_ADDR", "rune-0.rune:7946")
	t.Setenv("RUNE_STREAM_CHUNK_SIZE", "1")
	t.Setenv("RUNE_GC_INTERVAL", "1s")
	t.Setenv("RUNE_EVICTION_THRESHOLD", "1")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, []string{"etcd1:2379", "etcd2:2379"}, cfg.EtcdEndpoints)
}
