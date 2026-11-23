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
	cases := map[string]struct {
		env  map[string]string
		want string
	}{
		"cluster without node addr":   {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379"}, "RUNE_NODE_ADDR"},
		"cluster with localhost addr": {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "localhost:7946"}, "RUNE_NODE_ADDR"},
		"cluster with 127.0.0.1 addr": {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "127.0.0.1:7946"}, "RUNE_NODE_ADDR"},
		"cluster with ::1 addr":       {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "[::1]:7946"}, "RUNE_NODE_ADDR"},
		"cluster with empty host":     {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": ":7946"}, "RUNE_NODE_ADDR"},
		"cluster with 0.0.0.0 addr":   {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "0.0.0.0:7946"}, "RUNE_NODE_ADDR"},
		"cluster with [::] addr":      {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "[::]:7946"}, "RUNE_NODE_ADDR"},
		"cluster with no port":        {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "rune-0"}, "RUNE_NODE_ADDR"},
		"cluster with empty port":     {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "rune-0:"}, "RUNE_NODE_ADDR"},
		"cluster with named port":     {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "rune-0:abc"}, "RUNE_NODE_ADDR"},
		"cluster with port 0":         {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "rune-0:0"}, "RUNE_NODE_ADDR"},
		"cluster with port 65536":     {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379", "RUNE_NODE_ADDR": "rune-0:65536"}, "RUNE_NODE_ADDR"},
		"empty etcd endpoint":         {map[string]string{"RUNE_ETCD_ENDPOINTS": "etcd:2379,", "RUNE_NODE_ADDR": "rune-0:7946"}, "RUNE_ETCD_ENDPOINTS"},
		"chunk size zero":             {map[string]string{"RUNE_STREAM_CHUNK_SIZE": "0"}, "RUNE_STREAM_CHUNK_SIZE"},
		"chunk size negative":         {map[string]string{"RUNE_STREAM_CHUNK_SIZE": "-1"}, "RUNE_STREAM_CHUNK_SIZE"},
		"chunk size 4194300":          {map[string]string{"RUNE_STREAM_CHUNK_SIZE": "4194300"}, "RUNE_STREAM_CHUNK_SIZE"},
		"gc interval zero":            {map[string]string{"RUNE_GC_INTERVAL": "0s"}, "RUNE_GC_INTERVAL"},
		"gc interval sub-second":      {map[string]string{"RUNE_GC_INTERVAL": "500ms"}, "RUNE_GC_INTERVAL"},
		"discard ratio zero":          {map[string]string{"RUNE_GC_DISCARD_RATIO": "0"}, "RUNE_GC_DISCARD_RATIO"},
		"discard ratio one":           {map[string]string{"RUNE_GC_DISCARD_RATIO": "1"}, "RUNE_GC_DISCARD_RATIO"},
		"threshold zero":              {map[string]string{"RUNE_EVICTION_THRESHOLD": "0"}, "RUNE_EVICTION_THRESHOLD"},
		"threshold above one":         {map[string]string{"RUNE_EVICTION_THRESHOLD": "7"}, "RUNE_EVICTION_THRESHOLD"},
		"max storage zero":            {map[string]string{"RUNE_MAX_STORAGE": "0"}, "RUNE_MAX_STORAGE"},
		"max storage negative":        {map[string]string{"RUNE_MAX_STORAGE": "-5GB"}, "RUNE_MAX_STORAGE"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			_, err := LoadConfig()
			assert.ErrorContains(t, err, c.want)
		})
	}
}

func TestLoadConfigAcceptsBoundaryValues(t *testing.T) {
	t.Setenv("RUNE_ETCD_ENDPOINTS", " etcd1:2379 , etcd2:2379")
	t.Setenv("RUNE_NODE_ADDR", "rune-0.rune:7946")
	t.Setenv("RUNE_STREAM_CHUNK_SIZE", "1")
	_, err := LoadConfig()
	require.NoError(t, err)

	t.Setenv("RUNE_STREAM_CHUNK_SIZE", "4194299")
	t.Setenv("RUNE_GC_INTERVAL", "1s")
	t.Setenv("RUNE_EVICTION_THRESHOLD", "1")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, []string{"etcd1:2379", "etcd2:2379"}, cfg.EtcdEndpoints)
}

func TestValueLimitsDefaultsAndOverrides(t *testing.T) {
	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, int64(512<<20), cfg.MaxValueBytes)
	assert.Equal(t, int64(2<<30), cfg.MaxInflightBytes)
	assert.Equal(t, 30*time.Second, cfg.GetIdleTimeout)

	t.Setenv("RUNE_MAX_VALUE_BYTES", "10MB")
	t.Setenv("RUNE_MAX_INFLIGHT_BYTES", "100MB")
	t.Setenv("RUNE_GET_IDLE_TIMEOUT", "5s")
	cfg, err = LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, cfg.GetIdleTimeout)
	assert.Equal(t, int64(10<<20), cfg.MaxValueBytes)
	assert.Equal(t, int64(100<<20), cfg.MaxInflightBytes)
}

func TestValueLimitsInvalid(t *testing.T) {
	cases := []struct{ maxValue, inflight string }{
		{"0", ""},
		{"-1", ""},
		{"abc", ""},
		{"", "0"},
		{"", "xyz"},
		{"100MB", "10MB"}, // budget smaller than one max-size value
		{"1GB", "4GB"},    // Badger rejects values of 1 GiB or more
	}
	for _, v := range []string{"0s", "-1s", "soon"} {
		t.Run("idle "+v, func(t *testing.T) {
			t.Setenv("RUNE_GET_IDLE_TIMEOUT", v)
			_, err := LoadConfig()
			assert.Error(t, err)
		})
	}
	for _, c := range cases {
		t.Run(c.maxValue+"/"+c.inflight, func(t *testing.T) {
			if c.maxValue != "" {
				t.Setenv("RUNE_MAX_VALUE_BYTES", c.maxValue)
			}
			if c.inflight != "" {
				t.Setenv("RUNE_MAX_INFLIGHT_BYTES", c.inflight)
			}
			_, err := LoadConfig()
			assert.Error(t, err)
		})
	}
}
