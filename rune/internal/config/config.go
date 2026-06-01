package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	LogLevel           string
	DataDir            string
	NodeID             string
	NodeAddr           string
	EtcdEndpoints      []string
	EvictionThreshold  float64
	MaxStorageBytes    int64
	EvictionSizeWeight float64
	EvictionAgeWeight  float64
	GCInterval         time.Duration
	GCDiscardRatio     float64
	Port               int
	MetricsPort        int
	StreamChunkSize    int
	BlockCacheSize     int64
}

// LoadConfig builds a Config from environment variables, falling back to defaults.
//
// Variables:
//
//	RUNE_PORT               (default: 7946)
//	RUNE_METRICS_PORT       (default: 9090)
//	RUNE_DATA_DIR           (default: /var/rune/data)
//	RUNE_LOG_LEVEL          (default: info)
//	RUNE_MAX_STORAGE        (default: 100GB)
//	RUNE_EVICTION_THRESHOLD (default: 0.8)
//	RUNE_EVICTION_SIZE_WEIGHT (default: 1.0)
//	RUNE_EVICTION_AGE_WEIGHT  (default: 1.0)
//	RUNE_STREAM_CHUNK_SIZE  (default: 1048576)
//	RUNE_GC_INTERVAL        (default: 10m)
//	RUNE_GC_DISCARD_RATIO   (default: 0.5)
//	RUNE_ETCD_ENDPOINTS     (default: "" — single-node mode; comma-separated in cluster mode)
//	RUNE_BLOCK_CACHE_SIZE   (default: 0 — use Badger's built-in default; e.g. "256MB" enables a larger cache for read-heavy workloads)
//	RUNE_NODE_ID            (default: hostname)
//	RUNE_NODE_ADDR          (default: localhost:{RUNE_PORT})
func LoadConfig() (*Config, error) {
	cfg := &Config{
		Port:               7946,
		MetricsPort:        9090,
		DataDir:            "/var/rune/data",
		LogLevel:           "info",
		MaxStorageBytes:    100 * 1024 * 1024 * 1024, // 100GB
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1024 * 1024, // 1MB
		GCInterval:         10 * time.Minute,
		GCDiscardRatio:     0.5,
	}

	var err error

	if v := os.Getenv("RUNE_PORT"); v != "" {
		if cfg.Port, err = strconv.Atoi(v); err != nil {
			return nil, fmt.Errorf("RUNE_PORT: %w", err)
		}
	}
	if v := os.Getenv("RUNE_METRICS_PORT"); v != "" {
		if cfg.MetricsPort, err = strconv.Atoi(v); err != nil {
			return nil, fmt.Errorf("RUNE_METRICS_PORT: %w", err)
		}
	}
	if v := os.Getenv("RUNE_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("RUNE_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("RUNE_MAX_STORAGE"); v != "" {
		if cfg.MaxStorageBytes, err = parseBytes(v); err != nil {
			return nil, fmt.Errorf("RUNE_MAX_STORAGE: %w", err)
		}
	}
	if v := os.Getenv("RUNE_EVICTION_THRESHOLD"); v != "" {
		if cfg.EvictionThreshold, err = strconv.ParseFloat(v, 64); err != nil {
			return nil, fmt.Errorf("RUNE_EVICTION_THRESHOLD: %w", err)
		}
	}
	if v := os.Getenv("RUNE_EVICTION_SIZE_WEIGHT"); v != "" {
		if cfg.EvictionSizeWeight, err = strconv.ParseFloat(v, 64); err != nil {
			return nil, fmt.Errorf("RUNE_EVICTION_SIZE_WEIGHT: %w", err)
		}
	}
	if v := os.Getenv("RUNE_EVICTION_AGE_WEIGHT"); v != "" {
		if cfg.EvictionAgeWeight, err = strconv.ParseFloat(v, 64); err != nil {
			return nil, fmt.Errorf("RUNE_EVICTION_AGE_WEIGHT: %w", err)
		}
	}
	if v := os.Getenv("RUNE_STREAM_CHUNK_SIZE"); v != "" {
		if cfg.StreamChunkSize, err = strconv.Atoi(v); err != nil {
			return nil, fmt.Errorf("RUNE_STREAM_CHUNK_SIZE: %w", err)
		}
	}
	if v := os.Getenv("RUNE_GC_INTERVAL"); v != "" {
		if cfg.GCInterval, err = time.ParseDuration(v); err != nil {
			return nil, fmt.Errorf("RUNE_GC_INTERVAL: %w", err)
		}
	}
	if v := os.Getenv("RUNE_GC_DISCARD_RATIO"); v != "" {
		if cfg.GCDiscardRatio, err = strconv.ParseFloat(v, 64); err != nil {
			return nil, fmt.Errorf("RUNE_GC_DISCARD_RATIO: %w", err)
		}
	}
	if v := os.Getenv("RUNE_BLOCK_CACHE_SIZE"); v != "" {
		if cfg.BlockCacheSize, err = parseBytes(v); err != nil {
			return nil, fmt.Errorf("RUNE_BLOCK_CACHE_SIZE: %w", err)
		}
	}
	if v := os.Getenv("RUNE_ETCD_ENDPOINTS"); v != "" {
		cfg.EtcdEndpoints = strings.Split(v, ",")
	}
	hostname, _ := os.Hostname()
	cfg.NodeID = hostname
	if v := os.Getenv("RUNE_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	cfg.NodeAddr = fmt.Sprintf("localhost:%d", cfg.Port)
	if v := os.Getenv("RUNE_NODE_ADDR"); v != "" {
		cfg.NodeAddr = v
	}
	return cfg, nil
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}
	upper := strings.ToUpper(s)
	for _, entry := range suffixes {
		if trimmed, ok := strings.CutSuffix(upper, entry.suffix); ok {
			n, err := strconv.ParseInt(trimmed, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			return n * entry.mult, nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format %q", s)
	}
	return n, nil
}
