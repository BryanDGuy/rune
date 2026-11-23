package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Defaults for the memory limits. server.New also applies them to zero fields,
// so a Config built without LoadConfig does not reject all traffic.
const (
	DefaultMaxValueBytes    = 512 * 1024 * 1024      // 512MB
	DefaultMaxInflightBytes = 2 * 1024 * 1024 * 1024 // 2GB
	DefaultGetIdleTimeout   = 30 * time.Second

	// badgerMaxValueBytes is Badger v4's limit: Txn.SetEntry rejects values longer
	// than Options.ValueLogFileSize (default 1<<30 - 1, badger txn.go:368).
	badgerMaxValueBytes = 1<<30 - 1
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
	MaxValueBytes      int64
	MaxInflightBytes   int64
	GetIdleTimeout     time.Duration
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
//	RUNE_MAX_VALUE_BYTES    (default: 512MB — larger Sets fail with ResourceExhausted)
//	RUNE_MAX_INFLIGHT_BYTES (default: 2GB — server-wide cap on buffered Set/Get value bytes; must be >= RUNE_MAX_VALUE_BYTES)
//	RUNE_GET_IDLE_TIMEOUT   (default: 30s — a Get whose client reads nothing for this long is ended, releasing its bytes)
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
		MaxValueBytes:      DefaultMaxValueBytes,
		MaxInflightBytes:   DefaultMaxInflightBytes,
		GetIdleTimeout:     DefaultGetIdleTimeout,
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
	if v := os.Getenv("RUNE_MAX_VALUE_BYTES"); v != "" {
		if cfg.MaxValueBytes, err = parseBytes(v); err != nil {
			return nil, fmt.Errorf("RUNE_MAX_VALUE_BYTES: %w", err)
		}
	}
	if v := os.Getenv("RUNE_MAX_INFLIGHT_BYTES"); v != "" {
		if cfg.MaxInflightBytes, err = parseBytes(v); err != nil {
			return nil, fmt.Errorf("RUNE_MAX_INFLIGHT_BYTES: %w", err)
		}
	}
	if v := os.Getenv("RUNE_GET_IDLE_TIMEOUT"); v != "" {
		if cfg.GetIdleTimeout, err = time.ParseDuration(v); err != nil {
			return nil, fmt.Errorf("RUNE_GET_IDLE_TIMEOUT: %w", err)
		}
	}
	if v := os.Getenv("RUNE_ETCD_ENDPOINTS"); v != "" {
		for e := range strings.SplitSeq(v, ",") {
			cfg.EtcdEndpoints = append(cfg.EtcdEndpoints, strings.TrimSpace(e))
		}
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate rejects values that would hang, crash or silently split the cluster at runtime.
func (c *Config) Validate() error {
	var errs []error
	if len(c.EtcdEndpoints) > 0 {
		host, port, err := net.SplitHostPort(c.NodeAddr)
		ip := net.ParseIP(host)
		p, perr := strconv.Atoi(port)
		if err != nil || host == "" || perr != nil || p < 1 || p > 65535 || strings.EqualFold(host, "localhost") || (ip != nil && (ip.IsLoopback() || ip.IsUnspecified())) {
			errs = append(errs, fmt.Errorf("RUNE_NODE_ADDR %q: cluster mode needs a host:port (port 1-65535) peers can reach, not empty, loopback or unspecified", c.NodeAddr))
		}
		if slices.Contains(c.EtcdEndpoints, "") {
			errs = append(errs, errors.New("RUNE_ETCD_ENDPOINTS: contains an empty endpoint"))
		}
	}
	// gRPC's 4 MiB default recv limit counts the proto tag (1 B) + length varint (4 B).
	if c.StreamChunkSize <= 0 || c.StreamChunkSize > 4<<20-5 {
		errs = append(errs, fmt.Errorf("RUNE_STREAM_CHUNK_SIZE %d: must be in (0, 4194299]", c.StreamChunkSize))
	}
	if c.GCInterval < time.Second {
		errs = append(errs, fmt.Errorf("RUNE_GC_INTERVAL %s: must be at least 1s", c.GCInterval))
	}
	if c.GCDiscardRatio <= 0 || c.GCDiscardRatio >= 1 {
		errs = append(errs, fmt.Errorf("RUNE_GC_DISCARD_RATIO %g: must be in (0, 1)", c.GCDiscardRatio))
	}
	if c.EvictionThreshold <= 0 || c.EvictionThreshold > 1 {
		errs = append(errs, fmt.Errorf("RUNE_EVICTION_THRESHOLD %g: must be in (0, 1]", c.EvictionThreshold))
	}
	if c.MaxStorageBytes <= 0 {
		errs = append(errs, fmt.Errorf("RUNE_MAX_STORAGE %d: must be > 0", c.MaxStorageBytes))
	}
	if c.MaxValueBytes <= 0 || c.MaxValueBytes > badgerMaxValueBytes {
		errs = append(errs, fmt.Errorf("RUNE_MAX_VALUE_BYTES must be in (0, %d], got %d", badgerMaxValueBytes, c.MaxValueBytes))
	}
	if c.MaxInflightBytes < c.MaxValueBytes {
		errs = append(errs, fmt.Errorf("RUNE_MAX_INFLIGHT_BYTES (%d) must be >= RUNE_MAX_VALUE_BYTES (%d)", c.MaxInflightBytes, c.MaxValueBytes))
	}
	if c.GetIdleTimeout <= 0 {
		errs = append(errs, fmt.Errorf("RUNE_GET_IDLE_TIMEOUT must be > 0, got %s", c.GetIdleTimeout))
	}
	return errors.Join(errs...)
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
