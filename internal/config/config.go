package config

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Port               int           `yaml:"port"`
	DataDir            string        `yaml:"data-dir"`
	MaxStorage         string        `yaml:"max-storage"`
	MaxStorageBytes    int64         `yaml:"-"`
	EvictionThreshold  float64       `yaml:"eviction-threshold"`
	EvictionSizeWeight float64       `yaml:"eviction-size-weight"`
	EvictionAgeWeight  float64       `yaml:"eviction-age-weight"`
	StreamChunkSize    int           `yaml:"stream-chunk-size"`
	GCInterval         time.Duration `yaml:"gc-interval"`
	GCDiscardRatio     float64       `yaml:"gc-discard-ratio"`
	TTLSweepInterval   time.Duration `yaml:"ttl-sweep-interval"`
	LogLevel           string        `yaml:"log-level"`
	MetricsPort        int           `yaml:"metrics-port"`
}

func defaults() *Config {
	return &Config{
		Port:               7946,
		DataDir:            "/var/rune/data",
		MaxStorage:         "100GB",
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1024 * 1024, // 1MB
		GCInterval:         10 * time.Minute,
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   60 * time.Second,
		LogLevel:           "info",
		MetricsPort:        9090,
	}
}

func LoadConfig(path string) (*Config, error) {
	cfg := defaults()

	if path != "" {
		absPath, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			return nil, fmt.Errorf("resolve config path: %w", err)
		}
		data, err := fs.ReadFile(os.DirFS(filepath.Dir(absPath)), filepath.Base(absPath))
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}

	if cfg.MaxStorage != "" {
		b, err := parseBytes(cfg.MaxStorage)
		if err != nil {
			return nil, fmt.Errorf("parse max-storage: %w", err)
		}
		cfg.MaxStorageBytes = b
	}

	if v := os.Getenv("RUNE_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("RUNE_PORT: %w", err)
		}
		cfg.Port = n
	}
	if v := os.Getenv("RUNE_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("RUNE_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
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
