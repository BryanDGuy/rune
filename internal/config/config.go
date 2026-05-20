package config

import (
	"fmt"
	"os"
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
		MaxStorageBytes:    107374182400,
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
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
		if cfg.MaxStorage != "" {
			b, err := parseBytes(cfg.MaxStorage)
			if err != nil {
				return nil, fmt.Errorf("parse max-storage: %w", err)
			}
			cfg.MaxStorageBytes = b
		}
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
	multipliers := map[string]int64{
		"KB": 1024,
		"MB": 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"TB": 1024 * 1024 * 1024 * 1024,
	}
	upper := strings.ToUpper(s)
	for suffix, mult := range multipliers {
		if strings.HasSuffix(upper, suffix) {
			n, err := strconv.ParseInt(strings.TrimSuffix(upper, suffix), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			return n * mult, nil
		}
	}
	return strconv.ParseInt(s, 10, 64)
}
