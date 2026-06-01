package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	_ "google.golang.org/grpc/encoding/gzip" // registers gzip decompressor for compressed client requests

	"github.com/bryandguy/rune/rune/internal/cluster"
	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/logging"
	"github.com/bryandguy/rune/rune/internal/server"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/bryandguy/rune/rune/internal/telemetry"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		logging.New(logging.LevelInfo).Error("load config", "err", err)
		os.Exit(1)
	}

	logger := logging.New(logging.ParseLevel(cfg.LogLevel))

	m := telemetry.New()

	metricsLis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", fmt.Sprintf(":%d", cfg.MetricsPort))
	if err != nil {
		logger.Error("metrics listen", "err", err)
		os.Exit(1)
	}

	store, err := storage.NewBadgerStore(cfg, m)
	if err != nil {
		_ = metricsLis.Close()
		logger.Error("open storage", "err", err)
		os.Exit(1)
	}

	srv, clusterCleanup := buildServer(cfg, store, logger, m)

	if err := srv.Start(context.Background()); err != nil {
		clusterCleanup()
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("close storage", "err", closeErr)
		}
		_ = metricsLis.Close()
		logger.Error("start server", "err", err)
		os.Exit(1)
	}
	logger.Info("rune listening", "port", cfg.Port, "cluster", len(cfg.EtcdEndpoints) > 0)

	metricsSrv := &http.Server{
		Handler:           m.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := metricsSrv.Serve(metricsLis); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "err", err)
		}
	}()
	logger.Info("metrics listening", "port", cfg.MetricsPort)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	logger.Info("shutting down")
	srv.Stop()
	clusterCleanup()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics shutdown", "err", err)
	}
	if err := store.Close(); err != nil {
		logger.Error("close storage", "err", err)
	}
}

func buildServer(cfg *config.Config, store storage.Storage, logger *logging.Logger, m *telemetry.Metrics) (*server.Server, func()) {
	if len(cfg.EtcdEndpoints) == 0 {
		return server.New(cfg, store, logger, nil, m), func() {}
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("connect etcd", "err", err)
		os.Exit(1)
	}

	membership := cluster.New(etcdClient, cfg.NodeID, cfg.NodeAddr, logger)
	if err := membership.Start(context.Background()); err != nil {
		_ = etcdClient.Close()
		logger.Error("start membership", "err", err)
		os.Exit(1)
	}

	dialer := cluster.NewPeerDialer()
	cleanup := func() {
		membership.Stop()
		dialer.Close()
		_ = etcdClient.Close()
	}
	return server.New(cfg, store, logger, &server.ClusterOptions{Membership: membership, Dialer: dialer}, m), cleanup
}
