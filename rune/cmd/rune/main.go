package main

import (
	"context"
	"fmt"
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
	"github.com/bryandguy/rune/rune/internal/metrics"
	"github.com/bryandguy/rune/rune/internal/server"
	"github.com/bryandguy/rune/rune/internal/storage"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		logging.New(logging.LevelInfo).Error("load config", "err", err)
		os.Exit(1)
	}

	logger := logging.New(logging.ParseLevel(cfg.LogLevel))

	m := metrics.New()

	store, err := storage.NewBadgerStore(cfg, m)
	if err != nil {
		logger.Error("open storage", "err", err)
		os.Exit(1)
	}

	srv, clusterCleanup := buildServer(cfg, store, logger, m)

	if err := srv.Start(context.Background()); err != nil {
		clusterCleanup()
		logger.Error("start server", "err", err)
		os.Exit(1)
	}
	logger.Info("rune listening", "port", cfg.Port, "cluster", len(cfg.EtcdEndpoints) > 0)

	metricsSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler:           m.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() { _ = metricsSrv.ListenAndServe() }()
	logger.Info("metrics listening", "port", cfg.MetricsPort)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	srv.Stop()
	clusterCleanup()
	if err := store.Close(); err != nil {
		logger.Error("close storage", "err", err)
	}
}

func buildServer(cfg *config.Config, store storage.Storage, logger *logging.Logger, m *metrics.Metrics) (*server.Server, func()) {
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
