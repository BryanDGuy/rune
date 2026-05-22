package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/bryandguy/rune/internal/cluster"
	"github.com/bryandguy/rune/internal/config"
	"github.com/bryandguy/rune/internal/logging"
	"github.com/bryandguy/rune/internal/server"
	"github.com/bryandguy/rune/internal/storage"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		logging.New(logging.LevelInfo).Error("load config", "err", err)
		os.Exit(1)
	}

	logger := logging.New(logging.ParseLevel(cfg.LogLevel))

	store, err := storage.NewBadgerStore(cfg)
	if err != nil {
		logger.Error("open storage", "err", err)
		os.Exit(1)
	}

	srv, clusterCleanup := buildServer(cfg, store, logger)

	if err := srv.Start(context.Background()); err != nil {
		clusterCleanup()
		logger.Error("start server", "err", err)
		os.Exit(1)
	}
	logger.Info("rune listening", "port", cfg.Port, "cluster", len(cfg.EtcdEndpoints) > 0)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	logger.Info("shutting down")
	srv.Stop()
	clusterCleanup()
	if err := store.Close(); err != nil {
		logger.Error("close storage", "err", err)
	}
}

func buildServer(cfg *config.Config, store storage.Storage, logger *logging.Logger) (*server.Server, func()) {
	if len(cfg.EtcdEndpoints) == 0 {
		return server.New(cfg, store, logger, nil), func() {}
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("connect etcd", "err", err)
		os.Exit(1)
	}

	m := cluster.New(etcdClient, cfg.NodeID, cfg.NodeAddr, logger)
	if err := m.Start(context.Background()); err != nil {
		_ = etcdClient.Close()
		logger.Error("start membership", "err", err)
		os.Exit(1)
	}

	dialer := cluster.NewPeerDialer()
	cleanup := func() {
		m.Stop()
		dialer.Close()
		_ = etcdClient.Close()
	}
	return server.New(cfg, store, logger, &server.ClusterOptions{Membership: m, Dialer: dialer}), cleanup
}
