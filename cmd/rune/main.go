package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/bryandguy/rune/internal/cluster"
	"github.com/bryandguy/rune/internal/config"
	"github.com/bryandguy/rune/internal/server"
	"github.com/bryandguy/rune/internal/storage"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store, err := storage.NewBadgerStore(cfg)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}

	srv, clusterCleanup := buildServer(cfg, store)

	if err := srv.Start(context.Background()); err != nil {
		clusterCleanup()
		log.Fatalf("start server: %v", err)
	}
	log.Printf("Rune listening on :%d (cluster=%v)", cfg.Port, len(cfg.EtcdEndpoints) > 0)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Println("shutting down...")
	srv.Stop()
	clusterCleanup()
	if err := store.Close(); err != nil {
		log.Printf("close storage: %v", err)
	}
}

func buildServer(cfg *config.Config, store storage.Storage) (*server.Server, func()) {
	if len(cfg.EtcdEndpoints) == 0 {
		return server.New(cfg, store, nil), func() {}
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("connect etcd: %v", err)
	}

	m := cluster.New(etcdClient, cfg.NodeID, cfg.NodeAddr)
	if err := m.Start(context.Background()); err != nil {
		_ = etcdClient.Close()
		log.Fatalf("start membership: %v", err)
	}

	dialer := cluster.NewPeerDialer()
	cleanup := func() {
		m.Stop()
		dialer.Close()
		_ = etcdClient.Close()
	}
	return server.New(cfg, store, &server.ClusterOptions{Membership: m, Dialer: dialer}), cleanup
}
