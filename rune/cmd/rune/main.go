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

// leaveDelay gives peers' etcd watches time to see this node leave before it stops serving forwards.
const leaveDelay = 2 * time.Second

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

	requestedMax := cfg.MaxStorageBytes
	store, err := storage.NewBadgerStore(cfg, m)
	if err != nil {
		_ = metricsLis.Close()
		logger.Error("open storage", "err", err)
		os.Exit(1)
	}
	store.SetLogger(logger)
	if cfg.MaxStorageBytes < requestedMax {
		logger.Warn("RUNE_MAX_STORAGE exceeds filesystem size; clamped", "requested_bytes", requestedMax, "effective_bytes", cfg.MaxStorageBytes)
	}

	srv, join, shutdown := buildServer(cfg, store, logger, m)
	fail := func(msg string, err error) {
		shutdown()
		if closeErr := store.Close(); closeErr != nil {
			logger.Error("close storage", "err", closeErr)
		}
		_ = metricsLis.Close()
		logger.Error(msg, "err", err)
		os.Exit(1)
	}

	if err := srv.Start(context.Background()); err != nil {
		fail("start server", err)
	}
	// Register in the ring only once the server is serving, so peers never forward to a closed port.
	if err := join(context.Background()); err != nil {
		fail("start membership", err)
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
	signal.Stop(quit) // a second SIGTERM/Ctrl-C kills the process instead of waiting out the drain

	logger.Info("shutting down")
	shutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics shutdown", "err", err)
	}
	if err := store.Close(); err != nil {
		logger.Error("close storage", "err", err)
	}
}

// buildServer returns the server, a join func that registers this node in the
// ring and marks it Ready (call after srv.Start), and a shutdown func that stops everything in order.
func buildServer(cfg *config.Config, store storage.Storage, logger *logging.Logger, m *telemetry.Metrics) (*server.Server, func(context.Context) error, func()) {
	if len(cfg.EtcdEndpoints) == 0 {
		srv := server.New(cfg, store, logger, nil, m)
		return srv, func(context.Context) error { srv.MarkReady(); return nil }, srv.Stop
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
	dialer := cluster.NewPeerDialer()
	membership.OnPeerLeave = dialer.Forget
	srv := server.New(cfg, store, logger, &server.ClusterOptions{Membership: membership, Dialer: dialer}, m)
	joined := false
	shutdown := func() {
		// Stop revokes the lease, so peers drop this node from their rings.
		switch err := membership.Stop(); {
		case err != nil:
			// Peers keep this node until its lease expires; keep serving until then.
			logger.Warn("lease revoke failed; waiting for lease expiry", "err", err)
			time.Sleep(cluster.LeaseTTL + leaveDelay)
		case joined:
			time.Sleep(leaveDelay)
		}
		srv.Stop()
		dialer.Close()
		_ = etcdClient.Close()
	}
	join := func(ctx context.Context) error {
		if err := membership.Start(ctx); err != nil {
			return err
		}
		joined = true
		srv.MarkReady()
		return nil
	}
	return srv, join, shutdown
}
