package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

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

	srv := server.New(cfg, store)
	if err := srv.Start(context.Background()); err != nil {
		log.Fatalf("start server: %v", err)
	}
	log.Printf("Rune listening on :%d", cfg.Port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Println("shutting down...")
	srv.Stop()
	if err := store.Close(); err != nil {
		log.Printf("close storage: %v", err)
	}
}
