
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bryandguy/rune/internal/config"
	"github.com/bryandguy/rune/internal/server"
	"github.com/bryandguy/rune/internal/storage"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to config file (default: rune.yaml if it exists)")
	flag.StringVar(&configPath, "c", "", "path to config file (shorthand)")
	flag.Parse()

	// If --config not set and rune.yaml exists in working directory, use it.
	if configPath == "" {
		if _, err := os.Stat("rune.yaml"); err == nil {
			configPath = "rune.yaml"
		}
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store, err := storage.NewBadgerStore(cfg)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}

	srv := server.New(cfg, store)
	if err := srv.Start(); err != nil {
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
