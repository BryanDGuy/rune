// Copyright 2026 BryanDGuy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
