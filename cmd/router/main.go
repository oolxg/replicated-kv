// Command router runs a stateless coordinator: it owns the consistent-hash
// ring and forwards client GET/PUT requests to the responsible storage nodes.
// It holds no durable state, so it is horizontally scalable and restart-safe.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oolxg/replicated-kv/internal/config"
	"github.com/oolxg/replicated-kv/internal/coordinator"
	"github.com/oolxg/replicated-kv/internal/ring"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("configuration error", "err", err)
		os.Exit(1)
	}

	coord := coordinator.New(ring.New(cfg.Nodes), logger)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           coord.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("router listening", "addr", cfg.Addr, "nodes", cfg.Nodes)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		logger.Error("server failed", "err", err)
		os.Exit(1)
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
		os.Exit(1)
	}
	logger.Info("router stopped")
}
