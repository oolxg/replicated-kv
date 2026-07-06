// Command storage runs a single, standalone storage node: an in-memory
// key-value store fronted by the internal HTTP API. It is the stateful tier of
// the system. At this layer it has no knowledge of the ring or of replication;
// those are added by the router/coordinator layers.
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
	"github.com/oolxg/replicated-kv/internal/shed"
	"github.com/oolxg/replicated-kv/internal/storage"
	"github.com/oolxg/replicated-kv/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.StorageFromEnv()
	if err != nil {
		logger.Error("configuration error", "err", err)
		os.Exit(1)
	}

	limiter := shed.New(cfg.ShedConcurrent, cfg.ShedQueue)
	handler := storage.NewHandler(store.New(), limiter, logger)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Translate SIGINT/SIGTERM into context cancellation for a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("storage node listening",
			"addr", cfg.Addr, "shed_concurrent", cfg.ShedConcurrent, "shed_queue", cfg.ShedQueue)
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
	logger.Info("storage node stopped")
}
