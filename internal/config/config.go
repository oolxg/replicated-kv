// Package config resolves the router's static configuration from the
// environment. Membership is static: the full storage-node list is injected at
// deploy time (Terraform), so there is no discovery/gossip.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the router's runtime configuration.
type Config struct {
	Addr  string   // client-facing listen address, e.g. ":8080"
	Nodes []string // storage node addresses (host:port)

	// Quorum parameters. Defaults derive from the node count:
	// RF = min(3, len(Nodes)), W = R = RF/2+1 (majority), which yields the
	// assignment's configs — 1 node: RF1/W1/R1; 3 or 5 nodes: RF3/W2/R2 —
	// with no explicit configuration. W+R > RF makes reads overlap the
	// newest committed write.
	RF int // replication factor: how many replicas hold each key
	W  int // write quorum: acks required before a PUT succeeds
	R  int // read quorum: replies required before a GET succeeds

	// Load-shedding bounds for the client-facing edge (KV_SHED_CONCURRENT /
	// KV_SHED_QUEUE). Requests beyond concurrent+queue are answered 503.
	ShedConcurrent int
	ShedQueue      int

	// Read-through cache for hot keys (KV_CACHE_SIZE / KV_CACHE_TTL).
	// CacheSize 0 disables caching. The TTL bounds cross-router staleness:
	// another router's write becomes visible here at most TTL later.
	CacheSize int
	CacheTTL  time.Duration
}

// StorageConfig is a storage node's runtime configuration.
type StorageConfig struct {
	Addr string // listen address, e.g. ":8080"

	// Load-shedding bounds for the internal API. Storage handlers are
	// microsecond-fast in-memory operations, so the limit exists to bound
	// goroutine and memory blowup under fan-in from multiple routers, not to
	// pace the CPU.
	ShedConcurrent int
	ShedQueue      int
}

// StorageFromEnv reads KV_ADDR (default ":8080") and optional
// KV_SHED_CONCURRENT / KV_SHED_QUEUE overrides.
func StorageFromEnv() (StorageConfig, error) {
	c := StorageConfig{Addr: envOr("KV_ADDR", ":8080")}
	var err error
	if c.ShedConcurrent, err = intEnv("KV_SHED_CONCURRENT", 256); err != nil {
		return StorageConfig{}, err
	}
	if c.ShedQueue, err = intEnv("KV_SHED_QUEUE", 512); err != nil {
		return StorageConfig{}, err
	}
	if err := validateShed(c.ShedConcurrent, c.ShedQueue); err != nil {
		return StorageConfig{}, err
	}
	return c, nil
}

func validateShed(concurrent, queue int) error {
	if concurrent < 1 {
		return fmt.Errorf("KV_SHED_CONCURRENT=%d must be at least 1", concurrent)
	}
	if queue < 0 {
		return fmt.Errorf("KV_SHED_QUEUE=%d must not be negative", queue)
	}
	return nil
}

// FromEnv reads KV_ADDR (default ":8080"), KV_NODES (required, comma-separated
// host:port list), and optional KV_RF / KV_W / KV_R overrides.
func FromEnv() (Config, error) {
	c := Config{Addr: envOr("KV_ADDR", ":8080")}

	raw := os.Getenv("KV_NODES")
	if raw == "" {
		return Config{}, fmt.Errorf("KV_NODES is required (comma-separated host:port list)")
	}
	for _, n := range strings.Split(raw, ",") {
		if n = strings.TrimSpace(n); n != "" {
			c.Nodes = append(c.Nodes, n)
		}
	}
	if len(c.Nodes) == 0 {
		return Config{}, fmt.Errorf("KV_NODES contained no usable addresses")
	}

	var err error
	if c.RF, err = intEnv("KV_RF", min(3, len(c.Nodes))); err != nil {
		return Config{}, err
	}
	if c.W, err = intEnv("KV_W", c.RF/2+1); err != nil {
		return Config{}, err
	}
	if c.R, err = intEnv("KV_R", c.RF/2+1); err != nil {
		return Config{}, err
	}

	if c.RF < 1 || c.RF > len(c.Nodes) {
		return Config{}, fmt.Errorf("KV_RF=%d must be between 1 and the node count (%d)", c.RF, len(c.Nodes))
	}
	if c.W < 1 || c.W > c.RF {
		return Config{}, fmt.Errorf("KV_W=%d must be between 1 and KV_RF (%d)", c.W, c.RF)
	}
	if c.R < 1 || c.R > c.RF {
		return Config{}, fmt.Errorf("KV_R=%d must be between 1 and KV_RF (%d)", c.R, c.RF)
	}

	// The router is IO-bound fan-out work, so its edge admits more
	// concurrency than a storage node by default.
	if c.ShedConcurrent, err = intEnv("KV_SHED_CONCURRENT", 1024); err != nil {
		return Config{}, err
	}
	if c.ShedQueue, err = intEnv("KV_SHED_QUEUE", 1024); err != nil {
		return Config{}, err
	}
	if err := validateShed(c.ShedConcurrent, c.ShedQueue); err != nil {
		return Config{}, err
	}

	if c.CacheSize, err = intEnv("KV_CACHE_SIZE", 4096); err != nil {
		return Config{}, err
	}
	if c.CacheSize < 0 {
		return Config{}, fmt.Errorf("KV_CACHE_SIZE=%d must not be negative (0 disables the cache)", c.CacheSize)
	}
	if c.CacheTTL, err = durationEnv("KV_CACHE_TTL", time.Second); err != nil {
		return Config{}, err
	}
	if c.CacheSize > 0 && c.CacheTTL <= 0 {
		return Config{}, fmt.Errorf("KV_CACHE_TTL=%s must be positive when the cache is enabled", c.CacheTTL)
	}
	return c, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not an integer", key, v)
	}
	return n, nil
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a duration (e.g. 500ms, 2s)", key, v)
	}
	return d, nil
}
