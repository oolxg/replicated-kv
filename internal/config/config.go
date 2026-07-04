// Package config resolves the router's static configuration from the
// environment. Membership is static: the full storage-node list is injected at
// deploy time (Terraform), so there is no discovery/gossip.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
