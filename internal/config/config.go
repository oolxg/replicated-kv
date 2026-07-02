// Package config resolves the router's static configuration from the
// environment. Membership is static: the full storage-node list is injected at
// deploy time (Terraform), so there is no discovery/gossip.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Config is the router's runtime configuration.
type Config struct {
	Addr  string   // client-facing listen address, e.g. ":8080"
	Nodes []string // storage node addresses (host:port)
}

// FromEnv reads KV_ADDR (optional, default ":8080") and KV_NODES (required,
// comma-separated host:port list).
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
	return c, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
