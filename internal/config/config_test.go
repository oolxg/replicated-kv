package config

import (
	"strings"
	"testing"
)

func TestFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantRF  int
		wantW   int
		wantR   int
		wantErr string // substring of the expected error; empty = no error
	}{
		{
			name:   "single node defaults to RF1 W1 R1",
			env:    map[string]string{"KV_NODES": "a:1"},
			wantRF: 1, wantW: 1, wantR: 1,
		},
		{
			name:   "three nodes default to RF3 W2 R2",
			env:    map[string]string{"KV_NODES": "a:1,b:1,c:1"},
			wantRF: 3, wantW: 2, wantR: 2,
		},
		{
			name:   "five nodes still default to RF3 W2 R2",
			env:    map[string]string{"KV_NODES": "a:1,b:1,c:1,d:1,e:1"},
			wantRF: 3, wantW: 2, wantR: 2,
		},
		{
			name:   "explicit overrides",
			env:    map[string]string{"KV_NODES": "a:1,b:1,c:1,d:1,e:1", "KV_RF": "5", "KV_W": "3", "KV_R": "1"},
			wantRF: 5, wantW: 3, wantR: 1,
		},
		{
			name:   "whitespace and empty entries tolerated",
			env:    map[string]string{"KV_NODES": " a:1 , ,b:1,"},
			wantRF: 2, wantW: 2, wantR: 2,
		},
		{
			name:    "missing KV_NODES",
			env:     map[string]string{},
			wantErr: "KV_NODES is required",
		},
		{
			name:    "RF exceeding node count",
			env:     map[string]string{"KV_NODES": "a:1,b:1,c:1", "KV_RF": "4"},
			wantErr: "KV_RF=4",
		},
		{
			name:    "W above RF",
			env:     map[string]string{"KV_NODES": "a:1,b:1,c:1", "KV_W": "4"},
			wantErr: "KV_W=4",
		},
		{
			name:    "zero R",
			env:     map[string]string{"KV_NODES": "a:1,b:1,c:1", "KV_R": "0"},
			wantErr: "KV_R=0",
		},
		{
			name:    "non-integer RF",
			env:     map[string]string{"KV_NODES": "a:1", "KV_RF": "lots"},
			wantErr: "not an integer",
		},
		{
			name:    "zero shed concurrency rejected",
			env:     map[string]string{"KV_NODES": "a:1", "KV_SHED_CONCURRENT": "0"},
			wantErr: "KV_SHED_CONCURRENT=0",
		},
		{
			name:    "negative shed queue rejected",
			env:     map[string]string{"KV_NODES": "a:1", "KV_SHED_QUEUE": "-5"},
			wantErr: "KV_SHED_QUEUE=-5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"KV_ADDR", "KV_NODES", "KV_RF", "KV_W", "KV_R", "KV_SHED_CONCURRENT", "KV_SHED_QUEUE"} {
				t.Setenv(k, tt.env[k]) // empty string reads as unset
			}
			c, err := FromEnv()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.RF != tt.wantRF || c.W != tt.wantW || c.R != tt.wantR {
				t.Fatalf("got RF%d/W%d/R%d, want RF%d/W%d/R%d", c.RF, c.W, c.R, tt.wantRF, tt.wantW, tt.wantR)
			}
		})
	}
}
