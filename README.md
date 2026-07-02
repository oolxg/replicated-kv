# replicated-kv

A replicated, in-memory key-value store built for the TU Berlin **Scalability
Engineering** (Summer Semester 2026) prototyping assignment. It demonstrates
horizontal and vertical scaling, hand-implemented overload mitigation, and
quorum replication with a clean stateless/stateful split.

> **Status — built layer by layer.** Each layer compiles, runs, and passes its
> tests before the next begins. See [Build status](#build-status).

## Architecture (target)

```
                 client (k6)
                     |
            +--------v---------+   stateless: holds the hash ring only.
            |   Router/Coord   |   Computes the preference list per key,
            | (stateless tier) |   runs quorum read/write, reconciles (LWW).
            +--------+---------+
       forward by hash(key) to the N replicas in the preference list
        +------------+------------+------------+
   +----v----+  +----v----+  +----v----+  +----v----+
   | storage |  | storage |  | storage |  | storage |  stateful: each owns a
   |  node   |  |  node   |  |  node   |  |  node   |  slice of the keyspace
   +---------+  +---------+  +---------+  +---------+  in a sharded in-memory map.
```

- **Router** = stateless coordinator. Restart-safe, horizontally scalable.
- **Storage node** = stateful replica. Owns partitions of the keyspace.

Membership is **static**: the full node list is injected at deploy time. No
gossip / dynamic discovery (a deliberate scope limit, documented on the
limitations slide).

## Build status

| Layer | Scope | Status |
|------:|-------|:------:|
| 1 | Standalone storage node — sharded in-memory store + internal HTTP API | ✅ |
| 2 | Router + consistent-hash ring + forwarding (RF=1) | ✅ |
| 3 | Replication + quorum + LWW reconcile | ⏳ |
| 4 | Load shedding (overload mitigation) | ⏳ |
| 5 | Retries (backoff + jitter) + read-through cache | ⏳ |
| 6 | Terraform deploy (1 / 3 / 5 nodes) | ⏳ |
| 7 | k6 benchmark + scaling graph | ⏳ |

## Layer 1 — standalone storage node

The stateful tier in isolation: one node, an in-memory store with last-writer-
wins (LWW) versioning, and the internal HTTP API. No ring, no replication yet.

### Store semantics

- Keyspace striped across 32 independently-locked shards (FNV-1a of the key) so
  writers to different keys do not contend on a single lock.
- Each value carries the timestamp it was written at. `Put` applies a write iff
  its timestamp is newer than the stored one — or equal, with a
  lexicographically-greater value (a deterministic tie-break so replicas
  converge regardless of write arrival order).

### Internal HTTP API (called by the router only)

| Method | Path | Body | Responses |
|--------|------|------|-----------|
| `GET` | `/internal/get/{key}` | — | `200 {value,timestamp}` · `404` |
| `PUT` | `/internal/put/{key}` | `{value,timestamp}` | `200 {applied}` · `400` |
| `GET` | `/healthz` | — | `200` |

### Run it

```sh
# from the repo root
go test -race ./...                       # unit tests (race detector on)
go vet ./...
go build -o bin/storage ./cmd/storage

KV_ADDR=:8080 ./bin/storage               # start a node

curl -s -XPUT localhost:8080/internal/put/foo \
  -d '{"value":"bar","timestamp":1}'      # -> {"applied":true}
curl -s localhost:8080/internal/get/foo   # -> {"value":"bar","timestamp":1}
curl -s localhost:8080/healthz            # -> {"status":"ok"}
```

## Layer 2 — router + ring + forwarding (RF=1)

The stateless tier. The router owns a consistent-hash ring (150 virtual nodes
per physical node, SHA-256 placement) and forwards each client request to the
single node responsible for the key. It assigns the write timestamp, so LWW has
one clock per write path. No replication yet (RF=1).

### Client-facing HTTP API (on the router)

| Method | Path | Body | Responses |
|--------|------|------|-----------|
| `GET` | `/kv/{key}` | — | `200 {value,timestamp}` · `404` · `502` |
| `PUT` | `/kv/{key}` | `{value}` | `200` · `400` · `502` |
| `GET` | `/healthz` | — | `200` |

### Run it (1 router + 3 storage nodes, locally)

```sh
go build -o bin/router ./cmd/router
go build -o bin/storage ./cmd/storage

KV_ADDR=:19001 ./bin/storage &
KV_ADDR=:19002 ./bin/storage &
KV_ADDR=:19003 ./bin/storage &
KV_ADDR=:19000 KV_NODES=127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003 ./bin/router &

curl -s -XPUT localhost:19000/kv/apple -d '{"value":"apple-v"}'  # -> 200
curl -s localhost:19000/kv/apple    # -> {"value":"apple-v","timestamp":...}
```

`KV_NODES` is the static membership list (comma-separated `host:port`), injected
at deploy time. `KV_ADDR` defaults to `:8080`.

## License

[MIT](LICENSE).
