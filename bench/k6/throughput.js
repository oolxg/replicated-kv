// Throughput benchmark for the scaling graph (assignment req 2).
//
// Workload: 90% GET / 10% PUT over a large uniform keyspace, so sharding
// actually parallelizes across storage nodes — a hot key would pin the whole
// load to one node and flatten the curve. Reads of not-yet-written keys
// return 404, which is a valid, full-cost response for throughput purposes.
//
// Env:
//   ROUTER   - host:port of the router (default localhost:8080)
//   VUS      - concurrent virtual users (default 150)
//   DURATION - measurement window (default 60s)
//   KEYSPACE - number of distinct keys (default 100000)
import http from 'k6/http';
import { check } from 'k6';

const ROUTER = __ENV.ROUTER || 'localhost:8080';
const KEYSPACE = Number(__ENV.KEYSPACE || 100000);

export const options = {
  vus: Number(__ENV.VUS || 150),
  duration: __ENV.DURATION || '60s',
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  // Cheapens the generator (status still checked), so a modest loadgen VM
  // never becomes the bottleneck; the server-side workload is unchanged.
  discardResponseBodies: true,
};

export default function () {
  const key = `k-${Math.floor(Math.random() * KEYSPACE)}`;
  let res;
  if (Math.random() < 0.1) {
    res = http.put(`http://${ROUTER}/kv/${key}`, JSON.stringify({ value: `v-${key}` }), {
      headers: { 'Content-Type': 'application/json' },
    });
    check(res, { 'put ok': (r) => r.status === 200 });
  } else {
    res = http.get(`http://${ROUTER}/kv/${key}`);
    check(res, { 'get ok': (r) => r.status === 200 || r.status === 404 });
  }
}
