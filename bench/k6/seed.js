// Pre-fills the whole benchmark keyspace so throughput measurements are
// stationary: without seeding, GETs start as cheap 404s and gradually become
// full value responses as PUTs fill the store, and throughput drifts for the
// first minutes instead of measuring a fixed workload.
import http from 'k6/http';
import exec from 'k6/execution';

const ROUTER = __ENV.ROUTER || 'localhost:8080';
const KEYSPACE = Number(__ENV.KEYSPACE || 100000);

export const options = {
  scenarios: {
    seed: {
      executor: 'shared-iterations',
      vus: 100,
      iterations: KEYSPACE,
      maxDuration: '180s',
    },
  },
  discardResponseBodies: true,
};

export default function () {
  const i = exec.scenario.iterationInTest; // globally unique 0..KEYSPACE-1
  http.put(`http://${ROUTER}/kv/k-${i}`, JSON.stringify({ value: `v-${i}` }), {
    headers: { 'Content-Type': 'application/json' },
  });
}
