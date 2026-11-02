import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

export const transitionDuration = new Trend('state_transition_duration_ms');
export const conflictRate = new Rate('optimistic_concurrency_conflicts');
export const successfulTransitions = new Counter('successful_state_transitions');

export const options = {
  scenarios: {
    state_machine_stress: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '30s', target: 50 },  // Ramp-up to 50 concurrent transactions
        { duration: '1m', target: 200 },  // Stress at 200 concurrent transactions
        { duration: '30s', target: 0 },   // Ramp-down
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<150', 'p(99)<300'], // 95% of state updates under 150ms
    http_req_failed: ['rate<0.01'],                // Error rate < 1%
    'optimistic_concurrency_conflicts': ['rate<0.05'], // Max 5% race conflict aborts
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';

export default function () {
  const vuId = __VU;
  const iterId = __ITER;
  const sagaId = `018d9f1c-7201-7000-8000-${String(vuId).padStart(6, '0')}${String(iterId).padStart(6, '0')}`;
  const publicId = `sag_${String(vuId).padStart(13, '0')}${String(iterId).padStart(13, '0')}`;
  const tenantId = `tenant_${vuId % 10}`;

  const headers = {
    'Content-Type': 'application/json',
    'traceparent': '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
    'x-request-id': `req_${vuId}_${iterId}`,
    'x-correlation-id': `corr_${sagaId}`,
    'x-tenant-id': tenantId,
  };

  // 1. Initialize State
  const initPayload = JSON.stringify({
    id: sagaId,
    publicId: publicId,
    tenantId: tenantId,
    source: 'k6-load-runner',
    externalId: `ext_${sagaId}`,
    correlationId: `corr_${sagaId}`,
  });

  const initRes = http.post(`${BASE_URL}/api/v1/sagas`, initPayload, { headers });
  check(initRes, {
    'saga initialized (201 or 409)': (r) => r.status === 201 || r.status === 409,
  });

  // 2. State Transition: CREATED -> RUNNING
  const transitionPayload = JSON.stringify({
    tenantId: tenantId,
    sagaId: sagaId,
    event: 'ORCHESTRATOR_CLAIM',
    expectedVersion: 1,
    fencingToken: 1,
    reason: 'k6 load test worker claim',
  });

  const start = Date.now();
  const transitionRes = http.post(`${BASE_URL}/api/v1/sagas/transitions`, transitionPayload, { headers });
  transitionDuration.add(Date.now() - start);

  if (transitionRes.status === 200) {
    successfulTransitions.add(1);
    conflictRate.add(0);
  } else if (transitionRes.status === 409) {
    conflictRate.add(1);
  }

  check(transitionRes, {
    'transition status valid (200, 409)': (r) => r.status === 200 || r.status === 409,
  });

  sleep(0.05);
}
