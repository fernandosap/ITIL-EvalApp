'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const runtime = require('../lib/runtime-hardening.js');
const { mapScopesToRole, roleFromClaims } = require('../shared/xsuaa.js');
const { readConnConfig } = require('../shared/db-pool.js');

function makeRes() {
  const listeners = new Map();
  return {
    statusCode: 200,
    headers: {},
    body: null,
    setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; },
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.emit('finish'); return this; },
    once(name, fn) {
      const list = listeners.get(name) || [];
      list.push(fn);
      listeners.set(name, list);
    },
    emit(name) {
      const list = listeners.get(name) || [];
      listeners.delete(name);
      for (const fn of list) fn();
    }
  };
}

function makeReq(path, method = 'GET', headers = {}, body = null) {
  return { path, method, headers, body, socket: { remoteAddress: '127.0.0.1' } };
}

function tick() {
  return new Promise((resolve) => setImmediate(resolve));
}

test.beforeEach(() => runtime.resetForTests());

test('secureMathRandom always returns a value in [0,1)', () => {
  for (let i = 0; i < 200; i++) {
    const value = runtime.secureMathRandom();
    assert.ok(value >= 0);
    assert.ok(value < 1);
  }
});

test('session/start has an independent global per-IP rate limit', () => {
  let nextCalls = 0;
  for (let i = 0; i < 10; i++) {
    const res = makeRes();
    runtime.sessionStartGuard(makeReq('/api/session/start', 'POST', {}, { code: `ABC2${String(i).padStart(2, '0')}` }), res, () => { nextCalls += 1; });
    assert.equal(res.statusCode, 200);
  }

  const sameIpDifferentCode = makeRes();
  runtime.sessionStartGuard(makeReq('/api/session/start', 'POST', {}, { code: 'XYZ234' }), sameIpDifferentCode, () => { nextCalls += 1; });
  assert.equal(sameIpDifferentCode.statusCode, 429);
  assert.equal(sameIpDifferentCode.body.error, 'too_many_attempts');
  assert.equal(nextCalls, 10);

  const otherIp = makeRes();
  runtime.sessionStartGuard(makeReq('/api/session/start', 'POST', { 'x-forwarded-for': '203.0.113.20' }, { code: 'XYZ234' }), otherIp, () => { nextCalls += 1; });
  assert.equal(otherIp.statusCode, 200);
  assert.equal(nextCalls, 11);
});

test('submit is locked while in-flight and cached after success for idempotent retry', () => {
  const headers = { 'x-exam-token': 'exam-token-1' };
  const firstRes = makeRes();
  let firstNext = 0;
  runtime.submitGuard(makeReq('/api/submit', 'POST', headers), firstRes, () => { firstNext += 1; });
  assert.equal(firstNext, 1);

  const secondRes = makeRes();
  runtime.submitGuard(makeReq('/api/submit', 'POST', headers), secondRes, () => assert.fail('concurrent submit should not continue'));
  assert.equal(secondRes.statusCode, 409);
  assert.equal(secondRes.body.error, 'submission_in_progress');

  const success = { ok: true, result: { score: 10, total: 10, pct: 100, pass: true } };
  firstRes.json(success);

  const retryRes = makeRes();
  runtime.submitGuard(makeReq('/api/submit', 'POST', headers), retryRes, () => assert.fail('cached retry should not continue'));
  assert.deepEqual(retryRes.body, success);
});

test('question-set writes are serialized in the single-instance runtime', async () => {
  const first = makeRes();
  await runtime.questionSetMutationGuard(makeReq('/api/admin/question-sets/1/clone', 'POST'), first, () => {});

  const second = makeRes();
  await runtime.questionSetMutationGuard(makeReq('/api/admin/question-sets/upload', 'POST'), second, () => assert.fail('second mutation should be blocked'));
  assert.equal(second.statusCode, 409);
  assert.equal(second.body.error, 'question_set_mutation_in_progress');

  first.emit('finish');
  const third = makeRes();
  let passed = false;
  await runtime.questionSetMutationGuard(makeReq('/api/admin/question-sets/upload', 'POST'), third, () => { passed = true; });
  assert.equal(passed, true);
});

test('revoke-all invalidates pre-existing opaque XSUAA sessions in runtime guard', async () => {
  runtime._state.revocationFetchedAt = Date.now();
  runtime._state.xsuaaSessions.set('old-session', Date.now() - 1000);
  const revokeRes = makeRes();
  let revokeNext = false;
  runtime.hardeningMiddleware(makeReq('/api/admin/sessions/revoke-all', 'POST', { cookie: 'xsuaa_session=old-session' }), revokeRes, () => { revokeNext = true; });
  await tick();
  assert.equal(revokeNext, true);
  revokeRes.statusCode = 200;
  revokeRes.emit('finish');

  const after = makeRes();
  let afterNext = false;
  runtime.hardeningMiddleware(makeReq('/api/admin/me', 'GET', { cookie: 'xsuaa_session=old-session' }), after, () => { afterNext = true; });
  await tick();
  assert.equal(afterNext, false);
  assert.equal(after.statusCode, 401);
  assert.equal(after.body.error, 'session_revoked');
});

test('XSUAA role mapping requires exact xsappname scope when configured', () => {
  assert.equal(mapScopesToRole(['app!t1.manager'], 'app!t1'), 'manager');
  assert.equal(mapScopesToRole(['other!t1.admin'], 'app!t1'), null);
  assert.equal(mapScopesToRole(['admin'], 'app!t1'), null);
  assert.equal(roleFromClaims({ scope: 'app!t1.reviewer' }, 'app!t1'), 'reviewer');
});

test('HANA pooled TLS certificate validation defaults to true', () => {
  const cfg = readConnConfig({ HANA_HOST: 'hana.example', HANA_PORT: '443', HANA_USER: 'u', HANA_PASSWORD: 'p' });
  assert.equal(cfg.encrypt, true);
  assert.equal(cfg.sslValidateCertificate, true);
});
