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
    once(name, fn) { listeners.set(name, fn); },
    emit(name) { const fn = listeners.get(name); if (fn) { listeners.delete(name); fn(); } }
  };
}

function makeReq(path, method = 'GET', headers = {}) {
  return { path, method, headers, socket: { remoteAddress: '127.0.0.1' } };
}

test.beforeEach(() => runtime.resetForTests());

test('secureMathRandom always returns a value in [0,1)', () => {
  for (let i = 0; i < 200; i++) {
    const value = runtime.secureMathRandom();
    assert.ok(value >= 0);
    assert.ok(value < 1);
  }
});

test('session/start is independently rate limited', () => {
  let nextCalls = 0;
  for (let i = 0; i < 10; i++) {
    const res = makeRes();
    runtime.hardeningMiddleware(makeReq('/api/session/start', 'POST'), res, () => { nextCalls += 1; });
    assert.equal(res.statusCode, 200);
  }
  const blocked = makeRes();
  runtime.hardeningMiddleware(makeReq('/api/session/start', 'POST'), blocked, () => { nextCalls += 1; });
  assert.equal(blocked.statusCode, 429);
  assert.equal(blocked.body.error, 'too_many_attempts');
  assert.equal(nextCalls, 10);
});

test('concurrent submit using same exam token is blocked until first response finishes', () => {
  const headers = { 'x-exam-token': 'exam-token-1' };
  const firstRes = makeRes();
  let firstNext = 0;
  runtime.hardeningMiddleware(makeReq('/api/submit', 'POST', headers), firstRes, () => { firstNext += 1; });
  assert.equal(firstNext, 1);

  const secondRes = makeRes();
  let secondNext = 0;
  runtime.hardeningMiddleware(makeReq('/api/submit', 'POST', headers), secondRes, () => { secondNext += 1; });
  assert.equal(secondRes.statusCode, 409);
  assert.equal(secondRes.body.error, 'submission_in_progress');
  assert.equal(secondNext, 0);

  firstRes.emit('finish');
  const thirdRes = makeRes();
  runtime.hardeningMiddleware(makeReq('/api/submit', 'POST', headers), thirdRes, () => { secondNext += 1; });
  assert.equal(secondNext, 1);
});

test('question-set writes are serialized in the single-instance runtime', () => {
  const first = makeRes();
  runtime.hardeningMiddleware(makeReq('/api/admin/question-sets/1/clone', 'POST'), first, () => {});

  const second = makeRes();
  runtime.hardeningMiddleware(makeReq('/api/admin/question-sets/upload', 'POST'), second, () => assert.fail('second mutation should be blocked'));
  assert.equal(second.statusCode, 409);
  assert.equal(second.body.error, 'question_set_mutation_in_progress');

  first.emit('finish');
  const third = makeRes();
  let passed = false;
  runtime.hardeningMiddleware(makeReq('/api/admin/question-sets/upload', 'POST'), third, () => { passed = true; });
  assert.equal(passed, true);
});

test('revoke-all invalidates pre-existing opaque XSUAA sessions in runtime guard', () => {
  runtime._state.xsuaaSessions.set('old-session', Date.now() - 1000);
  const revokeRes = makeRes();
  runtime.hardeningMiddleware(makeReq('/api/admin/sessions/revoke-all', 'POST', { cookie: 'xsuaa_session=old-session' }), revokeRes, () => {});
  revokeRes.statusCode = 200;
  revokeRes.emit('finish');

  const after = makeRes();
  runtime.hardeningMiddleware(makeReq('/api/admin/me', 'GET', { cookie: 'xsuaa_session=old-session' }), after, () => assert.fail('revoked session should not pass'));
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
