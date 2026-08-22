'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const runtime = require('../lib/runtime-hardening.js');
const legacyLogin = require('../lib/core/legacy-login.js');
const sharedRateLimit = require('../lib/core/rate-limit-shared.js');
const authCore = require('../lib/core/auth.js');
const { mapScopesToRole, roleFromClaims } = require('../shared/xsuaa.js');
const { readConnConfig } = require('../shared/db-pool.js');
const { config } = require('../lib/core/db.js');
const { generateCode, CHARS } = require('../lib/core/access-codes.js');
const { normalizeUpload } = require('../lib/core/question-sets.js');
const { normalizeIncident, incidentHash } = require('../lib/core/proctor.js');

function makeRes() {
  const listeners = new Map();
  return {
    statusCode: 200,
    body: null,
    headers: {},
    setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; },
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.emit('finish'); return this; },
    once(name, fn) { const list = listeners.get(name) || []; list.push(fn); listeners.set(name, list); },
    emit(name) { const list = listeners.get(name) || []; listeners.delete(name); for (const fn of list) fn(); }
  };
}

function req(body = {}, ip = '127.0.0.1') {
  return { path: '/api/session/start', method: 'POST', body, headers: {}, ip, socket: { remoteAddress: ip } };
}

function restoreEnv(name, value) {
  if (value == null) delete process.env[name];
  else process.env[name] = value;
}

test.beforeEach(() => runtime.resetForTests());

test('session start throttle is global per IP, not bypassable by rotating codes', async () => {
  let passed = 0;
  for (let i = 0; i < 10; i += 1) {
    const res = makeRes();
    await runtime.sessionStartGuard(req({ code: `ABC2${String(i).padStart(2, '0')}` }), res, () => { passed += 1; });
  }
  const blocked = makeRes();
  await runtime.sessionStartGuard(req({ code: 'ZZZ999' }), blocked, () => { passed += 1; });
  assert.equal(passed, 10);
  assert.equal(blocked.statusCode, 429);
  assert.equal(blocked.body.error, 'too_many_attempts');
});

test('shared rate-limit bucket hashes sensitive keys before persistence', () => {
  const key = 'session-start:pair:127.0.0.1:ABC234';
  const hash = sharedRateLimit.keyHash(key);
  assert.match(hash, /^[a-f0-9]{64}$/);
  assert.notEqual(hash, key);
});

test('reviewer-only legacy password configuration still authenticates', () => {
  const names = ['ADMIN_HASH', 'MANAGER_HASH', 'REVIEWER_HASH', 'CONTENT_EDITOR_HASH'];
  const previous = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  try {
    delete process.env.ADMIN_HASH;
    delete process.env.MANAGER_HASH;
    process.env.REVIEWER_HASH = 'a'.repeat(64);
    delete process.env.CONTENT_EDITOR_HASH;
    legacyLogin.resetForTests();
    const res = makeRes();
    let fellThrough = false;
    legacyLogin.fallbackLoginGuard(
      { body: { hash: 'a'.repeat(64) }, headers: {}, ip: '127.0.0.1', socket: { remoteAddress: '127.0.0.1' } },
      res,
      () => { fellThrough = true; }
    );
    assert.equal(fellThrough, false);
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.ok, true);
    assert.equal(res.body.role, 'reviewer');
    const decoded = Buffer.from(res.body.token, 'base64url').toString('utf8').split(':');
    assert.equal(decoded[2], 'reviewer');
    assert.equal(decoded.length, 5);
  } finally {
    for (const name of names) restoreEnv(name, previous[name]);
  }
});

test('access code generator uses the approved alphabet and produces six characters', () => {
  const seen = new Set();
  for (let i = 0; i < 500; i += 1) {
    const code = generateCode();
    assert.match(code, /^[A-Z2-9]{6}$/);
    for (const c of code) assert.ok(CHARS.includes(c));
    seen.add(code);
  }
  assert.ok(seen.size > 490, 'unexpectedly high collision rate');
});

test('question upload validator rejects invalid answer keys and duplicate q_num', () => {
  const result = normalizeUpload([
    { qNum: 1, stem: 'One', opts: ['A', 'B'], correctIndices: [0], multi: false },
    { qNum: 1, stem: 'Two', opts: ['A', 'B'], correctIndices: [5], multi: false }
  ]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => e.includes('duplicate q_num')));
  assert.ok(result.errors.some((e) => e.includes('invalid correct_indices')));
});

test('proctor incident normalization is bounded and hashes deterministically', () => {
  const incident = normalizeIncident({ type: 'tab_switch', detail: 'x'.repeat(2000), time: '10:00:00 AM' });
  assert.equal(incident.type, 'tab_switch');
  assert.equal(incident.detail.length, 1000);
  assert.equal(incidentHash('ABC234', incident), incidentHash('ABC234', incident));
  assert.notEqual(incidentHash('ABC234', incident), incidentHash('XYZ234', incident));
});

test('JWT issued-at helper reads iat in milliseconds', () => {
  const payload = Buffer.from(JSON.stringify({ iat: 12345 })).toString('base64url');
  const token = `${Buffer.from('{}').toString('base64url')}.${payload}.sig`;
  assert.equal(authCore.jwtIssuedAt(token), 12345000);
});

test('SSO token encryption round-trips without storing plaintext', () => {
  const env = { HANA_PASSWORD: 'high-entropy-test-password', HANA_SCHEMA: 'ITIL_EXAM' };
  const token = 'header.payload.signature';
  const encrypted = authCore.encryptToken(token, env);
  assert.notEqual(encrypted.ciphertext, token);
  assert.equal(authCore.decryptToken({
    TOKEN_CIPHERTEXT: encrypted.ciphertext,
    TOKEN_IV: encrypted.iv,
    TOKEN_TAG: encrypted.tag
  }, env), token);
});

test('XSUAA role mapping requires exact xsappname scope', () => {
  assert.equal(mapScopesToRole(['app!t1.manager'], 'app!t1'), 'manager');
  assert.equal(mapScopesToRole(['other!t1.admin'], 'app!t1'), null);
  assert.equal(mapScopesToRole(['admin'], 'app!t1'), null);
  assert.equal(roleFromClaims({ scope: 'app!t1.reviewer' }, 'app!t1'), 'reviewer');
});

test('HANA direct and pooled connections validate TLS certificates by default', () => {
  const env = { HANA_HOST: 'hana.example', HANA_PORT: '443', HANA_USER: 'u', HANA_PASSWORD: 'p' };
  assert.equal(config(env).connection.sslValidateCertificate, true);
  assert.equal(readConnConfig(env).sslValidateCertificate, true);
});

test('durable submit uses INSERT rather than MERGE and transaction mode', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'lib', 'core', 'submit.js'), 'utf8');
  assert.match(source, /INSERT INTO EXAM_RESULTS/);
  assert.doesNotMatch(source, /MERGE INTO EXAM_RESULTS/);
  assert.match(source, /transaction:\s*true/);
  assert.match(source, /idempotentReplay/);
});

test('question-set mutations use CURRENT_IDENTITY_VALUE and transactions', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'lib', 'core', 'question-sets.js'), 'utf8');
  const dbSource = fs.readFileSync(path.join(__dirname, '..', 'lib', 'core', 'db.js'), 'utf8');
  assert.match(dbSource, /CURRENT_IDENTITY_VALUE\(\)/);
  assert.doesNotMatch(source, /SELECT MAX\(QUESTION_SET_ID\)/);
  assert.doesNotMatch(source, /SELECT MAX\(SECTION_ID\)/);
  assert.match(source, /transaction:\s*true/g);
});

test('distributed runtime controls and explicit service wiring are present', () => {
  const authSource = fs.readFileSync(path.join(__dirname, '..', 'lib', 'core', 'auth.js'), 'utf8');
  const proctorSource = fs.readFileSync(path.join(__dirname, '..', 'lib', 'core', 'proctor.js'), 'utf8');
  const runtimeSource = fs.readFileSync(path.join(__dirname, '..', 'lib', 'runtime-hardening.js'), 'utf8');
  const dbSource = fs.readFileSync(path.join(__dirname, '..', 'lib', 'core', 'db.js'), 'utf8');
  assert.match(authSource, /ADMIN_SSO_SESSIONS_V2/);
  assert.match(authSource, /TOKEN_CIPHERTEXT/);
  assert.match(authSource, /aes-256-gcm/);
  assert.match(proctorSource, /INSERT INTO EXAM_PROCTOR_INCIDENTS/);
  assert.match(runtimeSource, /\/api\/admin\/live-sessions/);
  assert.match(runtimeSource, /\/api\/admin\/proctor\/incidents\/:code/);
  assert.match(runtimeSource, /\/api\/admin\/question-sets\/:id\/readiness/);
  assert.match(dbSource, /APP_RATE_LIMITS/);
  assert.match(dbSource, /APP_MUTEX/);
  assert.doesNotMatch(runtimeSource, /require\.cache\[/);
});
