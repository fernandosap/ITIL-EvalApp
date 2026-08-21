'use strict';

// Tests for requireAdminWriteRate — the per-(role+IP)
// rate limit on state-changing admin requests, added
// in commit 1abae61. Defaults: 60 requests / 60s window.
//
// We exercise the middleware in isolation by wiring it
// to a stub auth that just sets req.adminRole.

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const { createAuthMiddleware } = require('../lib/middleware.js');
const rateLimit = require('../lib/rate-limit.js');

function buildApp({ max = 60, windowMs = 60000 } = {}) {
  rateLimit._resetForTests();
  const app = express();
  const adminAuth = createAuthMiddleware({
    tryXsuaaAuth: () => null,
    parseAdminToken: () => null,
    getAdminTokenNotBefore: () => 0,
    withDb: () => { throw new Error('not used in test'); },
    hasDbConfig: false,
    getXsuaaConfig: () => null,
    hasPermission: () => true,
    checkRateLimit: rateLimit.checkRateLimit,
    getClientIp: (req) => req.ip || '127.0.0.1',
    log: () => {}
  });
  // Stub auth: pretend the request is from an admin of
  // the role passed via the test-only header. We bypass
  // requireAdmin so we don't have to mint a real token.
  app.use((req, _res, next) => {
    req.adminRole = req.headers['x-test-role'] || 'admin';
    next();
  });
  app.post('/write', adminAuth.requireAdminWriteRate({ max, windowMs }), (_req, res) => {
    res.json({ ok: true });
  });
  return app;
}

async function makeRequest(app, opts = {}) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0);
    const port = server.address().port;
    const headers = { 'Content-Type': 'application/json' };
    if (opts.role) headers['X-Test-Role'] = opts.role;
    fetch(`http://127.0.0.1:${port}/write`, {
      method: 'POST',
      headers,
      body: opts.body || '{}'
    }).then(async (r) => {
      const respHeaders = {};
      r.headers.forEach((v, k) => { respHeaders[k.toLowerCase()] = v; });
      const body = await r.text();
      server.close();
      resolve({ status: r.status, headers: respHeaders, body });
    }).catch((e) => { server.close(); reject(e); });
  });
}

test('admin write rate: allows the first N requests under the limit', async () => {
  const app = buildApp({ max: 3, windowMs: 60000 });
  for (let i = 1; i <= 3; i += 1) {
    const r = await makeRequest(app);
    assert.equal(r.status, 200, `request ${i} of 3 should be allowed`);
  }
  rateLimit._resetForTests();
});

test('admin write rate: blocks the request that exceeds the limit with 429', async () => {
  const app = buildApp({ max: 2, windowMs: 60000 });
  await makeRequest(app);
  await makeRequest(app);
  const r = await makeRequest(app);
  assert.equal(r.status, 429, 'request 3 of 2 should be 429');
  const body = JSON.parse(r.body);
  assert.equal(body.error, 'too_many_writes');
  rateLimit._resetForTests();
});

test('admin write rate: 429 response includes Retry-After header', async () => {
  const app = buildApp({ max: 1, windowMs: 30000 });
  await makeRequest(app);
  const r = await makeRequest(app);
  assert.equal(r.status, 429);
  assert.ok(r.headers['retry-after'], 'Retry-After must be set on 429');
  assert.equal(Number(r.headers['retry-after']), 30,
    'Retry-After value should be the window in seconds');
  rateLimit._resetForTests();
});

test('admin write rate: different roles at the same IP share the bucket', async () => {
  // Intentional: the budget is meant to bound backend load,
  // not per-user throughput. Two admins at the same IP share
  // the 60/min budget.
  const app = buildApp({ max: 2, windowMs: 60000 });
  await makeRequest(app, { role: 'admin' });
  await makeRequest(app, { role: 'manager' });
  const r = await makeRequest(app, { role: 'admin' });
  assert.equal(r.status, 429,
    'third request from either role should be blocked (shared bucket)');
  rateLimit._resetForTests();
});

test('admin write rate: different IPs are independent buckets', async () => {
  rateLimit._resetForTests();
  const app = buildApp({ max: 1, windowMs: 60000 });
  // Express sets req.ip from the socket. To simulate two
  // different IPs, we override via X-Forwarded-For which
  // the production app honors. The test app uses req.ip
  // directly via getClientIp, so we spoof by listening
  // on a different interface (not portable). Easier: stub
  // getClientIp to return different values.
  const adminAuth = createAuthMiddleware({
    tryXsuaaAuth: () => null,
    parseAdminToken: () => null,
    getAdminTokenNotBefore: () => 0,
    withDb: () => { throw new Error('not used in test'); },
    hasDbConfig: false,
    getXsuaaConfig: () => null,
    hasPermission: () => true,
    checkRateLimit: rateLimit.checkRateLimit,
    getClientIp: (req) => req.headers['x-test-ip'] || '127.0.0.1',
    log: () => {}
  });
  const app2 = express();
  app2.use((req, _res, next) => { req.adminRole = 'admin'; next(); });
  app2.post('/write', adminAuth.requireAdminWriteRate({ max: 1, windowMs: 60000 }), (_req, res) => res.json({ ok: true }));
  // First IP uses its 1 quota.
  const r1 = await makeRequestWithHeaders(app2, { 'X-Test-Ip': '1.1.1.1' });
  assert.equal(r1.status, 200);
  // Second IP is independent — its 1 quota is still available.
  const r2 = await makeRequestWithHeaders(app2, { 'X-Test-Ip': '2.2.2.2' });
  assert.equal(r2.status, 200);
  // First IP's second request is blocked.
  const r3 = await makeRequestWithHeaders(app2, { 'X-Test-Ip': '1.1.1.1' });
  assert.equal(r3.status, 429);
  rateLimit._resetForTests();
});

async function makeRequestWithHeaders(app, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0);
    const port = server.address().port;
    fetch(`http://127.0.0.1:${port}/write`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...extraHeaders },
      body: '{}'
    }).then(async (r) => {
      const body = await r.text();
      server.close();
      resolve({ status: r.status, body });
    }).catch((e) => { server.close(); reject(e); });
  });
}

test('admin write rate: missing adminRole is keyed as "unknown" (does not crash)', async () => {
  const app = express();
  rateLimit._resetForTests();
  const adminAuth = createAuthMiddleware({
    tryXsuaaAuth: () => null,
    parseAdminToken: () => null,
    getAdminTokenNotBefore: () => 0,
    withDb: () => { throw new Error('not used'); },
    hasDbConfig: false,
    getXsuaaConfig: () => null,
    hasPermission: () => true,
    checkRateLimit: rateLimit.checkRateLimit,
    getClientIp: () => '127.0.0.1',
    log: () => {}
  });
  // Note: NO middleware that sets req.adminRole. The
  // rate-limit middleware should fall back to "unknown"
  // and still function.
  app.post('/write', adminAuth.requireAdminWriteRate({ max: 1, windowMs: 60000 }), (_req, res) => res.json({ ok: true }));
  const r1 = await makeRequestWithHeaders(app);
  assert.equal(r1.status, 200);
  const r2 = await makeRequestWithHeaders(app);
  assert.equal(r2.status, 429);
  rateLimit._resetForTests();
});
