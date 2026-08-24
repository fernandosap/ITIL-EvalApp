'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createAuthMiddleware } = require('../lib/middleware.js');

function makeDeps(overrides = {}) {
  return {
    tryXsuaaAuth: () => null,
    readXsuaaSession: async () => null,
    parseAdminToken: () => null,
    getAdminTokenNotBefore: async () => 0,
    withDb: async (fn) => fn({}),
    hasDbConfig: true,
    getXsuaaConfig: () => null,
    hasPermission: (role, perm) => role === 'admin' && perm === 'codes:read',
    log: () => {},
    ...overrides
  };
}

function makeReq(headers = {}) {
  return { headers, ip: '127.0.0.1' };
}

function makeRes() {
  const res = {
    _status: 0,
    _body: null,
    status(code) { this._status = code; return this; },
    json(body) { this._body = body; return this; }
  };
  return res;
}

test('createAuthMiddleware: throws when required deps are missing', () => {
  assert.throws(() => createAuthMiddleware({}), /tryXsuaaAuth is required/);
  assert.throws(
    () => createAuthMiddleware({ tryXsuaaAuth: () => null }),
    /parseAdminToken is required/
  );
});

test('requireAdmin: returns 401 when no auth is present and no hint', async () => {
  const m = createAuthMiddleware(makeDeps());
  const res = makeRes();
  let nextCalled = false;
  m.requireAdmin(makeReq(), res, () => { nextCalled = true; });
  await new Promise((r) => setImmediate(r));
  assert.equal(nextCalled, false);
  assert.equal(res._status, 401);
  assert.equal(res._body.error, 'unauthorized');
  assert.match(res._body.hint, /X-Admin-Token/);
});

test('requireAdmin: surfaces XSUAA hint when bound', async () => {
  const m = createAuthMiddleware(makeDeps({
    getXsuaaConfig: () => ({ xsappname: 'x' })
  }));
  const res = makeRes();
  let nextCalled = false;
  m.requireAdmin(makeReq(), res, () => { nextCalled = true; });
  await new Promise((r) => setImmediate(r));
  assert.equal(nextCalled, false);
  assert.equal(res._status, 401);
  assert.match(res._body.hint, /XSUAA/);
});

test('requireAdmin: passes through when XSUAA auth succeeds', () => {
  let nextCalled = false;
  const m = createAuthMiddleware(makeDeps({
    tryXsuaaAuth: () => ({ role: 'admin', sub: 'u1' })
  }));
  const req = makeReq();
  const res = makeRes();
  m.requireAdmin(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, true);
  assert.equal(req.adminRole, 'admin');
  assert.equal(req.adminSubject, 'u1');
  assert.equal(req.authMethod, 'xsuaa');
});

test('requireAdmin: restores shared xsuaa session from cookie when sync auth misses', async () => {
  let nextCalled = false;
  const m = createAuthMiddleware(makeDeps({
    tryXsuaaAuth: (req) => req.xsuaaSessionAuth
      ? { role: req.xsuaaSessionAuth.role, sub: req.xsuaaSessionAuth.sub }
      : null,
    readXsuaaSession: async (id) => id === 'abc123'
      ? { token: 'jwt', role: 'admin', sub: 'u2' }
      : null
  }));
  const req = makeReq({ cookie: 'xsuaa_session=abc123' });
  const res = makeRes();
  m.requireAdmin(req, res, () => { nextCalled = true; });
  await new Promise((r) => setImmediate(r));
  assert.equal(nextCalled, true);
  assert.equal(req.adminRole, 'admin');
  assert.equal(req.adminSubject, 'u2');
  assert.equal(req.authMethod, 'xsuaa');
  assert.equal(req.headers.authorization, 'Bearer jwt');
});

test('requireAdmin: legacy SHA-256 token path accepts a valid token', async () => {
  let nextCalled = false;
  const m = createAuthMiddleware(makeDeps({
    parseAdminToken: () => ({ role: 'admin', issuedAt: Date.now() })
  }));
  const req = makeReq({ 'x-admin-token': 'whatever' });
  const res = makeRes();
  m.requireAdmin(req, res, () => { nextCalled = true; });
  // async — wait one microtask + setImmediate
  await new Promise((r) => setImmediate(r));
  assert.equal(nextCalled, true);
  assert.equal(req.adminRole, 'admin');
  assert.equal(req.authMethod, 'token');
});

test('requireAdmin: 401 session_revoked when token issued before notBefore', async () => {
  const m = createAuthMiddleware(makeDeps({
    parseAdminToken: () => ({ role: 'admin', issuedAt: 1000 }),
    getAdminTokenNotBefore: async () => 5000
  }));
  const req = makeReq({ 'x-admin-token': 'x' });
  const res = makeRes();
  let nextCalled = false;
  m.requireAdmin(req, res, () => { nextCalled = true; });
  await new Promise((r) => setImmediate(r));
  assert.equal(nextCalled, false);
  assert.equal(res._status, 401);
  assert.equal(res._body.error, 'session_revoked');
});

test('requireAdmin: 500 admin_auth_failed when withDb throws and logs', async () => {
  const logs = [];
  const m = createAuthMiddleware(makeDeps({
    parseAdminToken: () => ({ role: 'admin', issuedAt: Date.now() }),
    withDb: async () => { throw new Error('hana down'); },
    log: (level, event, meta) => logs.push({ level, event, meta })
  }));
  const req = makeReq({ 'x-admin-token': 'x' });
  req.requestId = 'r1';
  const res = makeRes();
  let nextCalled = false;
  m.requireAdmin(req, res, () => { nextCalled = true; });
  await new Promise((r) => setImmediate(r));
  assert.equal(nextCalled, false);
  assert.equal(res._status, 500);
  assert.equal(res._body.error, 'admin_auth_failed');
  assert.equal(logs.length, 1);
  assert.equal(logs[0].event, 'admin_auth_failed');
  assert.equal(logs[0].meta.message, 'hana down');
});

test('requireAdmin: skips DB revocation when hasDbConfig is false', async () => {
  let withDbCalled = false;
  const m = createAuthMiddleware(makeDeps({
    hasDbConfig: false,
    parseAdminToken: () => ({ role: 'admin', issuedAt: Date.now() }),
    withDb: async () => { withDbCalled = true; return 0; }
  }));
  const req = makeReq({ 'x-admin-token': 'x' });
  const res = makeRes();
  let nextCalled = false;
  m.requireAdmin(req, res, () => { nextCalled = true; });
  await new Promise((r) => setImmediate(r));
  assert.equal(nextCalled, true);
  assert.equal(withDbCalled, false, 'withDb must not be called when hasDbConfig is false');
});

test('requireAdminRole: passes through when role matches', () => {
  const m = createAuthMiddleware(makeDeps());
  const req = { adminRole: 'admin' };
  const res = makeRes();
  let nextCalled = false;
  m.requireAdminRole('admin')(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, true);
});

test('requireAdminRole: 403 when role is not admin (admin-only path)', () => {
  const m = createAuthMiddleware(makeDeps());
  const req = { adminRole: 'manager' };
  const res = makeRes();
  let nextCalled = false;
  m.requireAdminRole('admin')(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, false);
  assert.equal(res._status, 403);
  assert.equal(res._body.error, 'role_required');
  assert.equal(res._body.requiredRole, 'admin');
});

test('requireAdminRole: passes through when role matches (e.g. manager path)', () => {
  // The footgun fix: previously requireAdminRole('manager') was a no-op
  // for any non-admin role. Now it actually enforces the role match.
  const m = createAuthMiddleware(makeDeps());
  const req = { adminRole: 'manager' };
  const res = makeRes();
  let nextCalled = false;
  m.requireAdminRole('manager')(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, true);
});

test('requireAdminRole: 403 when caller is admin but role required is manager', () => {
  const m = createAuthMiddleware(makeDeps());
  const req = { adminRole: 'admin' };
  const res = makeRes();
  let nextCalled = false;
  m.requireAdminRole('manager')(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, false);
  assert.equal(res._status, 403);
  assert.equal(res._body.error, 'role_required');
  assert.equal(res._body.requiredRole, 'manager');
});

test('requireAdminRole: 403 when adminRole is missing entirely', () => {
  const m = createAuthMiddleware(makeDeps());
  const req = {};
  const res = makeRes();
  let nextCalled = false;
  m.requireAdminRole('admin')(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, false);
  assert.equal(res._status, 403);
});

test('requirePermission: passes through when role has the permission', () => {
  const m = createAuthMiddleware(makeDeps({
    hasPermission: (role, perm) => role === 'manager' && perm === 'codes:read'
  }));
  const req = { adminRole: 'manager' };
  const res = makeRes();
  let nextCalled = false;
  m.requirePermission('codes:read')(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, true);
});

test('requirePermission: 403 forbidden when permission is missing', () => {
  const m = createAuthMiddleware(makeDeps({
    hasPermission: () => false
  }));
  const req = { adminRole: 'manager' };
  const res = makeRes();
  let nextCalled = false;
  m.requirePermission('codes:read')(req, res, () => { nextCalled = true; });
  assert.equal(nextCalled, false);
  assert.equal(res._status, 403);
  assert.equal(res._body.error, 'forbidden');
  assert.equal(res._body.permission, 'codes:read');
});
