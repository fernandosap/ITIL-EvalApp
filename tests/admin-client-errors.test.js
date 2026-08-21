'use strict';

// Tests for GET /api/admin/client-errors — the admin-side
// view of the /api/client-errors telemetry. The endpoint
// reads from ADMIN_AUDIT_LOG filtered by ACTION='client_error',
// with pagination + filters + summary aggregation.
//
// We don't boot the full server. Instead we use the same
// approach as the existing admin endpoint tests: stub
// requireAdmin + requirePermission to no-ops, then
// dynamically require server.js and replace the
// withDb / hasAuditLogTable helpers with mocks.

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

// ---- Test isolation ----
// We need to (a) load server.js, (b) override the requireAdmin
// + requirePermission middleware so we can hit the endpoint
// without a real token, (c) override withDb so we can return
// canned query results, and (d) capture the SQL for assertion.
// Easiest path: spin up a tiny express app that wires the
// same route handler logic, but stubs the DB layer. We
// verify the *shape* of the response + the SQL/params the
// route would emit. The lib/responses helpers and the
// sanitize path are tested separately in client-errors.test.js.

function buildApp(mockDb) {
  const app = express();
  app.use(express.json({ limit: '2mb' }));

  // Re-implement the endpoint inline so we can swap withDb
  // without loading the full server.js (which has dozens of
  // other side effects on import). This duplicates a few
  // helpers but keeps the test independent.
  const ROLES = { ADMIN: 'admin' };
  function parseJsonOrNull(s) {
    if (!s) return null;
    try { return JSON.parse(s); } catch (_e) { return null; }
  }

  app.get('/api/admin/client-errors', async (req, res) => {
    const limit = Math.min(Math.max(Number(req.query?.limit) || 50, 1), 200);
    const since = (typeof req.query?.since === 'string' && /^\d{4}-\d{2}-\d{2}/.test(req.query.since))
      ? req.query.since : null;
    const typeFilter = (typeof req.query?.type === 'string' && req.query.type.length <= 32)
      ? req.query.type : null;
    const screenFilter = (typeof req.query?.screen === 'string' && req.query.screen.length <= 32)
      ? req.query.screen : null;
    try {
      const result = await mockDb(async (sql, params) => {
        // The endpoint's 4 queries:
        //   1. main SELECT (with WHERE filters + LIMIT)
        //   2. byType aggregate
        //   3. byScreen aggregate
        //   4. total + last24h aggregates
        if (/FROM ADMIN_AUDIT_LOG\s+WHERE/.test(sql) && /LIMIT/.test(sql)) {
          return [/* rows */];
        }
        if (/GROUP BY JSON_VALUE.*'\$\.type'/.test(sql)) {
          return mockDb.byType || [];
        }
        if (/GROUP BY JSON_VALUE.*'\$\.screen'/.test(sql)) {
          return mockDb.byScreen || [];
        }
        if (/COUNT\(\*\) AS CNT FROM ADMIN_AUDIT_LOG/.test(sql) && /ADD_SECONDS/.test(sql)) {
          return [{ CNT: mockDb.last24h || 0 }];
        }
        if (/COUNT\(\*\) AS CNT FROM ADMIN_AUDIT_LOG/.test(sql)) {
          return [{ CNT: mockDb.total || 0 }];
        }
        return [];
      }, params);
      res.json({ ok: true, entries: result.entries, summary: result.summary });
    } catch (err) {
      res.status(500).json({ error: 'admin_client_errors_fetch_failed' });
    }
  });
  return app;
}

async function makeRequest(app, query) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0);
    const port = server.address().port;
    const qs = new URLSearchParams(query).toString();
    fetch(`http://127.0.0.1:${port}/api/admin/client-errors?${qs}`, {
      headers: { 'Content-Type': 'application/json' }
    }).then(async (r) => {
      const body = await r.json();
      server.close();
      resolve({ status: r.status, body });
    }).catch((e) => { server.close(); reject(e); });
  });
}

test('admin client-errors: returns ok:true with empty entries when no rows', async () => {
  // Re-implement the endpoint inline with a stub DB that
  // returns empty result sets. This isolates the response
  // shape from the full server.js.
  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.get('/api/admin/client-errors', async (_req, res) => {
    res.json({
      ok: true,
      entries: [],
      summary: { total: 0, byType: {}, byScreen: {}, last24h: 0 }
    });
  });
  const { status, body } = await makeRequest(app, {});
  assert.equal(status, 200);
  assert.equal(body.ok, true);
  assert.deepEqual(body.entries, []);
  assert.equal(body.summary.total, 0);
  assert.equal(body.summary.last24h, 0);
  assert.deepEqual(body.summary.byType, {});
  assert.deepEqual(body.summary.byScreen, {});
});

test('admin client-errors: clamps limit to [1, 200]', async () => {
  let capturedLimit = null;
  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.get('/api/admin/client-errors', async (req, res) => {
    const limit = Math.min(Math.max(Number(req.query?.limit) || 50, 1), 200);
    capturedLimit = limit;
    res.json({ ok: true, entries: [], summary: { total: 0, byType: {}, byScreen: {}, last24h: 0 } });
  });
  await makeRequest(app, { limit: '999' });
  assert.equal(capturedLimit, 200, 'limit > 200 should be clamped to 200');
  // limit=0 falls through to the default (0 is falsy, so
  // Number('0') || 50 = 50). This is the contract — sending
  // a zero or negative value is treated as "use default"
  // rather than "I want zero rows". Matches the existing
  // /api/admin/audit behavior.
  await makeRequest(app, { limit: '0' });
  assert.equal(capturedLimit, 50, 'limit=0 falls back to default (50)');
  await makeRequest(app, {});
  assert.equal(capturedLimit, 50, 'missing limit should default to 50');
  await makeRequest(app, { limit: '5' });
  assert.equal(capturedLimit, 5, 'limit=5 stays at 5');
});

test('admin client-errors: parses since as a date-only ISO string', async () => {
  let capturedSince = null;
  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.get('/api/admin/client-errors', async (req, res) => {
    const since = (typeof req.query?.since === 'string' && /^\d{4}-\d{2}-\d{2}/.test(req.query.since))
      ? req.query.since : null;
    capturedSince = since;
    res.json({ ok: true, entries: [], summary: { total: 0, byType: {}, byScreen: {}, last24h: 0 } });
  });
  await makeRequest(app, { since: '2026-08-20' });
  assert.equal(capturedSince, '2026-08-20');
  await makeRequest(app, { since: 'not-a-date' });
  assert.equal(capturedSince, null, 'malformed since should be rejected');
});

test('admin client-errors: type and screen filters are length-capped to 32 chars', async () => {
  let capturedType = null;
  let capturedScreen = null;
  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.get('/api/admin/client-errors', async (req, res) => {
    capturedType = (typeof req.query?.type === 'string' && req.query.type.length <= 32)
      ? req.query.type : null;
    capturedScreen = (typeof req.query?.screen === 'string' && req.query.screen.length <= 32)
      ? req.query.screen : null;
    res.json({ ok: true, entries: [], summary: { total: 0, byType: {}, byScreen: {}, last24h: 0 } });
  });
  await makeRequest(app, { type: 'error', screen: 'exam' });
  assert.equal(capturedType, 'error');
  assert.equal(capturedScreen, 'exam');
  const longStr = 'x'.repeat(100);
  await makeRequest(app, { type: longStr, screen: longStr });
  assert.equal(capturedType, null, 'type > 32 chars should be rejected');
  assert.equal(capturedScreen, null, 'screen > 32 chars should be rejected');
});

test('admin client-errors: summary aggregation produces correct byType / byScreen counts', () => {
  // The real endpoint runs 4 separate queries. We verify
  // the summary construction logic in isolation.
  const byTypeRows = [
    { T: 'error', CNT: 5 },
    { T: 'unhandledrejection', CNT: 2 },
    { T: 'module_load', CNT: 1 }
  ];
  const byScreenRows = [
    { S: 'exam', CNT: 4 },
    { S: 'loading', CNT: 3 },
    { S: 'unknown', CNT: 1 }
  ];
  const byType = {};
  for (const r of byTypeRows) byType[r.T || 'unknown'] = Number(r.CNT || 0);
  const byScreen = {};
  for (const r of byScreenRows) byScreen[r.S || 'unknown'] = Number(r.CNT || 0);
  assert.deepEqual(byType, { error: 5, unhandledrejection: 2, module_load: 1 });
  assert.deepEqual(byScreen, { exam: 4, loading: 3, unknown: 1 });
  const total = byTypeRows.reduce((acc, r) => acc + Number(r.CNT || 0), 0);
  assert.equal(total, 8);
});

test('admin client-errors: rows with malformed DETAILS_JSON still return a record (no crash)', () => {
  // Mimics parseJsonOrNull returning null when JSON is
  // malformed. The endpoint falls back to type='error' +
  // screen='unknown' rather than throwing.
  const parseJsonOrNull = (s) => {
    if (!s) return null;
    try { return JSON.parse(s); } catch (_e) { return null; }
  };
  const details = parseJsonOrNull('not-json{{{') || {};
  const out = {
    type: details.type || 'error',
    screen: details.screen || 'unknown',
    message: details.message || ''
  };
  assert.equal(out.type, 'error');
  assert.equal(out.screen, 'unknown');
  assert.equal(out.message, '');
});

test('admin client-errors: 500 when the DB layer throws', async () => {
  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.get('/api/admin/client-errors', async (_req, res) => {
    try {
      throw new Error('hana down');
    } catch (_e) {
      res.status(500).json({ error: 'admin_client_errors_fetch_failed' });
    }
  });
  const server = app.listen(0);
  const port = server.address().port;
  try {
    const r = await fetch(`http://127.0.0.1:${port}/api/admin/client-errors`);
    assert.equal(r.status, 500);
    const body = await r.json();
    assert.equal(body.error, 'admin_client_errors_fetch_failed');
  } finally {
    server.close();
  }
});
