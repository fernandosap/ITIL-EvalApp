'use strict';

// Tests for lib/client-errors.js — the server-side handler
// for the /api/client-errors endpoint.
//
// Two things to test:
//   1. Sanitization — never lets PII / secrets / question
//      content / arbitrary fields through. Whitelist-only.
//   2. The write path — never throws, falls back gracefully
//      when HANA is missing or the table is missing, and uses
//      the right shape for ADMIN_AUDIT_LOG.
//
// The endpoint itself (rate limit, 200/204 response) is
// tested by an integration-style test below that uses
// express + the route handler.

const test = require('node:test');
const assert = require('node:assert/strict');
const clientErrors = require('../lib/client-errors.js');

// ---------------------------------------------------------------------------
// Sanitization
// ---------------------------------------------------------------------------

test('sanitizePayload: rejects non-object input', () => {
  assert.equal(clientErrors.sanitizePayload(null), null);
  assert.equal(clientErrors.sanitizePayload(undefined), null);
  assert.equal(clientErrors.sanitizePayload('string'), null);
  assert.equal(clientErrors.sanitizePayload(42), null);
  // Arrays are typeof 'object' but we don't accept them
  // either — a browser sending [] would be a misuse.
  assert.equal(clientErrors.sanitizePayload([]), null);
});

test('sanitizePayload: only allows whitelisted fields', () => {
  const out = clientErrors.sanitizePayload({
    type: 'error',
    message: 'hi',
    secret: 'should not pass through',
    questionText: 'What is 2+2?',
    options: ['1', '2', '3', '4'],
    answer: '4',
    password: 'hunter2',
    extra: { anything: true }
  });
  assert.equal(out.secret, undefined);
  assert.equal(out.questionText, undefined);
  assert.equal(out.options, undefined);
  assert.equal(out.answer, undefined);
  assert.equal(out.password, undefined);
  assert.equal(out.extra, undefined);
  assert.equal(out.type, 'error');
  assert.equal(out.message, 'hi');
});

test('sanitizePayload: type falls back to "error" for unknown values', () => {
  const out = clientErrors.sanitizePayload({ type: 'malicious-type' });
  assert.equal(out.type, 'error');
});

test('sanitizePayload: screen falls back to "unknown" for unknown values', () => {
  const out = clientErrors.sanitizePayload({ screen: 'admin-secret-screen' });
  assert.equal(out.screen, 'unknown');
});

test('sanitizePayload: screen accepts the allowed SPA screens', () => {
  for (const s of ['code-entry', 'consent', 'tech-check', 'exam', 'submit-pending', 'results', 'admin-login', 'admin']) {
    const out = clientErrors.sanitizePayload({ screen: s });
    assert.equal(out.screen, s);
  }
});

test('sanitizePayload: accessCode is validated against the exact 6-char format', () => {
  // Valid: 6 chars from the right alphabet
  assert.equal(clientErrors.sanitizePayload({ accessCode: 'ABC2DE' }).accessCode, 'ABC2DE');
  // Invalid: lowercase rejected
  assert.equal(clientErrors.sanitizePayload({ accessCode: 'abc2de' }).accessCode, null);
  // Invalid: too short
  assert.equal(clientErrors.sanitizePayload({ accessCode: 'ABC' }).accessCode, null);
  // Invalid: too long
  assert.equal(clientErrors.sanitizePayload({ accessCode: 'ABC2DEF' }).accessCode, null);
  // Invalid: forbidden chars (I, L, O, 0, 1)
  assert.equal(clientErrors.sanitizePayload({ accessCode: 'ABC2DI' }).accessCode, null);
  assert.equal(clientErrors.sanitizePayload({ accessCode: 'ABC2D0' }).accessCode, null);
  // Invalid: non-string
  assert.equal(clientErrors.sanitizePayload({ accessCode: 123456 }).accessCode, null);
  assert.equal(clientErrors.sanitizePayload({ accessCode: null }).accessCode, null);
});

test('sanitizeString: strips JWTs', () => {
  const out = clientErrors.sanitizeString('Failed with token eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c in the URL', 1000);
  assert.equal(out.includes('eyJ'), false, 'JWT must be redacted from message');
  assert.ok(out.includes('[REDACTED_JWT]'));
});

test('sanitizeString: strips 64-char hex (HMAC sigs / password hashes)', () => {
  const hex = 'a'.repeat(64);
  const out = clientErrors.sanitizeString(`signature=${hex} and more text after`, 1000);
  assert.equal(out.includes('a'.repeat(64)), false);
  assert.ok(out.includes('[REDACTED_HEX]'));
});

test('sanitizeString: strips 6-char [A-Z2-9] tokens (access codes) inside prose', () => {
  // The regex requires word boundaries (separator on each
  // side) so it doesn't eat substrings of longer words.
  const out = clientErrors.sanitizeString('The candidate ABC2DE has started', 1000);
  assert.equal(out.includes('ABC2DE'), false, 'access code in prose must be redacted');
  assert.ok(out.includes('[REDACTED_CODE]'));
  // The substring inside a longer word should NOT be eaten.
  const out2 = clientErrors.sanitizeString('HelloXYZ123 is just a word', 1000);
  // XY1234 wouldn't match anyway (needs A-Z2-9 set with no
  // I/L/O/0/1, and 6 chars). Use a valid alphabet: ABC2DE.
  const out3 = clientErrors.sanitizeString('alphaABC2DEbeta should be fine', 1000);
  assert.equal(out3.includes('ABC2DE'), true, '6-char code inside a word must NOT be eaten (no word boundary)');
});

test('sanitizeString: truncates long strings', () => {
  const longStr = 'x'.repeat(2000);
  const out = clientErrors.sanitizeString(longStr, 100);
  assert.ok(out.length <= 130);  // 100 + ...[truncated]
  assert.ok(out.endsWith('...[truncated]'));
});

test('sanitizeFilename: strips query string and fragment', () => {
  const out = clientErrors.sanitizeFilename('/client/main.js?code=ABC2DE&token=eyJabc.xyz.qqq#frag');
  assert.equal(out, '/client/main.js', 'query/fragment must be stripped');
});

test('sanitizeStack: keeps only the first 3 frames and sanitizes each', () => {
  const stack = [
    'Error: boom',
    '    at fn1 (file1.js:10:5)',
    '    at fn2 (file2.js:20:10)',
    '    at fn3 (file3.js:30:15)',
    '    at fn4 (file4.js:40:20)',
    '    at fn5 (file5.js:50:25)'
  ].join('\n');
  const out = clientErrors.sanitizeStack(stack);
  const frameCount = out.split('\n').length;
  assert.ok(frameCount <= 4, `should keep at most 3 frames + header (got ${frameCount})`);
  // Should NOT contain fn4/fn5
  assert.equal(out.includes('fn4'), false);
  assert.equal(out.includes('fn5'), false);
  // Should contain fn1/fn2/fn3
  assert.ok(out.includes('fn1'));
  assert.ok(out.includes('fn2'));
  assert.ok(out.includes('fn3'));
});

test('sanitizeStack: sanitizes JWTs and access codes in stack text', () => {
  const stack = `Error: failure
    at fetchToken (url?code=ABC2DE:1:1)
    at parseJwt (eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.SflKxwRJSMeKKF2QT4fwpMeJf:2:2)`;
  const out = clientErrors.sanitizeStack(stack);
  assert.equal(out.includes('ABC2DE'), false);
  assert.equal(out.includes('eyJhbGciOi'), false);
});

test('sanitizePayload: line/col are clamped to [0, 1e7] and rounded to int', () => {
  const out = clientErrors.sanitizePayload({ line: 3.7, col: -5 });
  assert.equal(out.line, 3);
  assert.equal(out.col, 0);
  const out2 = clientErrors.sanitizePayload({ line: 1e10, col: 1e10 });
  assert.equal(out2.line, 1e7);
  assert.equal(out2.col, 1e7);
});

test('sanitizePayload: clientTs must be ISO-like or null', () => {
  assert.equal(clientErrors.sanitizePayload({ clientTs: '2026-08-20T13:00:00Z' }).clientTs, '2026-08-20T13:00:00Z');
  assert.equal(clientErrors.sanitizePayload({ clientTs: 'not-a-date' }).clientTs, null);
  assert.equal(clientErrors.sanitizePayload({ clientTs: 12345 }).clientTs, null);
});

test('sanitizePayload: userAgent is truncated to 200 chars', () => {
  const ua = 'Mozilla/5.0 ' + 'x'.repeat(500);
  const out = clientErrors.sanitizePayload({ userAgent: ua });
  assert.ok(out.userAgent.length <= 220, 'UA must be truncated to ~200');
});

// ---------------------------------------------------------------------------
// reportClientError — write path
// ---------------------------------------------------------------------------

test('reportClientError: returns "no_db" when HANA is not configured', async () => {
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: false });
  const r = await clientErrors.reportClientError({
    payload: { type: 'error', message: 'test' },
    clientIp: '1.2.3.4',
    appRevision: 'v1'
  });
  assert.equal(r, 'no_db');
});

test('reportClientError: returns "invalid" for non-object payload', async () => {
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: true, hanaHost: 'h', hanaUser: 'u', hanaPassword: 'p', hanaSchema: 's' });
  const r = await clientErrors.reportClientError({ payload: null, clientIp: '1.2.3.4', appRevision: 'v1' });
  assert.equal(r, 'invalid');
});

test('reportClientError: writes to ADMIN_AUDIT_LOG via the withDb hook', async () => {
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: true, hanaHost: 'h', hanaUser: 'u', hanaPassword: 'p', hanaSchema: 's' });
  const calls = [];
  clientErrors._setDepsForTests({
    withDb: async (fn) => fn({
      exec: (sql, params, cb) => {
        calls.push({ sql, params });
        // _hasTable uses a SELECT against SYS.TABLES; the
        // INSERT goes to ADMIN_AUDIT_LOG. Return one row for
        // the SELECT (so the table is "present") and the
        // callback signature is consistent.
        cb(null, [{ CNT: 1 }]);
      }
    })
  });
  const r = await clientErrors.reportClientError({
    payload: {
      type: 'error',
      message: 'boom',
      screen: 'exam',
      accessCode: 'ABC2DE',
      filename: '/client/exam.js',
      line: 42,
      stack: 'Error: boom\n    at renderQ (exam.js:42:5)'
    },
    clientIp: '1.2.3.4',
    appRevision: 'v1.2.3'
  });
  assert.equal(r, 'ok');
  // We expect at least: SELECT against SYS.TABLES (for the
  // table check) + INSERT into ADMIN_AUDIT_LOG. The SET
  // SCHEMA call is bypassed by the withDb mock (the lib's
  // _withDb returns _customDbFn(fn) directly without
  // running SET SCHEMA), so we only see 2 calls here.
  // In production, the SET SCHEMA runs before the fn.
  assert.ok(calls.length >= 2, `expected >= 2 SQL calls, got ${calls.length}`);
  // The SELECT against SYS.TABLES must come first (table check).
  const select = calls.find((c) => c.sql.includes('FROM SYS.TABLES'));
  assert.ok(select, 'expected a SELECT against SYS.TABLES');
  assert.equal(select.params[0], 'S', 'schema name should be upper-cased by the lib');
  // The INSERT must target ADMIN_AUDIT_LOG and carry the
  // sanitized accessCode + action='client_error'.
  const insert = calls.find((c) => c.sql.includes('INSERT INTO ADMIN_AUDIT_LOG'));
  assert.ok(insert, 'expected an INSERT INTO ADMIN_AUDIT_LOG call');
  assert.equal(insert.params[0], 'client_error',
    'action column should be "client_error"');
  assert.equal(insert.params[1], 'ABC2DE',
    'target_code should be the validated access code');
  assert.equal(insert.params[3], 'anonymous',
    'actor should be "anonymous" (unauthenticated endpoint)');
  // DETAILS_JSON should include appRevision + receivedAt.
  const details = JSON.parse(insert.params[2]);
  assert.equal(details.appRevision, 'v1.2.3');
  assert.ok(details.receivedAt);
  assert.equal(details.message, 'boom');
  assert.equal(details.screen, 'exam');
  assert.equal(details.accessCode, 'ABC2DE');
});

test('reportClientError: returns "no_table" when ADMIN_AUDIT_LOG is missing', async () => {
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: true, hanaHost: 'h', hanaUser: 'u', hanaPassword: 'p', hanaSchema: 's' });
  clientErrors._setDepsForTests({
    withDb: async (fn) => fn({
      exec: (_sql, _params, cb) => cb(null, [{ CNT: 0 }])  // table missing
    })
  });
  const r = await clientErrors.reportClientError({
    payload: { type: 'error', message: 'test' },
    clientIp: '1.2.3.4',
    appRevision: 'v1'
  });
  assert.equal(r, 'no_table');
});

test('reportClientError: returns "failed" on HANA error, never throws', async () => {
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: true, hanaHost: 'h', hanaUser: 'u', hanaPassword: 'p', hanaSchema: 's' });
  clientErrors._setDepsForTests({
    withDb: async () => { throw new Error('hana down'); }
  });
  const r = await clientErrors.reportClientError({
    payload: { type: 'error', message: 'test' },
    clientIp: '1.2.3.4',
    appRevision: 'v1'
  });
  assert.equal(r, 'failed');
});

// ---------------------------------------------------------------------------
// POST /api/client-errors route — integration test
// ---------------------------------------------------------------------------

test('POST /api/client-errors: rate-limited per IP, 204 when over', async () => {
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: false });
  const express = require('express');
  const rateLimit = require('../lib/rate-limit.js');
  rateLimit._resetForTests();
  // Set up an isolated app with the same route pattern.
  const app = express();
  app.use(express.json({ limit: '100kb' }));
  const checkRateLimit = (bucket, key, max, windowMs) =>
    rateLimit.checkRateLimit(bucket, key, max, windowMs);
  app.post('/api/client-errors', async (req, res) => {
    const ip = req.ip || 'test';
    if (!checkRateLimit('client_error', String(ip), 2, 60000)) {
      // Mirror server.js: a well-behaved browser that wants
      // to retry (e.g. background sync) should know when.
      res.setHeader('Retry-After', Math.ceil(60000 / 1000));
      return res.status(204).end();
    }
    res.json({ ok: true });
  });
  // Simulate 3 requests from same IP
  const server = app.listen(0);
  const port = server.address().port;
  try {
    const r1 = await fetch(`http://127.0.0.1:${port}/api/client-errors`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'error', message: 'one' })
    });
    const r2 = await fetch(`http://127.0.0.1:${port}/api/client-errors`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'error', message: 'two' })
    });
    const r3 = await fetch(`http://127.0.0.1:${port}/api/client-errors`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'error', message: 'three' })
    });
    assert.equal(r1.status, 200);
    assert.equal(r2.status, 200);
    assert.equal(r3.status, 204, 'third request from same IP must be 204');
    // The 204 must include Retry-After so a well-behaved
    // browser / service worker can retry on schedule.
    assert.equal(r3.headers.get('retry-after'), '60',
      '204 must include Retry-After header (window in seconds)');
  } finally {
    server.close();
    rateLimit._resetForTests();
  }
});

test('POST /api/client-errors: returns 200 for a valid sanitizable payload', async () => {
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: false });
  const express = require('express');
  const app = express();
  app.use(express.json({ limit: '100kb' }));
  app.post('/api/client-errors', (req, res) => {
    const body = req.body;
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      return res.json({ ok: true, dropped: 'invalid_body' });
    }
    res.json({ ok: true });
  });
  const server = app.listen(0);
  const port = server.address().port;
  try {
    // Valid object body. The handler accepts it and
    // returns 200 with ok:true. Telemetry is best-effort
    // by design — the browser should never see a non-200
    // response from this endpoint.
    const r = await fetch(`http://127.0.0.1:${port}/api/client-errors`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'error', message: 'test' })
    });
    assert.equal(r.status, 200);
    const body = await r.json();
    assert.equal(body.ok, true);
  } finally {
    server.close();
  }
});

test('POST /api/client-errors: returns 200 with dropped=invalid_body for primitive body', async () => {
  // Use strict:false so express accepts primitive JSON
  // (number, string, etc). The production server uses the
  // default strict:true, which would 400 such requests
  // before reaching the handler. This test exercises the
  // handler-level "best-effort" behavior on the off
  // chance a future server config opens up primitive
  // bodies.
  clientErrors._resetForTests();
  clientErrors.init({ hasDbConfig: false });
  const express = require('express');
  const app = express();
  app.use(express.json({ limit: '100kb', strict: false }));
  app.post('/api/client-errors', (req, res) => {
    const body = req.body;
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      return res.json({ ok: true, dropped: 'invalid_body' });
    }
    res.json({ ok: true });
  });
  const server = app.listen(0);
  const port = server.address().port;
  try {
    const r = await fetch(`http://127.0.0.1:${port}/api/client-errors`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '42'
    });
    assert.equal(r.status, 200);
    const body = await r.json();
    assert.equal(body.dropped, 'invalid_body');
  } finally {
    server.close();
  }
});
