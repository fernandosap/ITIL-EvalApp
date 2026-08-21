'use strict';

// Client-side error reporter. Backs the POST /api/client-errors
// endpoint. The endpoint exists so the browser can ship a small
// diagnostic payload (window error, unhandledrejection, failed
// <script> load) to the server when something goes wrong before
// the candidate can describe it.
//
// Why a separate module from lib/audit.js:
//   - lib/audit.js is admin-only. ADMIN_AUDIT_LOG.ACTOR is
//     NOT NULL, and the rows are exposed in the admin console
//     under the compliance/audit section. Client errors come
//     from anonymous candidates; the framing is different
//     (diagnostic telemetry, not compliance evidence) even
//     though we reuse the same physical table.
//   - Sanitization lives here, not in the route handler. The
//     route accepts the raw payload but does NOT call
//     tryWriteAdminAudit directly — it calls reportClientError
//     which validates + sanitizes + writes. That keeps the
//     safety-critical sanitization in one testable place.
//   - We want to keep the failure mode visible. If the HANA
//     write fails, the caller (browser) just gets {ok:true}
//     and the diagnostic is lost — but the server emits a
//     structured warn log. So a broken table is visible in
//     cf logs without breaking the user flow.
//
// Storage: we reuse ADMIN_AUDIT_LOG (same table). The row is
// distinguishable by ACTION = 'client_error'. This avoids a
// new table for what is structurally the same kind of
// "diagnostic event" record.
//
// Sanitization rules (in sanitizePayload below):
//   - All string fields pass through sanitizeString(), which:
//       - truncates to a max length
//       - strips JWTs (eyJ... patterns)
//       - strips 64-char hex (HMAC sigs / password hashes)
//       - strips 6-char [A-Z2-9] tokens (access codes) inside
//         prose, so a leaked message text doesn't accidentally
//         include one
//   - Stack traces are split into frames and only the first
//     MAX_STACK_FRAMES (3) are kept, sanitized.
//   - Question text, options, answers, and any field we don't
//     recognize are REJECTED. The schema is fixed (whitelisted
//     fields only), not arbitrary JSON.
//   - accessCode is REJECTED. The exam access code is a
//     credential and is never accepted as a field; we use
//     diagnosticSessionId (random UUID per browser tab)
//     for per-session correlation instead.

const hana = require('@sap/hana-client');
const { getPool, acquireConn, releaseConn } = require('../shared/db-pool.js');

let _config = null;
let _customDbFn = null;     // for tests
let _customLog = null;       // for tests
let _customHasTableFn = null;

const MAX_MESSAGE_LEN = 240;
const MAX_FILENAME_LEN = 200;
const MAX_STACK_LEN = 1200;
const MAX_LAST_ACTION_LEN = 80;
const MAX_USER_AGENT_LEN = 200;
const MAX_STACK_FRAMES = 3;
const ALLOWED_SCREENS = new Set([
  'loading', 'code-entry', 'consent', 'tech-check',
  'exam', 'submit-pending', 'results', 'admin-login',
  'admin', 'recovery', 'unknown'
]);
const ALLOWED_TYPES = new Set(['error', 'unhandledrejection', 'module_load', 'bootstrap_failure']);
const ACTION_NAME = 'client_error';

// Regex matchers for sanitization. We don't try to be
// exhaustive; we try to catch the common credential-shaped
// patterns so a leaked message text or stack doesn't
// accidentally include them.
//
// JWT: three base64url segments separated by dots, header
//   starts with "eyJ".
// 64-char hex: sha256 hex / password hashes / HMAC sigs.
// 6-char [A-Z2-9] with word boundaries: the access code
//   format. Defense-in-depth: even though the schema
//   no longer accepts accessCode as a field, an
//   accidental inclusion in a message or stack trace is
//   still redacted. The leading class includes URL/query
//   separators (=, ?, &) so codes embedded in URLs (e.g.
//   `?code=ABC2DE`) are caught. The word boundary prevents
//   eating the middle of longer words.
const REGEX_JWT = /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/g;
const REGEX_HEX_64 = /\b[A-Fa-f0-9]{64}\b/g;
const REGEX_ACCESS_CODE = new RegExp(
  '(^|[\\s.,;:(=?&/])([A-HJ-KM-NP-Z2-9]{6})(?=($|[\\s.,;:)\\]!]))',
  'g'
);

function _log(level, event, meta) {
  if (_customLog) return _customLog(level, event, meta);
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, event, ...(meta || {}) }));
}

function init(config) {
  _config = {
    hasDbConfig: Boolean(config.hasDbConfig),
    hanaHost: config.hanaHost,
    hanaPort: config.hanaPort || '443',
    hanaUser: config.hanaUser,
    hanaPassword: config.hanaPassword,
    hanaSchema: config.hanaSchema || 'ITIL_EXAM',
    hanaEncrypt: config.hanaEncrypt !== false,
    hanaSslValidateCertificate: config.hanaSslValidateCertificate === true
  };
}

function _setDepsForTests(deps) {
  if (!deps) return;
  if (typeof deps.withDb === 'function') _customDbFn = deps.withDb;
  if (typeof deps.hasClientErrorTable === 'function') _customHasTableFn = deps.hasClientErrorTable;
  if (typeof deps.log === 'function') _customLog = deps.log;
}

function _resetForTests() {
  _config = null;
  _customDbFn = null;
  _customLog = null;
  _customHasTableFn = null;
}

// Truncate + scrub. Always returns a string. Never throws.
function sanitizeString(s, maxLen) {
  if (typeof s !== 'string') return '';
  let out = s;
  out = out.replace(REGEX_JWT, '[REDACTED_JWT]');
  out = out.replace(REGEX_HEX_64, '[REDACTED_HEX]');
  // The access-code regex has a captured separator at index 1
  // and the code at index 2. We replace with sep + redacted.
  out = out.replace(REGEX_ACCESS_CODE, (m, sep) => `${sep}[REDACTED_CODE]`);
  if (out.length > maxLen) out = out.slice(0, maxLen) + '...[truncated]';
  return out;
}

function sanitizeFilename(s) {
  if (typeof s !== 'string') return '';
  // Strip query string and fragment — the URL might carry
  // access codes or tokens as query params (unlikely for our
  // /client/*.js routes, but defensive).
  let out = s.split('?')[0].split('#')[0];
  return sanitizeString(out, MAX_FILENAME_LEN);
}

function sanitizeStack(s) {
  if (typeof s !== 'string') return '';
  // Stack frames are typically one per line. The first line
  // is the error message ("Error: boom"); subsequent lines
  // are the frames. We keep the message + MAX_STACK_FRAMES
  // frames so the developer can still see WHERE it failed.
  const lines = s.split('\n');
  const head = lines.slice(0, 1 + MAX_STACK_FRAMES).join('\n');
  return sanitizeString(head, MAX_STACK_LEN);
}

function sanitizeDiagnosticSessionId(s) {
  if (typeof s !== 'string') return null;
  // Permissive but safe: short identifier, no whitespace,
  // no control characters. Matches what crypto.randomUUID()
  // emits (36 chars with hyphens) plus the legacy
  // 'dsid-...' / 'ua-...' fallbacks. We allow '/' so the
  // navigator.userAgent-derived fallback (`ua-Chrome/130`)
  // passes through; the storage location is a NCLOB so
  // any printable character is safe in HANA.
  if (s.length > 80) return null;
  if (!/^[A-Za-z0-9._\/-]+$/.test(s)) return null;
  return s;
}

// Whitelist schema. Anything not in this list is dropped
// silently. The browser is the only caller; if it sends
// extra fields, that's a sign the schema drifted; the
// server still records what it can.
//
// IMPORTANT: we deliberately do NOT accept an exam access
// code in the payload. The access code is a credential
// (anyone with it can sit the exam under another
// candidate's name) and it has no place in anonymous
// diagnostic telemetry. The browser uses a non-secret
// diagnosticSessionId (random UUID per tab) for
// correlation instead. We also strip any incoming
// `accessCode` field here as a defense-in-depth measure
// in case a future browser-side regression re-adds it.
function sanitizePayload(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return null;
  const type = ALLOWED_TYPES.has(input.type) ? String(input.type) : 'error';
  const screen = ALLOWED_SCREENS.has(input.screen) ? String(input.screen) : 'unknown';
  return {
    type,
    screen,
    // Non-secret per-tab correlation ID. Validated as
    // safe-for-storage: short identifier, no whitespace,
    // no path-y characters. Used by the admin dashboard
    // to group multiple errors from the same session.
    diagnosticSessionId: sanitizeDiagnosticSessionId(input.diagnosticSessionId),
    message: sanitizeString(input.message, MAX_MESSAGE_LEN),
    filename: sanitizeFilename(input.filename),
    line: Number.isFinite(input.line) ? Math.max(0, Math.min(1e7, Math.floor(input.line))) : 0,
    col: Number.isFinite(input.col) ? Math.max(0, Math.min(1e7, Math.floor(input.col))) : 0,
    stack: sanitizeStack(input.stack),
    lastAction: sanitizeString(input.lastAction, MAX_LAST_ACTION_LEN),
    userAgent: sanitizeString(input.userAgent, MAX_USER_AGENT_LEN),
    clientTs: typeof input.clientTs === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(input.clientTs)
      ? input.clientTs.slice(0, 40) : null
  };
}

function _dbConnect() {
  if (!_config || !_config.hasDbConfig) {
    throw new Error('HANA env vars are missing.');
  }
  const conn = hana.createConnection();
  conn.connect({
    serverNode: `${_config.hanaHost}:${_config.hanaPort}`,
    uid: _config.hanaUser,
    pwd: _config.hanaPassword,
    encrypt: _config.hanaEncrypt,
    sslValidateCertificate: _config.hanaSslValidateCertificate
  });
  return conn;
}

function _execQuery(conn, sql, params) {
  return new Promise((resolve, reject) => {
    conn.exec(sql, params || [], (err, rows) => {
      if (err) return reject(err);
      resolve(rows || []);
    });
  });
}

async function _withDb(fn) {
  if (_customDbFn) return _customDbFn(fn);
  const pool = getPool();
  let conn;
  let fromPool = false;
  if (pool) {
    try {
      conn = await acquireConn(pool);
      fromPool = true;
    } catch (err) {
      const msg = String((err && err.message) || err);
      if (!/maxConnectedOrPool/i.test(msg)) throw err;
      _log('warn', 'client_error_pool_exhausted_fallback', { message: msg });
      conn = _dbConnect();
    }
  } else {
    conn = _dbConnect();
  }
  try {
    await _execQuery(conn, `SET SCHEMA "${_config.hanaSchema}"`);
    return await fn(conn);
  } finally {
    if (fromPool) releaseConn(pool, conn);
    else {
      try { conn.disconnect(); } catch (_e) { /* ignore */ }
    }
  }
}

async function _hasTable(conn) {
  if (_customHasTableFn) return _customHasTableFn(conn);
  const rows = await _execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLES
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ADMIN_AUDIT_LOG'`,
    [String(_config.hanaSchema || '').toUpperCase()]
  );
  return Number((rows && rows[0] && rows[0].CNT) || 0) > 0;
}

// Write one sanitized client-error record. NEVER throws. The
// caller (the route handler) always responds {ok:true} to the
// browser regardless of what happens here; the diagnostic is
// best-effort. Failure is logged as a structured warn so a
// broken audit table is visible without breaking the user
// flow.
//
// Returns one of: 'ok' | 'no_table' | 'no_db' | 'failed'.
// On 'no_db' or 'no_table', the call is silently dropped —
// we don't queue it anywhere, because the volume could be
// unbounded in a real frontend regression and we don't want
// to fill up memory.
async function reportClientError({ payload, clientIp, appRevision }) {
  const sanitized = sanitizePayload(payload);
  if (!sanitized) return 'invalid';
  if (!_config || !_config.hasDbConfig) return 'no_db';
  try {
    const result = await _withDb(async (conn) => {
      const has = await _hasTable(conn);
      if (!has) return 'no_table';
      // Reuse ADMIN_AUDIT_LOG schema. ACTION = 'client_error'
      // is the discriminator. ACTOR = 'anonymous' because the
      // endpoint is unauthenticated. CLIENT_IP is the
      // candidate's IP. DETAILS_JSON holds the full sanitized
      // payload + the app revision at the time the report was
      // received (so we can correlate frontend vs backend
      // versions).
      const details = {
        ...sanitized,
        appRevision: String(appRevision || 'unknown').slice(0, 64),
        receivedAt: new Date().toISOString()
      };
      await _execQuery(
        conn,
        `INSERT INTO ADMIN_AUDIT_LOG (ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT)
         VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP)`,
        [
          ACTION_NAME,
          // TARGET_CODE is always NULL for client errors.
          // The exam access code is a credential and never
          // travels with diagnostic telemetry. Correlation
          // uses diagnosticSessionId in DETAILS_JSON instead.
          null,
          JSON.stringify(details),
          'anonymous',
          clientIp ? String(clientIp).slice(0, 120) : null
        ]
      );
      return 'ok';
    });
    if (result === 'no_table') {
      _log('warn', 'client_error_skipped_no_table', {});
      return 'no_table';
    }
    return result;
  } catch (err) {
    _log('warn', 'client_error_write_failed', {
      message: err && err.message ? err.message : String(err)
    });
    return 'failed';
  }
}

module.exports = {
  init,
  // Public sanitization (exported for unit tests + future
  // server-side use). Always returns a plain object; never
  // throws.
  sanitizePayload,
  sanitizeString,
  sanitizeFilename,
  sanitizeStack,
  sanitizeDiagnosticSessionId,
  reportClientError,
  // Constants (exported for tests + the route handler so
  // they agree on the schema).
  ALLOWED_TYPES,
  ALLOWED_SCREENS,
  ACTION_NAME,
  // Test hooks.
  _setDepsForTests,
  _resetForTests
};
