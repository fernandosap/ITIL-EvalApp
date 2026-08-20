/* eslint-disable no-console */
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const hana = require('@sap/hana-client');
const { version: APP_VERSION } = require('./package.json');

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  for (const rawLine of lines) {
    const line = String(rawLine || '').trim();
    if (!line || line.startsWith('#')) continue;
    const eqIdx = line.indexOf('=');
    if (eqIdx <= 0) continue;
    const key = line.slice(0, eqIdx).trim();
    if (!key || process.env[key] != null) continue;
    let value = line.slice(eqIdx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"'))
      || (value.startsWith('\'') && value.endsWith('\''))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

loadDotEnv(path.join(__dirname, '.env'));

const {
  normalizeExamTitle,
  hasPermission,
  ROLES,
  ROLE_LIST,
  isValidRole,
  CODE_STATUS,
  CODE_STATUS_LIST,
  QUESTION_SET_LIFECYCLE,
  EXAM_MODE,
  AUDIT_ACTION
} = require('./shared/constants.js');
const {
  makePRNG,
  seededShuffle,
  buildOrdering,
  pickQuestionsForSession,
  gradeExamFromSession
} = require('./shared/scoring.js');
const {
  getXsuaaConfig,
  verifyXsuaaJwt,
  roleFromClaims,
  buildAuthorizeUrl,
  exchangeCodeForToken,
  generateState,
  parseCookieHeader
} = require('./shared/xsuaa.js');
const {
  getPool,
  acquireConn,
  releaseConn
} = require('./shared/db-pool.js');
const audit = require('./lib/audit.js');
const rateLimit = require('./lib/rate-limit.js');
const { createAuthMiddleware } = require('./lib/middleware.js');
const sweeper = require('./lib/sweeper.js');
const {
  toCsvCell,
  toCsvRow,
  parseJsonOrNull,
  parseAnthropicText,
  buildSignedEnvelope,
  verifySignedEnvelope,
  jsonError,
  jsonOk
} = require('./lib/responses.js');
const signing = require('./lib/signing-context.js');

const app = express();
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;

const HANA_HOST = process.env.HANA_HOST;
const HANA_PORT = process.env.HANA_PORT || '443';
const HANA_USER = process.env.HANA_USER;
const HANA_PASSWORD = process.env.HANA_PASSWORD;
const HANA_SCHEMA = process.env.HANA_SCHEMA || 'ITIL_EXAM';
const HANA_ENCRYPT = String(process.env.HANA_ENCRYPT || 'true').toLowerCase() === 'true';
const HANA_SSL_VALIDATE_CERTIFICATE =
  String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'true').toLowerCase() === 'true';
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514';
const ANTHROPIC_VERSION = process.env.ANTHROPIC_VERSION || '2023-06-01';
const ADMIN_HASH = (process.env.ADMIN_HASH || '').trim().toLowerCase();
const MANAGER_HASH = (process.env.MANAGER_HASH || '').trim().toLowerCase();
const REVIEWER_HASH = (process.env.REVIEWER_HASH || '').trim().toLowerCase();
const CONTENT_EDITOR_HASH = (process.env.CONTENT_EDITOR_HASH || '').trim().toLowerCase();
const EXAM_NAME = process.env.EXAM_NAME || 'Academy Exam App';
const EXAM_DURATION_SECS = Number(process.env.EXAM_DURATION_SECS || 45 * 60);
const EXAM_PASS_PCT = Number(process.env.EXAM_PASS_PCT || 80);
const EXAM_ACTIVE = String(process.env.EXAM_ACTIVE || 'true').toLowerCase() !== 'false';
const PROCTOR_ENABLED = String(process.env.PROCTOR_ENABLED || 'true').toLowerCase() !== 'false';
const APP_REVISION = process.env.APP_REVISION || 'dev';
const APP_DEPLOYED_AT = process.env.APP_DEPLOYED_AT || new Date().toISOString();
const STALE_SESSION_MINUTES = Math.max(5, Number(process.env.STALE_SESSION_MINUTES || 30));
const APP_SETTING_EXAMS_ENABLED = 'EXAMS_ENABLED';
const APP_SETTING_ADMIN_TOKEN_NOT_BEFORE = 'ADMIN_TOKEN_NOT_BEFORE';
const AUTO_CLEAR_STALE_SESSIONS = String(process.env.AUTO_CLEAR_STALE_SESSIONS || 'true').toLowerCase() !== 'false';
const STALE_SESSION_SWEEP_MINUTES = Math.max(5, Number(process.env.STALE_SESSION_SWEEP_MINUTES || 10));
const STARTUP_STRICT = String(process.env.STARTUP_STRICT || 'false').toLowerCase() === 'true';

const HAS_DB_CONFIG = Boolean(HANA_HOST && HANA_USER && HANA_PASSWORD && HANA_SCHEMA);
const INDEX_PATH = path.join(__dirname, 'index.html');
const CLIENT_APP_PATH = path.join(__dirname, 'client-app.js'); // legacy shim, no longer used

// Wire lib/audit.js with the current HANA config. This must run before
// any tryWriteAdminAudit() call (which happens in route handlers).
audit.init({
  hasDbConfig: HAS_DB_CONFIG,
  hanaHost: HANA_HOST,
  hanaPort: HANA_PORT,
  hanaUser: HANA_USER,
  hanaPassword: HANA_PASSWORD,
  hanaSchema: HANA_SCHEMA,
  hanaEncrypt: HANA_ENCRYPT,
  hanaSslValidateCertificate: HANA_SSL_VALIDATE_CERTIFICATE
});

const FAVICON_PATH = path.join(__dirname, 'favicon.svg');

const EXAM_TTL_MS = 90 * 60 * 1000;
const ADMIN_TTL_MS = 8 * 60 * 60 * 1000;
const VALIDATE_MAX = 10;
const VALIDATE_WINDOW = 10 * 60 * 1000;
const ADMIN_LOGIN_MAX = 8;
const ADMIN_LOGIN_WINDOW = 15 * 60 * 1000;
const ADMIN_LOGIN_HASH_MAX = 20;       // defense vs password spray across IPs
const ADMIN_LOGIN_HASH_WINDOW = 15 * 60 * 1000;
const PROCTOR_MAX = 90;
const PROCTOR_WINDOW = 60 * 1000;
const _questionSetCache = new Map();
const _runtimeState = {
  adminTokenNotBefore: 0,
  adminTokenNotBeforeFetchedAt: 0,
  staleSessionSweepTimer: null,
  // Sweeper observability: a "tick" is one execution of the scheduled
  // cleanup. Without these fields, a silent crash of the sweep (e.g. HANA
  // unreachable) would be invisible — we only logged when something was
  // cleared. Now we log every tick and expose status via an admin endpoint.
  sweeperEnabled: false,
  sweeperStartedAt: 0,
  sweeperTickCount: 0,
  sweeperLastTickAt: 0,
  sweeperLastDurationMs: 0,
  sweeperLastCleared: [],
  sweeperLastError: null,
  sweeperTotalCleared: 0
};
const _metrics = {
  startedAt: Date.now(),
  requestsTotal: 0,
  requestsByRoute: new Map(),
  requestsByStatus: new Map(),
  slowRequests: 0,
  dbQueries: 0,
  dbSlowQueries: 0,
  dbErrors: 0,
  loginFailures: 0
};
const SLOW_QUERY_MS = Math.max(100, Number(process.env.SLOW_QUERY_MS || 400));
const SLOW_REQUEST_MS = Math.max(100, Number(process.env.SLOW_REQUEST_MS || 1200));

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: false }));
app.use((req, res, next) => {
  const requestId = req.headers['x-request-id']
    ? String(req.headers['x-request-id']).trim().slice(0, 120)
    : crypto.randomUUID();
  req.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);
  const startedAt = Date.now();
  _metrics.requestsTotal += 1;
  res.on('finish', () => {
    const routeKey = `${req.method} ${req.route?.path || req.path || 'unknown'}`;
    _metrics.requestsByRoute.set(routeKey, Number(_metrics.requestsByRoute.get(routeKey) || 0) + 1);
    _metrics.requestsByStatus.set(String(res.statusCode), Number(_metrics.requestsByStatus.get(String(res.statusCode)) || 0) + 1);
    const durationMs = Date.now() - startedAt;
    if (durationMs >= SLOW_REQUEST_MS) {
      _metrics.slowRequests += 1;
      appLog('warn', 'slow_request', { requestId, method: req.method, path: req.originalUrl, status: res.statusCode, durationMs });
    }
  });
  next();
});

function dbConnect() {
  if (!HAS_DB_CONFIG) throw new Error('HANA env vars are missing.');
  const conn = hana.createConnection();
  conn.connect({
    serverNode: `${HANA_HOST}:${HANA_PORT}`,
    uid: HANA_USER,
    pwd: HANA_PASSWORD,
    encrypt: HANA_ENCRYPT,
    sslValidateCertificate: HANA_SSL_VALIDATE_CERTIFICATE
  });
  return conn;
}

function execQuery(conn, sql, params = []) {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now();
    conn.exec(sql, params, (err, rows) => {
      const durationMs = Date.now() - startedAt;
      _metrics.dbQueries += 1;
      if (durationMs >= SLOW_QUERY_MS) {
        _metrics.dbSlowQueries += 1;
        appLog('warn', 'slow_query', { durationMs, sql: String(sql).slice(0, 220) });
      }
      if (err) {
        _metrics.dbErrors += 1;
        reject(err);
      } else resolve(rows || []);
    });
  });
}

function closeConn(conn) {
  return new Promise((resolve) => {
    try {
      conn.disconnect();
    } catch (_e) {
      // ignore
    }
    resolve();
  });
}

async function withDb(fn) {
  // Opt-in pool: if HANA_POOL_SIZE > 0, acquire from pool. Otherwise,
  // open/close a fresh connection per call (original behavior).
  // getPool() is SYNC; acquireConn() is ASYNC (uses the callback form
  // so the pool queues requests when all slots are in use).
  // Fallback: if the pool is exhausted (e.g. burst load), open a fresh
  // non-pooled connection instead of failing.
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
      appLog('warn', 'pool_exhausted_fallback', { message: msg });
      conn = dbConnect();
    }
  } else {
    conn = dbConnect();
  }
  try {
    await execQuery(conn, `SET SCHEMA "${HANA_SCHEMA}"`);
    return await fn(conn);
  } finally {
    if (fromPool) releaseConn(pool, conn);
    else closeConn(conn);
  }
}

function appLog(level, event, meta = {}) {
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, event, ...meta }));
}

// Legacy signing secret — derived from environment values. Kept so
// historical signed result summaries (signed before the versioned
// `RESULT_SIGNING_KEY` was introduced) still verify. New envelopes
// use the versioned `getSigningContext()` (lib/signing-context.js)
// instead. The legacy secret itself now lives in
// `lib/signing-context.js#loadLegacySecretFromEnv` and respects
// `RESULT_SIGNING_LEGACY_KEY` if set.

// Versioned signing key map.
//
// Env vars:
//   RESULT_SIGNING_KEY_ID         — stable identifier for the active key
//                                   (e.g. "v2", "prod-2026-08"). 1-64 chars,
//                                   [A-Za-z0-9._-] only.
//   RESULT_SIGNING_KEY            — the active secret (32+ chars).
//   RESULT_SIGNING_KEY_PREVIOUS_ID  — stable identifier for the previous
//                                     key (optional, for rotation).
//   RESULT_SIGNING_KEY_PREVIOUS     — the previous secret (optional).
//   RESULT_SIGNING_LEGACY_KEY       — optional. If set, freezes the
//                                     secret used for the `legacy`
//                                     kid so historical envelopes
//                                     keep verifying even after
//                                     HANA_PASSWORD / role hashes /
//                                     APP_REVISION change. Without
//                                     this, the legacy secret is
//                                     derived from those env vars
//                                     at every boot, and changes
//                                     to any of them silently break
//                                     verification of old envelopes.
//
// The actual keymap + validation + sign/verify helpers live in
// lib/signing-context.js. We re-export the few functions this
// file's startup / metrics paths need (parseSigningConfig,
// validateSigningConfig, isOperatorSigningConfig) and delegate
// the runtime path (signPayload, buildSignedEnvelopeLocal) to
// the cached context.

const SIGNING_KID_RESERVED = 'legacy';

// Pure helpers re-exported from lib/signing-context.js so this
// file's startup paths can keep using the same names.
const parseSigningConfig = signing.parseSigningConfig;
const validateSigningConfig = signing.validateSigningConfig;
const isOperatorSigningConfig = signing.isOperatorSigningConfig;

// Runtime: builds the SigningContext once per env change and
// reuses it. The context exposes sign/verify that read from the
// cached keyMap. This replaces the old getSigningKeyMap() that was
// called on every sign/verify (which meant re-parsing env vars
// and re-computing the derived secret on every request).
function getSigningKeyMap() {
  return signing.getSigningContext().keyMap;
}

// Runtime: sign a raw payload (HMAC of payload only — used for
// legacy "just-the-payload" signing paths and ad-hoc signatures).
// Delegates to the cached context so the operator's warnings fire
// exactly once, not per request.
function signPayload(payload) {
  return signing.getSigningContext().sign(payload).signature;
}

// Runtime: build a v2 signed envelope. Delegates to the cached
// context.
function buildSignedEnvelopeLocal(payload) {
  return signing.getSigningContext().sign(payload);
}

function getMetricsSnapshot() {
  return {
    uptimeSecs: Math.round((Date.now() - _metrics.startedAt) / 1000),
    requestsTotal: _metrics.requestsTotal,
    slowRequests: _metrics.slowRequests,
    dbQueries: _metrics.dbQueries,
    dbSlowQueries: _metrics.dbSlowQueries,
    dbErrors: _metrics.dbErrors,
    loginFailures: _metrics.loginFailures,
    requestsByStatus: Object.fromEntries(_metrics.requestsByStatus.entries()),
    topRoutes: [..._metrics.requestsByRoute.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12)
      .map(([route, count]) => ({ route, count })),
    // Audit-write visibility — surfaces a quietly broken audit log
    // (e.g. table dropped, HANA outage) without grepping logs.
    audit: audit.getMetrics()
  };
}

let _hasNotesColumn = null;
let _hasDeletedAtColumn = null;
let _hasQuestionSetModeColumns = null;
let _hasAuditLogTable = null;
let _hasQuestionSetVersionColumns = null;
async function hasNotesColumn(conn) {
  if (_hasNotesColumn !== null) return _hasNotesColumn;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ACCESS_CODES'
        AND COLUMN_NAME = 'NOTES'`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasNotesColumn = Number(rows?.[0]?.CNT || 0) > 0;
  return _hasNotesColumn;
}

async function hasDeletedAtColumn(conn) {
  if (_hasDeletedAtColumn !== null) return _hasDeletedAtColumn;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ACCESS_CODES'
        AND COLUMN_NAME = 'DELETED_AT'`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasDeletedAtColumn = Number(rows?.[0]?.CNT || 0) > 0;
  return _hasDeletedAtColumn;
}

async function hasQuestionSetModeColumns(conn) {
  if (_hasQuestionSetModeColumns !== null) return _hasQuestionSetModeColumns;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'QUESTION_SETS'
        AND COLUMN_NAME IN ('EXAM_MODE', 'SHOW_CORRECT_ANSWERS', 'COUNTS_TOWARD_RESULTS')`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasQuestionSetModeColumns = Number(rows?.[0]?.CNT || 0) === 3;
  return _hasQuestionSetModeColumns;
}

async function hasAuditLogTable(conn) {
  if (_hasAuditLogTable !== null) return _hasAuditLogTable;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLES
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ADMIN_AUDIT_LOG'`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasAuditLogTable = Number(rows?.[0]?.CNT || 0) > 0;
  return _hasAuditLogTable;
}

async function hasQuestionSetVersionColumns(conn) {
  if (_hasQuestionSetVersionColumns !== null) return _hasQuestionSetVersionColumns;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'QUESTION_SETS'
        AND COLUMN_NAME IN ('VERSION_GROUP_ID', 'VERSION_NUMBER', 'LIFECYCLE_STATUS', 'PARENT_QUESTION_SET_ID', 'IMPORT_SOURCE')`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasQuestionSetVersionColumns = Number(rows?.[0]?.CNT || 0) === 5;
  return _hasQuestionSetVersionColumns;
}

function sha256(str) {
  return crypto.createHash('sha256').update(String(str)).digest('hex');
}

function isSha256Hex(value) {
  return /^[a-f0-9]{64}$/i.test(String(value || '').trim());
}

function startupErrors() {
  const errors = [];
  if (!HAS_DB_CONFIG) errors.push('HANA connection env vars are incomplete.');
  if (ADMIN_HASH && !isSha256Hex(ADMIN_HASH)) errors.push('ADMIN_HASH is not a 64-char SHA-256 hex string.');
  if (MANAGER_HASH && !isSha256Hex(MANAGER_HASH)) errors.push('MANAGER_HASH is not a 64-char SHA-256 hex string.');
  if (REVIEWER_HASH && !isSha256Hex(REVIEWER_HASH)) errors.push('REVIEWER_HASH is not a 64-char SHA-256 hex string.');
  if (CONTENT_EDITOR_HASH && !isSha256Hex(CONTENT_EDITOR_HASH)) errors.push('CONTENT_EDITOR_HASH is not a 64-char SHA-256 hex string.');
  // Signing-key configuration: validate at startup, not on first
  // use. The operator-touched-and-invalid case is checked here, so
  // a misconfigured deploy fails fast (in STARTUP_STRICT mode it
  // would already have thrown via startServer(); here we just
  // surface the error in startupSummary so the operator can see
  // what happened before the HTTP listener opened).
  const signingConfig = parseSigningConfig(process.env);
  const signingErrors = validateSigningConfig(signingConfig);
  if (signingErrors.length && isOperatorSigningConfig(signingConfig)) {
    errors.push(
      'Result signing key configuration is invalid: ' + signingErrors.join('; ')
    );
  }
  return errors;
}

function startupWarnings() {
  const warnings = [];
  if (!HANA_ENCRYPT) {
    warnings.push('HANA encryption is disabled. Connection is plaintext.');
  }
  if (!ADMIN_HASH) warnings.push('ADMIN_HASH is not configured. Admin login is disabled.');
  if (!MANAGER_HASH) warnings.push('MANAGER_HASH is not configured. Manager login is disabled.');
  if (!REVIEWER_HASH) warnings.push('REVIEWER_HASH is not configured. Reviewer login is disabled.');
  if (!CONTENT_EDITOR_HASH) warnings.push('CONTENT_EDITOR_HASH is not configured. Content editor login is disabled.');
  if (STALE_SESSION_MINUTES < 5) warnings.push('STALE_SESSION_MINUTES is below 5.');
  return warnings;
}

function startupSummary() {
  // Snapshot the signing config at boot so the summary reflects
  // what the operator set. Calling getSigningKeyMap() here would
  // trigger a warning log if the config is invalid; we want the
  // summary to be side-effect-free.
  const signingConfig = parseSigningConfig(process.env);
  const signingConfigValid = validateSigningConfig(signingConfig).length === 0;
  return {
    ok: startupErrors().length === 0,
    errors: startupErrors(),
    warnings: startupWarnings(),
    env: {
      hasDbConfig: HAS_DB_CONFIG,
      hanaHostConfigured: Boolean(HANA_HOST),
      hanaSchema: HANA_SCHEMA,
      hanaEncrypt: HANA_ENCRYPT,
      hanaSslValidateCertificate: HANA_SSL_VALIDATE_CERTIFICATE,
      adminConfigured: Boolean(ADMIN_HASH),
      managerConfigured: Boolean(MANAGER_HASH),
      reviewerConfigured: Boolean(REVIEWER_HASH),
      contentEditorConfigured: Boolean(CONTENT_EDITOR_HASH),
      anthropicConfigured: Boolean(ANTHROPIC_API_KEY),
      autoClearStaleSessions: AUTO_CLEAR_STALE_SESSIONS,
      staleSessionSweepMinutes: STALE_SESSION_SWEEP_MINUTES,
      // Signing config summary. The key is "configured" if the
      // operator set any of the 4 RESULT_SIGNING_KEY* env vars;
      // "valid" means the set is consistent (kids well-formed,
      // distinct, secrets >= 32 chars, etc.). "activeKid" is what
      // the runtime would actually use (matches the `current`
      // field in the keyMap). If `configured` is false (no
      // operator config) OR `valid` is false (operator-touched-
      // but-invalid), `activeKid` falls back to "legacy".
      signingKeyConfigured: isOperatorSigningConfig(signingConfig),
      signingKeyValid: signingConfigValid,
      signingActiveKid: (isOperatorSigningConfig(signingConfig) && signingConfigValid)
        ? signingConfig.currentId
        : SIGNING_KID_RESERVED
    }
  };
}

function getClientIp(req) {
  if (req.ip) return String(req.ip);
  const xfwd = req.headers['x-forwarded-for'];
  if (typeof xfwd === 'string' && xfwd.trim()) return xfwd.split(',')[0].trim();
  return req.socket?.remoteAddress || 'unknown';
}

function getExamTokenSecret() {
  return crypto
    .createHash('sha256')
    .update([
      HANA_PASSWORD || '',
      HANA_SCHEMA || '',
      ADMIN_HASH || '',
      MANAGER_HASH || '',
      EXAM_NAME || ''
    ].join('|'))
    .digest('hex');
}

function createExamToken(code, nonce, expiry) {
  const payload = `${String(code).trim().toUpperCase()}:${Number(expiry)}:${String(nonce)}`;
  const sig = crypto.createHmac('sha256', getExamTokenSecret()).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}

function parseExamToken(token) {
  try {
    if (!token) return null;
    const decoded = Buffer.from(token, 'base64url').toString();
    const parts = decoded.split(':');
    if (parts.length !== 4) return null;
    const [code, expiryRaw, nonce, sig] = parts;
    const expiry = Number(expiryRaw);
    if (!/^[A-Z2-9]{6}$/.test(String(code || '').trim().toUpperCase())) return null;
    if (!Number.isFinite(expiry) || expiry <= Date.now()) return null;
    const payload = `${String(code).trim().toUpperCase()}:${expiry}:${String(nonce)}`;
    const expected = crypto.createHmac('sha256', getExamTokenSecret()).update(payload).digest('hex');
    if (sig.length !== expected.length) return null;
    if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) return null;
    return { code: String(code).trim().toUpperCase(), expiry, nonce: String(nonce) };
  } catch (_e) {
    return null;
  }
}

function sanitizeProgress(progress) {
  if (!progress || typeof progress !== 'object') return null;
  return {
    answers: Array.isArray(progress.answers) ? progress.answers : [],
    visited: Array.isArray(progress.visited) ? progress.visited : [],
    currentQ: Number.isInteger(progress.currentQ) ? progress.currentQ : (Number(progress.currentQ) || 0),
    incidents: Array.isArray(progress.incidents) ? progress.incidents : [],
    tabSwitches: Number(progress.tabSwitches) || 0,
    elapsedMs: Number(progress.elapsedMs) || 0
  };
}

function tokenSecretForRole(role) {
  if (role === ROLES.MANAGER) return MANAGER_HASH;
  if (role === ROLES.REVIEWER) return REVIEWER_HASH;
  if (role === ROLES.CONTENT_EDITOR) return CONTENT_EDITOR_HASH;
  return ADMIN_HASH;
}

function createAdminToken(role = ROLES.ADMIN) {
  const issuedAt = Date.now();
  const expiry = Date.now() + ADMIN_TTL_MS;
  const nonce = crypto.randomBytes(16).toString('hex');
  const safeRole = isValidRole(role) ? role : ROLES.ADMIN;
  const secret = tokenSecretForRole(safeRole);
  if (!secret) throw new Error(`${safeRole.toUpperCase()}_HASH is not configured.`);
  const payload = `${expiry}:${nonce}:${safeRole}:${issuedAt}`;
  const sig = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}

function parseAdminToken(token) {
  try {
    if (!token) return null;
    const decoded = Buffer.from(token, 'base64url').toString();
    const parts = decoded.split(':');
    const expiry = parts[0];
    const nonce = parts[1];
    const role = parts.length >= 4 ? parts[2] : ROLES.ADMIN;
    const issuedAt = parts.length === 5 ? Number(parts[3]) : 0;
    const sig = parts.length === 5 ? parts[4] : (parts.length === 4 ? parts[3] : parts[2]);
    if (!expiry || !nonce || !sig) return null;
    if (Date.now() > Number(expiry)) return null;
    const payload = parts.length === 5 ? `${expiry}:${nonce}:${role}:${issuedAt}` : (parts.length === 4 ? `${expiry}:${nonce}:${role}` : `${expiry}:${nonce}`);
    const safeRole = isValidRole(role) ? role : ROLES.ADMIN;
    const secret = tokenSecretForRole(safeRole);
    if (!secret) return null;
    const expected = crypto.createHmac('sha256', secret).update(payload).digest('hex');
    if (sig.length !== expected.length) return null;
    if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) return null;
    return {
      role: safeRole,
      expiry: Number(expiry),
      issuedAt: Number.isFinite(issuedAt) ? issuedAt : 0
    };
  } catch (_e) {
    return null;
  }
}

async function getAdminTokenNotBefore(conn) {
  if (!HAS_DB_CONFIG) return 0;
  const now = Date.now();
  if (_runtimeState.adminTokenNotBeforeFetchedAt && (now - _runtimeState.adminTokenNotBeforeFetchedAt) < 30_000) {
    return _runtimeState.adminTokenNotBefore;
  }
  let value = 0;
  if (await hasAppSettingsTable(conn)) {
    value = Number(await getAppSetting(conn, APP_SETTING_ADMIN_TOKEN_NOT_BEFORE, '0')) || 0;
  }
  _runtimeState.adminTokenNotBefore = value;
  _runtimeState.adminTokenNotBeforeFetchedAt = now;
  return value;
}

// Try Bearer auth first when XSUAA is bound. If the request carries a
// valid JWT signed by the XSUAA verification key, derive the role from
// the scope claim. Falls through to the legacy SHA-256 token path
// otherwise, so local dev (no XSUAA bound) keeps working.
//
// Also accepts a JWT from the `xsuaa_jwt` cookie — that's what the
// /oauth/callback endpoint sets after a successful code exchange.
function tryXsuaaAuth(req) {
  // 1. Authorization: Bearer <jwt> (API clients)
  const auth = String(req.headers.authorization || '').trim();
  if (auth.startsWith('Bearer ')) {
    const token = auth.slice(7).trim();
    if (token) {
      const xsuaa = getXsuaaConfig();
      if (xsuaa) {
        const claims = verifyXsuaaJwt(token, xsuaa);
        if (claims) {
          const role = roleFromClaims(claims);
          if (role) return { role, sub: claims.sub || null };
        }
      }
    }
  }
  // 2. xsuaa_jwt cookie (browser flow via /oauth/callback)
  const cookies = parseCookieHeader(req.headers.cookie);
  const cookieToken = cookies['xsuaa_jwt'];
  if (cookieToken) {
    const xsuaa = getXsuaaConfig();
    if (xsuaa) {
      const claims = verifyXsuaaJwt(cookieToken, xsuaa);
      if (claims) {
        const role = roleFromClaims(claims);
        if (role) return { role, sub: claims.sub || null };
      }
    }
  }
  return null;
}

// Auth middlewares are constructed via lib/middleware.js's factory.
// We declare the const AFTER the factory inputs (tryXsuaaAuth, etc.) so
// every dependency is in scope, then expose the middlewares as local
// function references for the rest of server.js to use.
const adminAuth = createAuthMiddleware({
  tryXsuaaAuth,
  parseAdminToken,
  getAdminTokenNotBefore,
  withDb,
  hasDbConfig: HAS_DB_CONFIG,
  getXsuaaConfig,
  hasPermission,
  log: appLog
});

async function requireAdmin(req, res, next) {
  return adminAuth.requireAdmin(req, res, next);
}

function requireAdminRole(role) {
  return adminAuth.requireAdminRole(role);
}

function requirePermission(permission) {
  return adminAuth.requirePermission(permission);
}

async function writeAdminAudit(conn, entry) {
  // Thin delegate to the lib/audit module. Kept as a function reference
  // so call sites in this file stay readable. The real logic — and the
  // failure metrics — live in lib/audit.js.
  return audit.tryWriteAdminAudit(entry).then((r) => r === 'ok');
}

async function tryWriteAdminAudit(entry) {
  // Delegate. Returns true on 'ok', false otherwise. For richer status
  // (skipped_no_table, failed, etc.) call audit.getMetrics() or wait
  // on the returned string directly via audit.tryWriteAdminAudit.
  const r = await audit.tryWriteAdminAudit(entry);
  return r === 'ok';
}

function createPersistedExamSessionFromSet(code, questionSet) {
  const selectedQuestions = pickQuestionsForSession(questionSet, code);
  const { qOrder, optOrders } = buildOrdering(selectedQuestions, code);
  const answerKey = selectedQuestions.map((question) => question.answer.slice());
  return {
    code,
    questionSetId: questionSet.id,
    questionSetName: normalizeExamTitle(questionSet.name),
    examMode: questionSet.examMode,
    showCorrectAnswers: questionSet.showCorrectAnswers === true,
    countsTowardResults: questionSet.countsTowardResults !== false,
    passPct: Number(questionSet.passPct || 80) || 80,
    durationSecs: (Number(questionSet.durationMinutes || 45) || 45) * 60,
    proctorEnabled: questionSet.proctorEnabled !== false,
    questions: selectedQuestions,
    answerKey,
    total: selectedQuestions.length,
    qOrder,
    optOrders,
    createdAt: Date.now()
  };
}

function checkRateLimit(bucket, key, max, windowMs) {
  // Delegate to lib/rate-limit.js. Same shape: returns true if under
  // the limit, false if exceeded. The lib tracks the same sliding window
  // and sweeps when the bucket Map grows past MAX_BUCKETS.
  return rateLimit.checkRateLimit(bucket, key, max, windowMs);
}

function requireExamSession(req, res, next) {
  const parsedToken = parseExamToken(String(req.headers['x-exam-token'] || '').trim());
  if (!parsedToken) return res.status(401).json({ error: 'invalid_exam_session' });
  withDb(async (conn) => {
    const stored = await getSavedSession(conn, parsedToken.code);
    if (!stored || !stored.resumeSupported || !stored.session) return null;
    if (stored.session.tokenNonce !== parsedToken.nonce) return null;
    if (Number(stored.session.tokenExpiry || 0) !== parsedToken.expiry) return null;
    if (Number(stored.session.tokenExpiry || 0) <= Date.now()) return null;
    return stored;
  }).then((stored) => {
    if (!stored) return res.status(401).json({ error: 'invalid_exam_session' });
    req.examSession = stored.session;
    req.examProgress = stored.progress;
    next();
  }).catch((err) => {
    appLog('error', 'exam_session_lookup_failed', { message: err.message });
    res.status(500).json({ error: 'exam_session_lookup_failed' });
  });
}

function clearQuestionSetCache(questionSetId = null) {
  if (questionSetId == null) {
    _questionSetCache.clear();
    return;
  }
  _questionSetCache.delete(String(questionSetId));
}

async function hasAppSettingsTable(conn) {
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM TABLES
      WHERE SCHEMA_NAME = CURRENT_SCHEMA
        AND TABLE_NAME = 'APP_SETTINGS'`
  );
  return Number(rows?.[0]?.CNT || 0) > 0;
}

async function getAppSetting(conn, key, fallbackValue = null) {
  if (!(await hasAppSettingsTable(conn))) return fallbackValue;
  const rows = await execQuery(conn, 'SELECT SETTING_VALUE FROM APP_SETTINGS WHERE SETTING_KEY = ?', [key]);
  if (!rows.length) return fallbackValue;
  return rows[0].SETTING_VALUE == null ? fallbackValue : String(rows[0].SETTING_VALUE);
}

async function setAppSetting(conn, key, value) {
  if (!(await hasAppSettingsTable(conn))) throw new Error('app_settings_missing');
  await execQuery(
    conn,
    `MERGE INTO APP_SETTINGS T
      USING (SELECT ? AS SETTING_KEY, ? AS SETTING_VALUE FROM DUMMY) S
         ON (T.SETTING_KEY = S.SETTING_KEY)
      WHEN MATCHED THEN UPDATE SET
        T.SETTING_VALUE = S.SETTING_VALUE,
        T.UPDATED_AT = CURRENT_UTCTIMESTAMP
      WHEN NOT MATCHED THEN INSERT
        (SETTING_KEY, SETTING_VALUE, UPDATED_AT)
        VALUES (S.SETTING_KEY, S.SETTING_VALUE, CURRENT_UTCTIMESTAMP)`,
    [key, String(value)]
  );
}

async function getExamEnabled(conn) {
  const value = await getAppSetting(conn, APP_SETTING_EXAMS_ENABLED, EXAM_ACTIVE ? 'true' : 'false');
  return String(value).toLowerCase() !== 'false';
}

function normalizeQuestionSetRow(row) {
  const examMode = String(row.EXAM_MODE || EXAM_MODE.GRADED).toUpperCase() === EXAM_MODE.PRACTICE ? EXAM_MODE.PRACTICE : EXAM_MODE.GRADED;
  return {
    id: Number(row.QUESTION_SET_ID),
    name: normalizeExamTitle(row.NAME || 'Exam'),
    description: row.DESCRIPTION || '',
    isActive: Boolean(row.IS_ACTIVE),
    durationMinutes: Number(row.DURATION_MINUTES || 45),
    passPct: Number(row.PASS_PCT || 80),
    proctorEnabled: row.PROCTOR_ENABLED == null ? true : Boolean(row.PROCTOR_ENABLED),
    examMode,
    showCorrectAnswers: row.SHOW_CORRECT_ANSWERS == null ? examMode === EXAM_MODE.PRACTICE : Boolean(row.SHOW_CORRECT_ANSWERS),
    countsTowardResults: row.COUNTS_TOWARD_RESULTS == null ? examMode !== EXAM_MODE.PRACTICE : Boolean(row.COUNTS_TOWARD_RESULTS),
    numQuestions: row.NUM_QUESTIONS == null ? null : Number(row.NUM_QUESTIONS),
    versionGroupId: row.VERSION_GROUP_ID == null ? Number(row.QUESTION_SET_ID) : Number(row.VERSION_GROUP_ID),
    versionNumber: row.VERSION_NUMBER == null ? 1 : Number(row.VERSION_NUMBER),
    lifecycleStatus: String(row.LIFECYCLE_STATUS || QUESTION_SET_LIFECYCLE.PUBLISHED).toUpperCase(),
    parentQuestionSetId: row.PARENT_QUESTION_SET_ID == null ? null : Number(row.PARENT_QUESTION_SET_ID),
    importSource: row.IMPORT_SOURCE || '',
    createdAt: row.CREATED_AT ? new Date(row.CREATED_AT).toISOString() : null,
    updatedAt: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
  };
}

async function getQuestionSetRows(conn, options = {}) {
  const includeCounts = Boolean(options.includeCounts);
  const hasModeColumns = await hasQuestionSetModeColumns(conn);
  const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
  const modeSelect = hasModeColumns
    ? 'qs.EXAM_MODE, qs.SHOW_CORRECT_ANSWERS, qs.COUNTS_TOWARD_RESULTS,'
    : `'${EXAM_MODE.GRADED}' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const modeGroup = hasModeColumns
    ? 'qs.EXAM_MODE, qs.SHOW_CORRECT_ANSWERS, qs.COUNTS_TOWARD_RESULTS,'
    : '';
  const modeSelectPlain = hasModeColumns
    ? 'EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS,'
    : `'${EXAM_MODE.GRADED}' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const versionSelect = hasVersionColumns
    ? 'qs.VERSION_GROUP_ID, qs.VERSION_NUMBER, qs.LIFECYCLE_STATUS, qs.PARENT_QUESTION_SET_ID, qs.IMPORT_SOURCE,'
    : 'qs.QUESTION_SET_ID AS VERSION_GROUP_ID, 1 AS VERSION_NUMBER, \'PUBLISHED\' AS LIFECYCLE_STATUS, NULL AS PARENT_QUESTION_SET_ID, NULL AS IMPORT_SOURCE,';
  const versionGroup = hasVersionColumns
    ? 'qs.VERSION_GROUP_ID, qs.VERSION_NUMBER, qs.LIFECYCLE_STATUS, qs.PARENT_QUESTION_SET_ID, qs.IMPORT_SOURCE,'
    : '';
  const versionSelectPlain = hasVersionColumns
    ? 'VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID, IMPORT_SOURCE,'
    : 'QUESTION_SET_ID AS VERSION_GROUP_ID, 1 AS VERSION_NUMBER, \'PUBLISHED\' AS LIFECYCLE_STATUS, NULL AS PARENT_QUESTION_SET_ID, NULL AS IMPORT_SOURCE,';
  const orderBy = hasVersionColumns
    ? 'VERSION_GROUP_ID ASC, VERSION_NUMBER DESC, NAME ASC, QUESTION_SET_ID ASC'
    : 'IS_ACTIVE DESC, NAME ASC, QUESTION_SET_ID ASC';
  const rows = includeCounts
    ? await execQuery(
        conn,
        `SELECT qs.QUESTION_SET_ID, qs.NAME, qs.DESCRIPTION, qs.IS_ACTIVE,
                qs.DURATION_MINUTES, qs.PASS_PCT, qs.PROCTOR_ENABLED, ${modeSelect}${versionSelect} qs.NUM_QUESTIONS,
                qs.CREATED_AT, qs.UPDATED_AT,
                COUNT(q.QUESTION_ID) AS QUESTION_COUNT
           FROM QUESTION_SETS qs
           LEFT JOIN QUESTION_SET_QUESTIONS q ON q.QUESTION_SET_ID = qs.QUESTION_SET_ID
          GROUP BY qs.QUESTION_SET_ID, qs.NAME, qs.DESCRIPTION, qs.IS_ACTIVE,
                   qs.DURATION_MINUTES, qs.PASS_PCT, qs.PROCTOR_ENABLED, ${modeGroup}${versionGroup} qs.NUM_QUESTIONS,
                   qs.CREATED_AT, qs.UPDATED_AT
          ORDER BY ${hasVersionColumns ? 'qs.VERSION_GROUP_ID ASC, qs.VERSION_NUMBER DESC,' : 'qs.IS_ACTIVE DESC,'} qs.NAME ASC, qs.QUESTION_SET_ID ASC`
      )
    : await execQuery(
        conn,
        `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
                DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelectPlain}${versionSelectPlain} NUM_QUESTIONS,
                CREATED_AT, UPDATED_AT
           FROM QUESTION_SETS
          ORDER BY ${orderBy}`
      );
  return rows.map((row) => ({
    ...normalizeQuestionSetRow(row),
    ...(includeCounts ? { questionCount: Number(row.QUESTION_COUNT || 0) } : {})
  }));
}

async function getActiveQuestionSetRow(conn) {
  const hasModeColumns = await hasQuestionSetModeColumns(conn);
  const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
  const modeSelect = hasModeColumns
    ? 'EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS,'
    : `'${EXAM_MODE.GRADED}' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const versionSelect = hasVersionColumns
    ? 'VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID, IMPORT_SOURCE,'
    : 'QUESTION_SET_ID AS VERSION_GROUP_ID, 1 AS VERSION_NUMBER, \'PUBLISHED\' AS LIFECYCLE_STATUS, NULL AS PARENT_QUESTION_SET_ID, NULL AS IMPORT_SOURCE,';
  const rows = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
            DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelect}${versionSelect} NUM_QUESTIONS,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SETS
      WHERE IS_ACTIVE = TRUE
      ORDER BY QUESTION_SET_ID ASC
      LIMIT 1`
  );
  if (rows.length) return normalizeQuestionSetRow(rows[0]);
  const fallback = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
            DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelect}${versionSelect} NUM_QUESTIONS,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SETS
      ORDER BY QUESTION_SET_ID ASC
      LIMIT 1`
  );
  return fallback.length ? normalizeQuestionSetRow(fallback[0]) : null;
}

async function resolveQuestionSetIdForCode(conn, code) {
  const hasDeletedAt = await hasDeletedAtColumn(conn);
  const assignedRows = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID
       FROM ACCESS_CODES
      WHERE ACCESS_CODE = ?
        ${hasDeletedAt ? 'AND DELETED_AT IS NULL' : ''}`,
    [code]
  );
  const assigned = assignedRows?.[0]?.QUESTION_SET_ID;
  if (assigned != null) return Number(assigned);
  const active = await getActiveQuestionSetRow(conn);
  return active ? Number(active.id) : null;
}

async function loadQuestionSet(conn, questionSetId, options = {}) {
  const allowEmpty = Boolean(options.allowEmpty);
  const cacheKey = String(questionSetId);
  if (!allowEmpty && _questionSetCache.has(cacheKey)) return _questionSetCache.get(cacheKey);

  const hasModeColumns = await hasQuestionSetModeColumns(conn);
  const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
  const modeSelect = hasModeColumns
    ? 'EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS,'
    : `'${EXAM_MODE.GRADED}' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const versionSelect = hasVersionColumns
    ? 'VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID, IMPORT_SOURCE,'
    : 'QUESTION_SET_ID AS VERSION_GROUP_ID, 1 AS VERSION_NUMBER, \'PUBLISHED\' AS LIFECYCLE_STATUS, NULL AS PARENT_QUESTION_SET_ID, NULL AS IMPORT_SOURCE,';
  const metaRows = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
            DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelect}${versionSelect} NUM_QUESTIONS,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SETS
      WHERE QUESTION_SET_ID = ?`,
    [questionSetId]
  );
  if (!metaRows.length) throw new Error(`Question set ${questionSetId} not found.`);

  const sectionRows = await execQuery(
    conn,
    `SELECT SECTION_ID, QUESTION_SET_ID, NAME, DESCRIPTION, DISPLAY_ORDER, DRAW_COUNT,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SECTIONS
      WHERE QUESTION_SET_ID = ?
      ORDER BY DISPLAY_ORDER ASC, SECTION_ID ASC`,
    [questionSetId]
  );

  const questionRows = await execQuery(
    conn,
    `SELECT q.QUESTION_ID, q.QUESTION_SET_ID, q.SECTION_ID, q.QUESTION_INDEX,
            q.STEM, q.NOTE, q.OPTS_JSON, q.ANSWER_JSON, q.MULTI,
            s.NAME AS SECTION_NAME, s.DISPLAY_ORDER AS SECTION_ORDER, s.DRAW_COUNT AS SECTION_DRAW_COUNT
       FROM QUESTION_SET_QUESTIONS q
       LEFT JOIN QUESTION_SECTIONS s ON s.SECTION_ID = q.SECTION_ID
      WHERE q.QUESTION_SET_ID = ?
      ORDER BY q.QUESTION_INDEX ASC, q.QUESTION_ID ASC`,
    [questionSetId]
  );

  const questions = questionRows.map((row) => {
    const opts = parseJsonOrNull(row.OPTS_JSON);
    const answer = parseJsonOrNull(row.ANSWER_JSON);
    if (!Array.isArray(opts) || !Array.isArray(answer)) {
      throw new Error(`Invalid question payload for QUESTION_ID=${row.QUESTION_ID}`);
    }
    return {
      questionId: Number(row.QUESTION_ID),
      questionSetId: Number(row.QUESTION_SET_ID),
      sectionId: row.SECTION_ID == null ? null : Number(row.SECTION_ID),
      questionIndex: Number(row.QUESTION_INDEX),
      stem: String(row.STEM || ''),
      note: row.NOTE || null,
      opts: opts.map((opt) => String(opt)),
      answer: answer.map((value) => Number(value)),
      multi: Boolean(row.MULTI),
      sectionName: row.SECTION_NAME || '',
      sectionOrder: row.SECTION_ORDER == null ? 0 : Number(row.SECTION_ORDER),
      sectionDrawCount: row.SECTION_DRAW_COUNT == null ? null : Number(row.SECTION_DRAW_COUNT)
    };
  });

  if (!allowEmpty && !questions.length) {
    throw new Error(`Question set ${questionSetId} has no questions loaded.`);
  }

  const questionSet = {
    ...normalizeQuestionSetRow(metaRows[0]),
    sections: sectionRows.map((row) => ({
      id: Number(row.SECTION_ID),
      questionSetId: Number(row.QUESTION_SET_ID),
      name: String(row.NAME || ''),
      description: row.DESCRIPTION || '',
      displayOrder: Number(row.DISPLAY_ORDER || 0),
      drawCount: row.DRAW_COUNT == null ? null : Number(row.DRAW_COUNT),
      createdAt: row.CREATED_AT ? new Date(row.CREATED_AT).toISOString() : null,
      updatedAt: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
    })),
    questions,
    totalQuestions: questions.length
  };

  if (!allowEmpty) _questionSetCache.set(cacheKey, questionSet);
  return questionSet;
}

async function loadResolvedQuestionSet(conn, code) {
  const questionSetId = await resolveQuestionSetIdForCode(conn, code);
  if (!questionSetId) throw new Error('No active question set is configured.');
  return loadQuestionSet(conn, questionSetId);
}

async function cloneQuestionSetWithChildren(conn, sourceId, overrides = {}) {
  const source = await loadQuestionSet(conn, sourceId, { allowEmpty: true });
  const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
  const nextVersion = hasVersionColumns
    ? Number((await execQuery(
        conn,
        'SELECT COALESCE(MAX(VERSION_NUMBER), 0) AS MAX_VERSION FROM QUESTION_SETS WHERE VERSION_GROUP_ID = ?',
        [source.versionGroupId || source.id]
      ))?.[0]?.MAX_VERSION || 0) + 1
    : 1;

  const baseName = String(overrides.name || `${source.name} v${nextVersion}`).trim();
  const baseDescription = overrides.description != null ? String(overrides.description) : source.description;
  if (hasVersionColumns) {
    await execQuery(
      conn,
      `INSERT INTO QUESTION_SETS
        (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS, NUM_QUESTIONS, VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID, IMPORT_SOURCE, CREATED_AT, UPDATED_AT)
       VALUES (?, ?, FALSE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
      [
        baseName,
        baseDescription || null,
        source.durationMinutes || 45,
        source.passPct || 80,
        source.proctorEnabled !== false ? 1 : 0,
        source.examMode || EXAM_MODE.GRADED,
        source.showCorrectAnswers === true ? 1 : 0,
        source.countsTowardResults !== false ? 1 : 0,
        source.numQuestions,
        source.versionGroupId || source.id,
        nextVersion,
        String(overrides.lifecycleStatus || QUESTION_SET_LIFECYCLE.DRAFT).toUpperCase(),
        source.id,
        overrides.importSource || source.importSource || null
      ]
    );
  } else {
    await execQuery(
      conn,
      `INSERT INTO QUESTION_SETS
        (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS, NUM_QUESTIONS, CREATED_AT, UPDATED_AT)
       VALUES (?, ?, FALSE, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
      [
        baseName,
        baseDescription || null,
        source.durationMinutes || 45,
        source.passPct || 80,
        source.proctorEnabled !== false ? 1 : 0,
        source.examMode || EXAM_MODE.GRADED,
        source.showCorrectAnswers === true ? 1 : 0,
        source.countsTowardResults !== false ? 1 : 0,
        source.numQuestions
      ]
    );
  }
  const createdRow = await execQuery(conn, 'SELECT MAX(QUESTION_SET_ID) AS QUESTION_SET_ID FROM QUESTION_SETS');
  const newSetId = Number(createdRow?.[0]?.QUESTION_SET_ID);

  const sectionIdMap = new Map();
  for (const section of source.sections || []) {
    await execQuery(
      conn,
      `INSERT INTO QUESTION_SECTIONS
        (QUESTION_SET_ID, NAME, DESCRIPTION, DISPLAY_ORDER, DRAW_COUNT, CREATED_AT, UPDATED_AT)
       VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
      [newSetId, section.name, section.description || null, section.displayOrder || 0, section.drawCount]
    );
    const sectionRow = await execQuery(conn, 'SELECT MAX(SECTION_ID) AS SECTION_ID FROM QUESTION_SECTIONS');
    sectionIdMap.set(section.id, Number(sectionRow?.[0]?.SECTION_ID));
  }

  for (const question of source.questions || []) {
    await execQuery(
      conn,
      `INSERT INTO QUESTION_SET_QUESTIONS
        (QUESTION_SET_ID, SECTION_ID, QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, CREATED_AT, UPDATED_AT)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
      [
        newSetId,
        question.sectionId == null ? null : (sectionIdMap.get(question.sectionId) || null),
        question.questionIndex,
        question.stem,
        question.note || null,
        JSON.stringify(question.opts || []),
        JSON.stringify(question.answer || []),
        question.multi ? 1 : 0
      ]
    );
  }
  clearQuestionSetCache();
  return loadQuestionSet(conn, newSetId, { allowEmpty: true });
}

function buildExamConfigForSet(questionSet, totalOverride = null, examEnabled = EXAM_ACTIVE) {
  const total = totalOverride == null
    ? (questionSet.numQuestions == null ? Number(questionSet.totalQuestions || 0) : Math.min(Number(questionSet.numQuestions || 0), Number(questionSet.totalQuestions || 0)))
    : Number(totalOverride || 0);
  const durationSecs = (Number(questionSet.durationMinutes || 45) || 45) * 60;
  const passPct = Number(questionSet.passPct || 80) || 80;
  return {
    examName: normalizeExamTitle(questionSet.name || EXAM_NAME),
    examDescription: questionSet.description || '',
    examActive: examEnabled,
    durationSecs,
    passPct,
    passScore: Math.ceil(total * passPct / 100),
    total,
    proctorEnabled: questionSet.proctorEnabled !== false,
    examMode: questionSet.examMode || EXAM_MODE.GRADED,
    isPractice: questionSet.examMode === EXAM_MODE.PRACTICE,
    showCorrectAnswers: questionSet.showCorrectAnswers === true,
    countsTowardResults: questionSet.countsTowardResults !== false
  };
}

async function getCodeRow(conn, code) {
  const hasNotes = await hasNotesColumn(conn);
  const hasDeletedAt = await hasDeletedAtColumn(conn);
  const rows = await execQuery(
    conn,
    `SELECT ACCESS_CODE, LABEL, ${hasNotes ? 'NOTES,' : ''} STATUS, SCORE, PCT, PASS, CREATED_AT
       FROM ACCESS_CODES
      WHERE ACCESS_CODE = ?
        ${hasDeletedAt ? 'AND DELETED_AT IS NULL' : ''}`,
    [code]
  );
  if (!rows.length) return null;
  const r = rows[0];
  return {
    accessCode: r.ACCESS_CODE,
    label: r.LABEL || null,
    notes: hasNotes ? (r.NOTES || '') : '',
    status: r.STATUS || 'unused',
    score: r.SCORE,
    pct: r.PCT,
    pass: r.PASS,
    createdAt: r.CREATED_AT || null
  };
}

async function getSavedSession(conn, code) {
  const rows = await execQuery(
    conn,
    `SELECT SESSION_JSON, ELAPSED_MS, TAB_SWITCHES
       FROM EXAM_SESSIONS
      WHERE ACCESS_CODE = ?`,
    [code]
  );
  if (!rows.length) return null;
  const parsed = parseJsonOrNull(rows[0].SESSION_JSON);
  if (!parsed || typeof parsed !== 'object') return null;

  const rawProgress = parsed.progress && typeof parsed.progress === 'object'
    ? parsed.progress
    : parsed;
  const progress = sanitizeProgress(rawProgress);
  if (!progress) return null;
  progress.elapsedMs = Number(rows[0].ELAPSED_MS || progress.elapsedMs || 0);
  progress.tabSwitches = Number(rows[0].TAB_SWITCHES || progress.tabSwitches || 0);

  const session = parsed.session && typeof parsed.session === 'object'
    ? parsed.session
    : null;
  const resumeSupported = Boolean(
    session &&
    Array.isArray(session.questions) &&
    Array.isArray(session.qOrder) &&
    Array.isArray(session.optOrders) &&
    Array.isArray(session.answerKey) &&
    session.tokenNonce &&
    Number(session.tokenExpiry || 0) > 0
  );

  return {
    progress,
    session: resumeSupported ? session : null,
    resumeSupported
  };
}

async function saveSession(conn, code, progress, session = null) {
  const payload = {
    progress: {
      answers: progress.answers || [],
      visited: progress.visited || [],
      currentQ: progress.currentQ || 0,
      incidents: progress.incidents || [],
      tabSwitches: progress.tabSwitches || 0,
      elapsedMs: progress.elapsedMs || 0
    },
    session: session || null
  };
  await execQuery(
    conn,
    `MERGE INTO EXAM_SESSIONS T
      USING (SELECT ? AS ACCESS_CODE, ? AS SESSION_JSON, ? AS ELAPSED_MS, ? AS TAB_SWITCHES FROM DUMMY) S
         ON (T.ACCESS_CODE = S.ACCESS_CODE)
      WHEN MATCHED THEN UPDATE SET
        T.SESSION_JSON = S.SESSION_JSON,
        T.ELAPSED_MS = S.ELAPSED_MS,
        T.TAB_SWITCHES = S.TAB_SWITCHES,
        T.UPDATED_AT = CURRENT_UTCTIMESTAMP
      WHEN NOT MATCHED THEN INSERT
        (ACCESS_CODE, SESSION_JSON, ELAPSED_MS, TAB_SWITCHES, UPDATED_AT)
        VALUES (S.ACCESS_CODE, S.SESSION_JSON, S.ELAPSED_MS, S.TAB_SWITCHES, CURRENT_UTCTIMESTAMP)`,
    [code, JSON.stringify(payload), payload.progress.elapsedMs, payload.progress.tabSwitches]
  );
}

async function deleteSession(conn, code) {
  await execQuery(conn, 'DELETE FROM EXAM_SESSIONS WHERE ACCESS_CODE = ?', [code]);
}

// Stale-session sweeper logic lives in lib/sweeper.js for testability.
// We just delegate with the right threshold and our local deleteSession
// (which goes through the same execQuery wrapper as everywhere else).
async function clearStaleSessionsWithConn(conn) {
  return sweeper.clearStaleSessionsWithConn(conn, STALE_SESSION_MINUTES, deleteSession);
}

// Snapshot of the stale-session sweeper state for the admin /sweeper-status
// endpoint. Pure read of _runtimeState; safe to call any time.
function getSweeperStatus() {
  const now = Date.now();
  const lastTickAt = _runtimeState.sweeperLastTickAt;
  const lastTickAgeMs = lastTickAt > 0 ? now - lastTickAt : null;
  return {
    enabled: _runtimeState.sweeperEnabled,
    intervalMinutes: STALE_SESSION_SWEEP_MINUTES,
    thresholdMinutes: STALE_SESSION_MINUTES,
    startedAt: _runtimeState.sweeperStartedAt || null,
    tickCount: _runtimeState.sweeperTickCount,
    lastTickAt: lastTickAt > 0 ? new Date(lastTickAt).toISOString() : null,
    lastTickAgeMs,
    // Flag as "stuck" if a tick was expected in the last 2 sweep intervals
    // but hasn't happened. Helps surface silent crashes.
    isStuck: _runtimeState.sweeperEnabled
      && lastTickAt > 0
      && lastTickAgeMs !== null
      && lastTickAgeMs > STALE_SESSION_SWEEP_MINUTES * 60 * 1000 * 2,
    lastDurationMs: _runtimeState.sweeperLastDurationMs,
    lastClearedCount: _runtimeState.sweeperLastCleared.length,
    lastClearedCodes: _runtimeState.sweeperLastCleared.slice(0, 20),
    totalCleared: _runtimeState.sweeperTotalCleared,
    lastError: _runtimeState.sweeperLastError
  };
}

function normalizeQuestionUploadEntry(entry) {
  const qNum = Number(entry?.qNum);
  const stem = String(entry?.stem || '').trim();
  const note = String(entry?.note || '').trim();
  const opts = Array.isArray(entry?.opts)
    ? entry.opts.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  const correctIndices = Array.isArray(entry?.correctIndices)
    ? entry.correctIndices.map((item) => Number(item)).filter((n) => Number.isInteger(n) && n >= 0)
    : [];
  const multi = Boolean(entry?.multi);
  return { qNum, stem, note, opts, correctIndices, multi };
}

function validateQuestionUploadEntries(questions) {
  const normalized = Array.isArray(questions) ? questions.map(normalizeQuestionUploadEntry) : [];
  const errors = [];
  const warnings = [];
  const qNums = new Map();
  const stems = new Map();
  const optionSignatures = new Map();

  if (!normalized.length) errors.push('At least one question is required.');
  if (normalized.length > 500) errors.push('Question set upload limit is 500 questions per file.');

  normalized.forEach((entry, idx) => {
    const label = `Row ${idx + 2}`;
    if (!Number.isInteger(entry.qNum) || entry.qNum < 1) errors.push(`${label}: q_num must be a positive integer.`);
    if (!entry.stem) errors.push(`${label}: stem is required.`);
    if (entry.stem.length > 2000) errors.push(`${label}: stem exceeds 2000 characters.`);
    if (entry.note.length > 1000) warnings.push(`${label}: note is longer than 1000 characters.`);
    if (entry.opts.length < 2) errors.push(`${label}: at least two options are required.`);
    if (entry.opts.length > 6) errors.push(`${label}: maximum six options are supported.`);
    if (!entry.correctIndices.length) errors.push(`${label}: correct_indices is required.`);
    if (!entry.multi && entry.correctIndices.length !== 1) errors.push(`${label}: single-select questions need exactly one correct index.`);
    if (entry.correctIndices.some((correctIdx) => correctIdx >= entry.opts.length)) {
      errors.push(`${label}: correct index is out of range for the option list.`);
    }
    if (new Set(entry.opts.map((opt) => opt.toLowerCase())).size !== entry.opts.length) {
      warnings.push(`${label}: duplicate option text detected.`);
    }

    const qNumKey = String(entry.qNum);
    if (qNums.has(qNumKey)) errors.push(`${label}: q_num ${entry.qNum} duplicates another row.`);
    else qNums.set(qNumKey, true);

    const stemKey = entry.stem.toLowerCase();
    if (stemKey) {
      if (stems.has(stemKey)) warnings.push(`${label}: duplicate question stem detected.`);
      else stems.set(stemKey, true);
    }

    const optionKey = entry.opts.map((opt) => opt.toLowerCase()).join('|');
    if (optionKey) {
      if (optionSignatures.has(optionKey)) warnings.push(`${label}: same option set appears in another question.`);
      else optionSignatures.set(optionKey, true);
    }
  });

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    normalized
  };
}

function parseDateFilter(value, endOfDay = false) {
  const text = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  return `${text}T${endOfDay ? '23:59:59.999' : '00:00:00.000'}Z`;
}

async function buildAdminNotifications(conn) {
  const notifications = [];
  const questionSets = await getQuestionSetRows(conn, { includeCounts: true });
  const activeSet = questionSets.find((set) => set.isActive) || null;
  if (!activeSet) {
    notifications.push({ level: 'high', kind: 'config', message: 'No active exam set configured.' });
  } else if ((activeSet.questionCount || 0) === 0) {
    notifications.push({ level: 'high', kind: 'config', message: `Active exam "${activeSet.name}" has no questions.` });
  }
  const staleRows = await execQuery(
    conn,
    `SELECT ACCESS_CODE, UPDATED_AT
       FROM EXAM_SESSIONS
      WHERE UPDATED_AT < ADD_SECONDS(CURRENT_UTCTIMESTAMP, ?)
      ORDER BY UPDATED_AT ASC
      LIMIT 10`,
    [-1 * STALE_SESSION_MINUTES * 60]
  );
  staleRows.forEach((row) => {
    notifications.push({
      level: 'medium',
      kind: 'session',
      message: `Stale active session: ${row.ACCESS_CODE}`,
      detail: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
    });
  });
  const flaggedRows = await execQuery(
    conn,
    `SELECT ACCESS_CODE, INCIDENT_COUNT, SUBMITTED_AT
       FROM EXAM_RESULTS
      WHERE INCIDENT_COUNT > 0
      ORDER BY SUBMITTED_AT DESC
      LIMIT 10`
  );
  flaggedRows.forEach((row) => {
    notifications.push({
      level: Number(row.INCIDENT_COUNT || 0) >= 3 ? 'high' : 'medium',
      kind: 'proctor',
      message: `Exam ${row.ACCESS_CODE} has ${row.INCIDENT_COUNT} flagged incident(s).`,
      detail: row.SUBMITTED_AT ? new Date(row.SUBMITTED_AT).toISOString() : null
    });
  });
  if (await hasAuditLogTable(conn)) {
    const failedLogins = await execQuery(
      conn,
      `SELECT COUNT(*) AS CNT
         FROM ADMIN_AUDIT_LOG
        WHERE ACTION = 'admin_login_failed'
          AND CREATED_AT >= ADD_SECONDS(CURRENT_UTCTIMESTAMP, -3600)`
    );
    const count = Number(failedLogins?.[0]?.CNT || 0);
    if (count >= 3) {
      notifications.push({
        level: 'high',
        kind: 'security',
        message: `${count} failed admin login attempts in the last hour.`
      });
    }
  }
  return notifications.slice(0, 20);
}

async function getResultRecord(conn, code) {
  const rows = await execQuery(
    conn,
    `SELECT RESULT_JSON
       FROM EXAM_RESULTS
      WHERE ACCESS_CODE = ?`,
    [code]
  );
  if (!rows.length) return null;
  return parseJsonOrNull(rows[0].RESULT_JSON);
}

function sanitizeCandidateResult(result) {
  if (!result || typeof result !== 'object') return result;
  const isPractice = result.examMode === EXAM_MODE.PRACTICE || result.isPractice === true;
  const showCorrectAnswers = isPractice && result.showCorrectAnswers === true;
  const safe = {
    ...result,
    isPractice,
    showCorrectAnswers
  };
  if (!showCorrectAnswers) {
    delete safe.questionResults;
  }
  return safe;
}

async function syncAccessCodeSummaryFromResult(conn, code, result) {
  const summary = officialSummaryFields(result);
  await execQuery(
    conn,
    `UPDATE ACCESS_CODES
        SET STATUS = '${CODE_STATUS.COMPLETED}',
            SCORE = ?,
            PCT = ?,
            PASS = ?,
            UPDATED_AT = CURRENT_UTCTIMESTAMP
      WHERE ACCESS_CODE = ?`,
    [summary.score, summary.pct, summary.pass, code]
  );
}

async function saveResult(conn, code, result) {
  const summary = officialSummaryFields(result);
  await execQuery(
    conn,
    `MERGE INTO EXAM_RESULTS T
      USING (
        SELECT ? AS ACCESS_CODE, ? AS SCORE, ? AS TOTAL, ? AS PCT, ? AS PASS, ? AS AUTO_SUBMIT,
               ? AS DURATION_SECS, ? AS TAB_SWITCHES, ? AS INCIDENT_COUNT, ? AS RESULT_JSON
          FROM DUMMY
      ) S
         ON (T.ACCESS_CODE = S.ACCESS_CODE)
      WHEN MATCHED THEN UPDATE SET
        T.SCORE = S.SCORE,
        T.TOTAL = S.TOTAL,
        T.PCT = S.PCT,
        T.PASS = S.PASS,
        T.AUTO_SUBMIT = S.AUTO_SUBMIT,
        T.DURATION_SECS = S.DURATION_SECS,
        T.TAB_SWITCHES = S.TAB_SWITCHES,
        T.INCIDENT_COUNT = S.INCIDENT_COUNT,
        T.RESULT_JSON = S.RESULT_JSON,
        T.SUBMITTED_AT = CURRENT_UTCTIMESTAMP
      WHEN NOT MATCHED THEN INSERT
        (ACCESS_CODE, SCORE, TOTAL, PCT, PASS, AUTO_SUBMIT, DURATION_SECS, TAB_SWITCHES, INCIDENT_COUNT, RESULT_JSON, SUBMITTED_AT)
        VALUES (S.ACCESS_CODE, S.SCORE, S.TOTAL, S.PCT, S.PASS, S.AUTO_SUBMIT, S.DURATION_SECS, S.TAB_SWITCHES, S.INCIDENT_COUNT, S.RESULT_JSON, CURRENT_UTCTIMESTAMP)`,
    [
      code,
      summary.score,
      result.total ?? 0,
      summary.pct,
      summary.pass,
      result.autoSubmit ? 1 : 0,
      result.durationSecs ?? 0,
      result.tabSwitches ?? 0,
      result.incidentCount ?? 0,
      JSON.stringify(result)
    ]
  );
}

function officialSummaryFields(result) {
  if (result?.countsTowardResults === false) {
    return { score: null, pct: null, pass: null };
  }
  return {
    score: result?.score ?? null,
    pct: result?.pct ?? null,
    pass: result?.pass == null ? null : (result.pass ? 1 : 0)
  };
}

async function updateCodeStatus(conn, code, status, result = null) {
  const summary = officialSummaryFields(result);
  await execQuery(
    conn,
    `UPDATE ACCESS_CODES
        SET STATUS = ?,
            SCORE = ?,
            PCT = ?,
            PASS = ?
      WHERE ACCESS_CODE = ?`,
    [status, summary.score, summary.pct, summary.pass, code]
  );
}

app.post('/api/proctor/check', requireExamSession, async (req, res) => {
  if (req.examSession?.proctorEnabled === false) {
    return res.json({ enabled: false, flag: false, reason: null });
  }
  const ip = getClientIp(req);
  if (!checkRateLimit('proctor_check', `${ip}:${req.examSession.code}`, PROCTOR_MAX, PROCTOR_WINDOW)) {
    return res.status(429).json({ error: 'too_many_attempts' });
  }
  const imageB64 = req.body && typeof req.body.imageB64 === 'string' ? req.body.imageB64 : '';
  if (!imageB64) return res.status(400).json({ error: 'missing_image' });
  if (imageB64.length > 2_000_000) return res.status(413).json({ error: 'image_too_large' });
  if (!ANTHROPIC_API_KEY) return res.json({ enabled: false, flag: false, reason: null });

  try {
    const prompt =
      'Exam proctor AI. Respond ONLY with JSON, no other text: {"flag":false,"reason":null} or {"flag":true,"reason":"brief reason"}. ' +
      'Flag ONLY if: no face visible, second person visible, phone/notes visible, candidate clearly looking away for extended period. ' +
      'Do NOT flag for minor head movements, blinking, or adjusting posture.';

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': ANTHROPIC_VERSION
      },
      body: JSON.stringify({
        model: ANTHROPIC_MODEL,
        max_tokens: 100,
        messages: [
          {
            role: 'user',
            content: [
              { type: 'image', source: { type: 'base64', media_type: 'image/jpeg', data: imageB64 } },
              { type: 'text', text: prompt }
            ]
          }
        ]
      }),
      signal: AbortSignal.timeout(12000)
    });

    if (!response.ok) {
      const detail = await response.text();
      appLog('warn', 'proctor_provider_non_ok', { status: response.status, detail: detail.slice(0, 300) });
      return res.status(502).json({ error: 'provider_non_ok' });
    }

    const data = await response.json();
    const text = parseAnthropicText(data.content);
    let parsed = { flag: false, reason: null };
    try {
      parsed = JSON.parse(text);
    } catch (_e) {
      appLog('warn', 'proctor_provider_parse_failed', { text: text.slice(0, 300) });
      return res.status(502).json({ error: 'provider_parse_failed' });
    }

    return res.json({
      enabled: true,
      flag: Boolean(parsed && parsed.flag),
      reason: parsed && parsed.reason ? String(parsed.reason) : null
    });
  } catch (err) {
    appLog('error', 'proctor_check_failed', { message: err.message });
    return res.status(500).json({ error: 'proctor_check_failed' });
  }
});

app.get('/api/health', async (_req, res) => {
  if (!HAS_DB_CONFIG) {
    return res.status(500).json({
      ok: false,
      message: 'Missing HANA env vars.',
      requestId: _req.requestId,
      startup: startupSummary()
    });
  }
  try {
    const status = await withDb(async (conn) => {
      await execQuery(conn, 'SELECT 1 AS OK FROM DUMMY');
      const activeSet = await getActiveQuestionSetRow(conn);
      if (!activeSet) throw new Error('No active question set found.');
      const questionSet = await loadQuestionSet(conn, activeSet.id);
      const setRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM QUESTION_SETS');
      const examEnabled = await getExamEnabled(conn);
      const adminTokenNotBefore = await getAdminTokenNotBefore(conn);
      return {
        activeSet,
        questionSet,
        setCount: Number(setRows?.[0]?.CNT || 0),
        examEnabled,
        adminTokenNotBefore
      };
    });
    res.json({
      ok: true,
      requestId: _req.requestId,
      db: 'connected',
      schema: HANA_SCHEMA,
      totalQuestions: status.questionSet.totalQuestions,
      totalQuestionSets: status.setCount,
      examActive: status.examEnabled,
      startup: startupSummary(),
      adminSessionRevokedAt: status.adminTokenNotBefore ? new Date(status.adminTokenNotBefore).toISOString() : null,
      activeQuestionSet: {
        id: status.activeSet.id,
        name: status.activeSet.name
      }
    });
  } catch (err) {
    appLog('error', 'health_failed', { requestId: _req.requestId, message: err.message });
    res.status(500).json({ ok: false, message: err.message, requestId: _req.requestId, startup: startupSummary() });
  }
});

app.get('/api/status', async (_req, res) => {
  try {
    const status = await withDb(async (conn) => {
      const activeSet = await getActiveQuestionSetRow(conn);
      if (!activeSet) throw new Error('No active question set configured.');
      const questionSet = await loadQuestionSet(conn, activeSet.id);
      const examEnabled = await getExamEnabled(conn);
      return buildExamConfigForSet(questionSet, null, examEnabled);
    });
    res.json(status);
  } catch (err) {
    appLog('error', 'status_failed', { message: err.message });
    res.status(500).json({ error: 'status_failed', message: err.message });
  }
});

app.get('/api/bootstrap', (_req, res) => {
  res.status(410).json({ error: 'bootstrap_removed', message: 'Client bootstrap is disabled for security reasons.' });
});

app.post('/api/validate', async (req, res) => {
  const ip = getClientIp(req);
  if (!checkRateLimit('candidate_validate', String(ip), VALIDATE_MAX, VALIDATE_WINDOW)) {
    return res.status(429).json({ valid: false, reason: 'too_many_attempts' });
  }

  const code = String(req.body?.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.json({ valid: false, reason: 'invalid_format' });

  try {
    const result = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) return { valid: false, reason: 'not_found' };
      const questionSet = await loadResolvedQuestionSet(conn, code);
      const examEnabled = await getExamEnabled(conn);
      const cfg = buildExamConfigForSet(questionSet, null, examEnabled);
      if (!cfg.examActive) return { valid: false, reason: 'exam_not_active', ...cfg };

      const savedResult = await getResultRecord(conn, code);
      if (savedResult || codeRow.status === 'completed') {
        return { valid: true, status: 'completed', result: sanitizeCandidateResult(savedResult) || null, questionSet: { id: questionSet.id, name: questionSet.name }, ...cfg };
      }

      const storedSession = await getSavedSession(conn, code);
      if ((storedSession && storedSession.progress) || codeRow.status === 'active') {
        return {
          valid: true,
          status: 'active',
          progress: storedSession?.resumeSupported ? storedSession.progress : null,
          questionSet: { id: questionSet.id, name: questionSet.name },
          ...cfg
        };
      }

      return { valid: true, status: 'unused', questionSet: { id: questionSet.id, name: questionSet.name }, ...cfg };
    });

    res.json(result);
  } catch (err) {
    appLog('error', 'validate_failed', { code, message: err.message });
    res.status(500).json({ error: 'validate_failed', message: err.message });
  }
});

app.post('/api/session/start', async (req, res) => {
  const code = String(req.body?.code || '').trim().toUpperCase();
  const fresh = Boolean(req.body?.fresh);
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    const payload = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) return { status: 404, body: { error: 'code_not_found' } };
      const questionSet = await loadResolvedQuestionSet(conn, code);
      const examEnabled = await getExamEnabled(conn);
      const sessionConfig = buildExamConfigForSet(questionSet, null, examEnabled);
      if (!sessionConfig.examActive) {
        return { status: 409, body: { error: 'exam_not_active', ...sessionConfig } };
      }

      const savedResult = await getResultRecord(conn, code);
      if (savedResult || codeRow.status === 'completed') {
        return { status: 409, body: { error: 'exam_completed' } };
      }

      if (fresh) await deleteSession(conn, code);
      const storedSession = fresh ? null : await getSavedSession(conn, code);
      const progress = storedSession?.resumeSupported ? storedSession.progress : null;
      const tokenNonce = crypto.randomBytes(16).toString('hex');
      const tokenExpiry = Date.now() + Math.max(
        EXAM_TTL_MS,
        ((Number(questionSet.durationMinutes || 45) || 45) * 60 * 1000) + (30 * 60 * 1000)
      );
      const sessionState = storedSession?.resumeSupported
        && storedSession.session
        && Number(storedSession.session.questionSetId) === Number(questionSet.id)
        ? {
            ...storedSession.session,
            questionSetName: normalizeExamTitle(questionSet.name),
            tokenNonce,
            tokenExpiry
          }
        : {
            ...createPersistedExamSessionFromSet(code, questionSet),
            tokenNonce,
            tokenExpiry
          };
      await saveSession(conn, code, progress || sanitizeProgress({}), sessionState);
      await updateCodeStatus(conn, code, 'active');

      return {
        status: 200,
        body: {
          ok: true,
          examToken: createExamToken(code, tokenNonce, tokenExpiry),
          progress,
          questionSet: { id: questionSet.id, name: questionSet.name },
          ...sessionConfig
        }
      };
    });

    res.status(payload.status).json(payload.body);
  } catch (err) {
    appLog('error', 'session_start_failed', { code, message: err.message });
    res.status(500).json({ error: 'session_start_failed', message: err.message });
  }
});

app.get('/api/question/:displayIdx', requireExamSession, async (req, res) => {
  const displayIdx = Number(req.params.displayIdx);
  const session = req.examSession;
  if (!Number.isInteger(displayIdx) || displayIdx < 0 || displayIdx >= session.qOrder.length) {
    return res.status(400).json({ error: 'invalid_question_index' });
  }

  try {
    const questionIdx = session.qOrder[displayIdx];
    const question = session.questions[questionIdx];
    const optionOrder = session.optOrders[displayIdx];
    if (!question) return res.status(404).json({ error: 'question_not_found' });
    res.json({
      displayIdx,
      total: session.total,
      stem: question.stem,
      note: question.note || null,
      multi: Boolean(question.multi),
      opts: optionOrder.map((idx) => question.opts[idx])
    });
  } catch (err) {
    appLog('error', 'question_fetch_failed', { message: err.message, displayIdx });
    res.status(500).json({ error: 'question_fetch_failed', message: err.message });
  }
});

app.post('/api/progress', requireExamSession, async (req, res) => {
  const session = req.examSession;
  const code = String(req.body?.code || '').trim().toUpperCase();
  if (code !== session.code) return res.status(403).json({ error: 'code_mismatch' });

  const progress = sanitizeProgress({
    answers: req.body?.answers,
    visited: req.body?.visited,
    currentQ: req.body?.currentQ,
    incidents: req.body?.incidents,
    tabSwitches: req.body?.tabSwitches,
    elapsedMs: req.body?.elapsedMs
  });

  try {
    await withDb(async (conn) => {
      await saveSession(conn, code, progress, session);
      await updateCodeStatus(conn, code, 'active');
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'progress_save_failed', { code, message: err.message });
    res.status(500).json({ error: 'progress_save_failed', message: err.message });
  }
});

app.post('/api/submit', requireExamSession, async (req, res) => {
  const session = req.examSession;
  const code = String(req.body?.code || '').trim().toUpperCase();
  if (code !== session.code) return res.status(403).json({ error: 'code_mismatch' });

  const answers = Array.isArray(req.body?.answers) ? req.body.answers : [];
  const durationSecs = Math.max(10, Math.min(Number(req.body?.durationSecs) || 0, Number(session.durationSecs || EXAM_DURATION_SECS) + 300));
  const tabSwitches = Number(req.body?.tabSwitches) || 0;
  const incidents = Array.isArray(req.body?.incidents) ? req.body.incidents : [];
  const autoSubmit = Boolean(req.body?.autoSubmit);

  try {
    const result = gradeExamFromSession(session, answers);
    const record = {
      code,
      questionSetId: session.questionSetId,
      questionSetName: session.questionSetName,
      examMode: session.examMode || EXAM_MODE.GRADED,
      isPractice: session.examMode === EXAM_MODE.PRACTICE,
      showCorrectAnswers: session.showCorrectAnswers === true,
      countsTowardResults: session.countsTowardResults !== false,
      score: result.score,
      total: result.total,
      pct: result.pct,
      pass: result.pass,
      autoSubmit,
      durationSecs,
      tabSwitches,
      incidents,
      incidentCount: incidents.length,
      questionResults: result.questionResults,
      sectionResults: result.sectionResults,
      submittedAt: new Date().toISOString()
    };

    await withDb(async (conn) => {
      await saveResult(conn, code, record);
      await updateCodeStatus(conn, code, 'completed', record);
      await deleteSession(conn, code);
    });

    appLog('info', 'exam_submitted', { code, score: record.score, pct: record.pct, pass: record.pass });
    res.json({ ok: true, result: sanitizeCandidateResult(record) });
  } catch (err) {
    appLog('error', 'submit_failed', { code, message: err.message });
    res.status(500).json({ error: 'submit_failed', message: err.message });
  }
});

app.get('/api/result/:code', requireAdmin, async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  try {
    const result = await withDb(async (conn) => getResultRecord(conn, code));
    if (!result) return res.status(404).json({ error: 'not_found' });
    res.json({ result: sanitizeCandidateResult(result) });
  } catch (err) {
    appLog('error', 'result_fetch_failed', { code, message: err.message });
    res.status(500).json({ error: 'result_fetch_failed', message: err.message });
  }
});

app.get('/api/admin/results/:code/signed-summary', requireAdmin, requirePermission('compliance:read'), async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  try {
    const envelope = await withDb(async (conn) => {
      const result = await getResultRecord(conn, code);
      if (!result) throw new Error('not_found');
      const payload = {
        generatedAt: new Date().toISOString(),
        code,
        questionSetId: result.questionSetId ?? null,
        questionSetName: normalizeExamTitle(result.questionSetName || ''),
        examMode: result.examMode || EXAM_MODE.GRADED,
        score: result.score ?? null,
        total: result.total ?? null,
        pct: result.pct ?? null,
        pass: result.pass ?? null,
        submittedAt: result.submittedAt || null,
        durationSecs: result.durationSecs ?? null,
        incidentCount: result.incidentCount ?? (Array.isArray(result.incidents) ? result.incidents.length : 0)
      };
      return buildSignedEnvelopeLocal(payload);
    });
    res.json({ ok: true, ...envelope });
  } catch (err) {
    const status = err.message === 'not_found' ? 404 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.post('/api/admin/results/verify-signature', requireAdmin, requirePermission('compliance:read'), (req, res) => {
  // Caller can submit the full envelope (preferred — uses kid-based
  // verification with rotation support), or the legacy
  // { payload, signature } pair (recomputed against the current key,
  // for back-compat with the original endpoint shape).
  const body = req.body || {};
  if (body.payload !== undefined && body.signature && body.algorithm) {
    // Reconstruct the envelope exactly as the signer built it. For
    // v2 envelopes we include the version field in the HMAC input,
    // so the verifier needs the original version. The caller is
    // expected to echo the full envelope (incl. version, kid,
    // algorithm) when present.
    const envelope = {
      version: body.version,
      algorithm: body.algorithm,
      kid: body.kid,
      payload: body.payload,
      signature: String(body.signature)
    };
    const matched = signing.getSigningContext().verify(envelope);
    return res.json({ ok: true, valid: matched !== null, matchedKid: matched ? matched.kid : null });
  }
  const payload = body.payload;
  const signature = String(body.signature || '');
  if (!payload || !signature) return res.status(400).json({ error: 'payload_and_signature_required' });
  // Legacy shape: just check the current key.
  const expected = signPayload(payload);
  res.json({ ok: true, valid: expected === signature, matchedKid: getSigningKeyMap().current });
});

// ---------------------------------------------------------------------------
// Auth methods + OAuth 2.0 authorization-code flow
// ---------------------------------------------------------------------------

// Returns which auth methods the SPA can offer the admin. Public — no
// credentials are exposed here, just the list of available mechanisms.
app.get('/api/admin/auth-methods', (_req, res) => {
  const xsuaa = getXsuaaConfig();
  const hasPassword = Boolean(ADMIN_HASH || MANAGER_HASH || REVIEWER_HASH || CONTENT_EDITOR_HASH);
  res.json({
    password: hasPassword,
    xsuaa: xsuaa
      ? {
          enabled: true,
          authorizeUrl: '/oauth/login',
          xsappname: xsuaa.xsappname || null
        }
      : { enabled: false }
  });
});

// Returns the current admin's identity (or 401 if not authenticated).
// Useful for the SPA to bootstrap after an OAuth callback that set a
// cookie — the SPA calls /api/admin/me on page load, and if it returns
// 200, the admin is signed in.
app.get('/api/admin/me', (req, res) => {
  const xsuaaAuth = tryXsuaaAuth(req);
  if (xsuaaAuth) {
    return res.json({
      ok: true,
      role: xsuaaAuth.role,
      sub: xsuaaAuth.sub || null,
      authMethod: 'xsuaa'
    });
  }
  const token = String(req.headers['x-admin-token'] || '').trim();
  const parsed = parseAdminToken(token);
  if (parsed) {
    return res.json({ ok: true, role: parsed.role, sub: null, authMethod: 'token' });
  }
  res.status(401).json({ ok: false, error: 'unauthorized' });
});

// Start the OAuth authorization-code flow. 302s to XSUAA's authorize
// endpoint with a random opaque state. The state is round-tripped to
// /oauth/callback to defend against CSRF.
app.get('/oauth/login', (req, res) => {
  const xsuaa = getXsuaaConfig();
  if (!xsuaa) {
    return res.status(503).json({ error: 'xsuaa_not_bound' });
  }
  const state = generateState();
  const redirectUri = buildOAuthRedirectUri(req);
  const url = buildAuthorizeUrl(xsuaa, redirectUri, state);
  // Stash the state in a short-lived cookie so the callback can verify.
  res.setHeader('Set-Cookie',
    `xsuaa_state=${encodeURIComponent(state)}; Path=/; Max-Age=600; HttpOnly; SameSite=Lax${req.secure ? '; Secure' : ''}`);
  res.redirect(302, url);
});

// XSUAA redirects the user back here with ?code=...&state=...
// We verify state, exchange the code for an access token, set it in
// an httpOnly cookie, and redirect to the SPA root.
app.get('/oauth/callback', async (req, res) => {
  const xsuaa = getXsuaaConfig();
  if (!xsuaa) return res.status(503).json({ error: 'xsuaa_not_bound' });
  const code = String(req.query.code || '');
  const state = String(req.query.state || '');
  if (!code) return res.status(400).json({ error: 'missing_code' });
  const cookies = parseCookieHeader(req.headers.cookie);
  const expectedState = cookies['xsuaa_state'];
  if (!expectedState || !state || expectedState !== state) {
    appLog('warn', 'oauth_state_mismatch', { requestId: req.requestId });
    return res.status(400).json({ error: 'state_mismatch' });
  }
  const redirectUri = buildOAuthRedirectUri(req);
  const tokenResp = await exchangeCodeForToken(xsuaa, code, redirectUri);
  if (!tokenResp || !tokenResp.ok || !tokenResp.accessToken) {
    // Log enough detail to debug: what failed, the upstream status,
    // and the human-readable error_description from XSUAA when present.
    appLog('error', 'oauth_token_exchange_failed', {
      requestId: req.requestId,
      reason: tokenResp ? tokenResp.error : 'null',
      statusCode: tokenResp ? tokenResp.statusCode : null,
      errorDescription: tokenResp ? tokenResp.errorDescription : null,
      message: tokenResp ? tokenResp.message : null
    });
    return res.status(502).json({ error: 'token_exchange_failed' });
  }
  // Set the JWT in an httpOnly cookie. Max-Age from the token's
  // expiresIn (default 1h if absent).
  const maxAge = Math.max(60, Number(tokenResp.expiresIn || 3600));
  const setCookie = [
    `xsuaa_jwt=${encodeURIComponent(tokenResp.accessToken)}`,
    'Path=/',
    `Max-Age=${maxAge}`,
    'HttpOnly',
    'SameSite=Lax',
    req.secure ? 'Secure' : ''
  ].filter(Boolean).join('; ');
  res.setHeader('Set-Cookie', setCookie);
  appLog('info', 'oauth_login_success', { requestId: req.requestId });
  res.redirect(302, '/?auth=ok');
});

// Build the absolute redirect_uri to send to XSUAA. Must match the one
// registered in xs-security.json exactly.
function buildOAuthRedirectUri(req) {
  // Trust X-Forwarded-Proto / X-Forwarded-Host from CF router.
  const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'https').split(',')[0].trim();
  const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
  return `${proto}://${host}/oauth/callback`;
}

app.post('/api/admin/login', (req, res) => {
  if (!ADMIN_HASH && !MANAGER_HASH) return res.status(503).json({ ok: false, error: 'admin_not_configured' });
  const ip = getClientIp(req);
  // IP-based limit: same IP can attempt at most N times per window.
  if (!checkRateLimit('admin_login', String(ip), ADMIN_LOGIN_MAX, ADMIN_LOGIN_WINDOW)) {
    appLog('warn', 'admin_login_rate_limited', { requestId: req.requestId, clientIp: ip });
    return res.status(429).json({ ok: false, error: 'too_many_attempts' });
  }

  const hash = String(req.body?.hash || '').trim().toLowerCase();
  // Hash-based limit: same SHA-256 (i.e. same candidate password) can be
  // tried at most M times per window ACROSS all IPs. Stops a password spray
  // where the attacker rotates IPs to dodge the per-IP limit. Bucket key
  // is the hash itself; never logged.
  if (hash && !checkRateLimit('admin_login_hash', hash, ADMIN_LOGIN_HASH_MAX, ADMIN_LOGIN_HASH_WINDOW)) {
    appLog('warn', 'admin_login_hash_rate_limited', { requestId: req.requestId, clientIp: ip });
    return res.status(429).json({ ok: false, error: 'too_many_attempts' });
  }
  const role = hash && hash === ADMIN_HASH
    ? 'admin'
    : (MANAGER_HASH && hash === MANAGER_HASH
      ? 'manager'
      : (REVIEWER_HASH && hash === REVIEWER_HASH
        ? 'reviewer'
        : (CONTENT_EDITOR_HASH && hash === CONTENT_EDITOR_HASH ? 'content_editor' : null)));
  if (!role) {
    _metrics.loginFailures += 1;
    const ipState = rateLimit.peekRateLimit('admin_login', String(ip));
    const hashState = rateLimit.peekRateLimit('admin_login_hash', hash || 'no_hash');
    const attempts = Math.max(Number(ipState.count || 0), Number(hashState.count || 0));
    void tryWriteAdminAudit({
      action: AUDIT_ACTION.LOGIN_FAILED,
      actor: ROLES.ADMIN,
      clientIp: ip,
      details: { reason: 'invalid_credentials', attempts }
    });
    if (attempts >= Math.max(3, ADMIN_LOGIN_MAX - 2)) {
      appLog('warn', 'admin_login_bruteforce_suspected', { requestId: req.requestId, clientIp: ip, attempts });
    }
    return setTimeout(() => res.status(401).json({ ok: false, error: 'invalid_credentials' }), 350);
  }

  void tryWriteAdminAudit({
    action: AUDIT_ACTION.LOGIN_SUCCESS,
    actor: role,
    clientIp: ip,
    details: { ok: true, role }
  });
  return res.json({ ok: true, token: createAdminToken(role), role });
});

app.post('/api/admin/logout', requireAdmin, async (req, res) => {
  void tryWriteAdminAudit({
    action: AUDIT_ACTION.LOGOUT,
    actor: req.adminRole || ROLES.ADMIN,
    clientIp: getClientIp(req),
    details: { ok: true, authMethod: req.authMethod || 'token' }
  });
  // Clear the XSUAA cookie if it was used for this session. Setting
  // Max-Age=0 expires the cookie on the browser immediately.
  res.setHeader('Set-Cookie',
    'xsuaa_jwt=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax' +
    (req.secure ? '; Secure' : ''));
  res.json({ ok: true });
});

app.post('/api/admin/sessions/revoke-all', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  try {
    const revokedAt = Date.now();
    await withDb(async (conn) => {
      await setAppSetting(conn, APP_SETTING_ADMIN_TOKEN_NOT_BEFORE, String(revokedAt));
      _runtimeState.adminTokenNotBefore = revokedAt;
      _runtimeState.adminTokenNotBeforeFetchedAt = Date.now();
      await writeAdminAudit(conn, {
        action: AUDIT_ACTION.SESSIONS_REVOKED,
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { revokedAt }
      });
    });
    res.json({ ok: true, revokedAt });
  } catch (err) {
    appLog('error', 'admin_revoke_sessions_failed', { requestId: req.requestId, message: err.message });
    res.status(500).json({ error: 'admin_revoke_sessions_failed' });
  }
});

app.get('/api/admin/codes', requireAdmin, requirePermission('codes:read'), async (_req, res) => {
  try {
    const payload = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const [rows, questionSets, examEnabled] = await Promise.all([
        execQuery(
          conn,
          `SELECT c.ACCESS_CODE, c.LABEL, ${hasNotes ? 'c.NOTES,' : `'' AS NOTES,`} c.STATUS, c.SCORE, c.PCT, c.PASS,
                  c.QUESTION_SET_ID, qs.NAME AS QUESTION_SET_NAME, qs.IS_ACTIVE AS QUESTION_SET_ACTIVE,
                  r.SCORE AS RESULT_SCORE, r.PCT AS RESULT_PCT, r.PASS AS RESULT_PASS,
                  r.DURATION_SECS, r.TAB_SWITCHES, r.INCIDENT_COUNT, r.SUBMITTED_AT, r.RESULT_JSON
             FROM ACCESS_CODES c
             LEFT JOIN QUESTION_SETS qs ON qs.QUESTION_SET_ID = c.QUESTION_SET_ID
             LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
            ${hasDeletedAt ? 'WHERE c.DELETED_AT IS NULL' : ''}
            ORDER BY c.ACCESS_CODE ASC`
        ),
        getQuestionSetRows(conn, { includeCounts: true }),
        getExamEnabled(conn)
      ]);
      return { rows, questionSets, examEnabled };
    });
    const codes = payload.rows.map((r) => {
      const parsedResult = parseJsonOrNull(r.RESULT_JSON);
      const countsTowardResults = parsedResult?.countsTowardResults !== false;
      return {
        code: r.ACCESS_CODE,
        label: r.LABEL || '',
        notes: r.NOTES || '',
        status: r.STATUS || 'unused',
        score: countsTowardResults ? (r.SCORE ?? r.RESULT_SCORE ?? parsedResult?.score ?? null) : null,
        pct: countsTowardResults ? (r.PCT ?? r.RESULT_PCT ?? parsedResult?.pct ?? null) : null,
        pass: countsTowardResults
          ? (r.PASS === null || r.PASS === undefined
              ? (r.RESULT_PASS === null || r.RESULT_PASS === undefined ? (parsedResult?.pass ?? null) : Boolean(r.RESULT_PASS))
              : Boolean(r.PASS))
          : null,
        durationSecs: r.DURATION_SECS,
        tabSwitches: r.TAB_SWITCHES || 0,
        incidentCount: r.INCIDENT_COUNT || 0,
        submittedAt: r.SUBMITTED_AT ? new Date(r.SUBMITTED_AT).toISOString() : null,
        incidents: parsedResult?.incidents || [],
        questionResults: Array.isArray(parsedResult?.questionResults) ? parsedResult.questionResults : [],
        questionSetId: r.QUESTION_SET_ID == null ? null : Number(r.QUESTION_SET_ID),
        questionSetName: normalizeExamTitle(r.QUESTION_SET_NAME || ''),
        questionSetActive: r.QUESTION_SET_ACTIVE == null ? false : Boolean(r.QUESTION_SET_ACTIVE),
        examMode: parsedResult?.examMode || '',
        isPractice: parsedResult?.examMode === EXAM_MODE.PRACTICE || parsedResult?.isPractice === true,
        countsTowardResults
      };
    });
    res.json({
      codes,
      questionSets: payload.questionSets,
      examActive: payload.examEnabled,
      role: _req.adminRole || ROLES.ADMIN
    });
  } catch (err) {
    appLog('error', 'admin_codes_failed', { message: err.message });
    res.status(500).json({ error: 'admin_codes_failed', message: err.message });
  }
});

app.get('/api/admin/system-status', requireAdmin, requirePermission('dashboard:read'), async (_req, res) => {
  try {
    const status = await withDb(async (conn) => {
      const questionSets = await getQuestionSetRows(conn, { includeCounts: true });
      const activeSet = questionSets.find((set) => set.isActive) || questionSets[0] || null;
      const activeQuestionSet = activeSet ? await loadQuestionSet(conn, activeSet.id) : null;
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const auditEnabled = await hasAuditLogTable(conn);
      const examEnabled = await getExamEnabled(conn);
      const accessCodeRows = await execQuery(conn, `SELECT COUNT(*) AS CNT FROM ACCESS_CODES ${hasDeletedAt ? 'WHERE DELETED_AT IS NULL' : ''}`);
      const resultRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM EXAM_RESULTS');
      const sessionRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM EXAM_SESSIONS');
      const questionRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM QUESTION_SET_QUESTIONS');
      const staleSessionRows = await execQuery(
        conn,
        `SELECT ACCESS_CODE, UPDATED_AT
           FROM EXAM_SESSIONS
          WHERE UPDATED_AT < ADD_SECONDS(CURRENT_UTCTIMESTAMP, ?)
          ORDER BY UPDATED_AT ASC
          LIMIT 5`,
        [-1 * STALE_SESSION_MINUTES * 60]
      );
      const auditRows = auditEnabled ? await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM ADMIN_AUDIT_LOG') : [{ CNT: 0 }];
      const adminTokenNotBefore = await getAdminTokenNotBefore(conn);
      return {
        ok: Boolean(activeQuestionSet && activeQuestionSet.totalQuestions > 0),
        schema: HANA_SCHEMA,
        questionCount: Number(questionRows?.[0]?.CNT || 0),
        questionSetCount: questionSets.length,
        activeQuestionSet: activeSet ? { id: activeSet.id, name: activeSet.name } : null,
        activeQuestionCount: activeQuestionSet ? activeQuestionSet.totalQuestions : 0,
        accessCodeCount: Number(accessCodeRows?.[0]?.CNT || 0),
        resultCount: Number(resultRows?.[0]?.CNT || 0),
        activeSessionCount: Number(sessionRows?.[0]?.CNT || 0),
        examEnabled,
        staleSessionCount: staleSessionRows.length,
        staleSessionMinutes: STALE_SESSION_MINUTES,
        staleSessions: staleSessionRows.map((row) => ({
          code: row.ACCESS_CODE,
          updatedAt: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
        })),
        auditCount: Number(auditRows?.[0]?.CNT || 0),
        appVersion: APP_VERSION,
        appRevision: APP_REVISION,
        deployedAt: APP_DEPLOYED_AT,
        startupErrors: startupErrors(),
        startupWarnings: startupWarnings(),
        adminSessionRevokedAt: adminTokenNotBefore ? new Date(adminTokenNotBefore).toISOString() : null,
        notesEnabled: Boolean(hasNotes),
        auditEnabled,
        adminConfigured: Boolean(ADMIN_HASH),
        managerConfigured: Boolean(MANAGER_HASH),
        reviewerConfigured: Boolean(REVIEWER_HASH),
        contentEditorConfigured: Boolean(CONTENT_EDITOR_HASH),
        metrics: getMetricsSnapshot(),
        warnings: [
          ...startupWarnings(),
          ...(questionSets.length ? [] : ['No question sets found.']),
          ...(activeQuestionSet && activeQuestionSet.totalQuestions > 0 ? [] : ['Active question set has no questions.']),
          ...(examEnabled ? [] : ['Exam access is currently disabled. Candidates cannot enter codes.']),
          ...(hasNotes ? [] : ['ACCESS_CODES.NOTES column is missing.']),
          ...(staleSessionRows.length ? [`${staleSessionRows.length} active session(s) look stale (${STALE_SESSION_MINUTES}+ min without a save).`] : []),
          ...(auditEnabled ? [] : ['ADMIN_AUDIT_LOG table is missing.']),
          ...(ADMIN_HASH ? [] : ['ADMIN_HASH is not configured on the server.']),
          ...(MANAGER_HASH ? [] : ['MANAGER_HASH is not configured. Manager role login is disabled until it is added.'])
        ]
      };
    });
    res.json(status);
  } catch (err) {
    appLog('error', 'admin_system_status_failed', { message: err.message });
    res.status(500).json({
      ok: false,
      schema: HANA_SCHEMA,
      questionCount: 0,
      questionSetCount: 0,
      activeQuestionSet: null,
      activeQuestionCount: 0,
      accessCodeCount: 0,
      resultCount: 0,
      activeSessionCount: 0,
      examEnabled: EXAM_ACTIVE,
      staleSessionCount: 0,
      staleSessionMinutes: STALE_SESSION_MINUTES,
      staleSessions: [],
      auditCount: 0,
      appVersion: APP_VERSION,
      appRevision: APP_REVISION,
      deployedAt: APP_DEPLOYED_AT,
      startupErrors: startupErrors(),
      startupWarnings: startupWarnings(),
      adminSessionRevokedAt: _runtimeState.adminTokenNotBefore ? new Date(_runtimeState.adminTokenNotBefore).toISOString() : null,
      notesEnabled: false,
      auditEnabled: false,
      adminConfigured: Boolean(ADMIN_HASH),
      managerConfigured: Boolean(MANAGER_HASH),
      reviewerConfigured: Boolean(REVIEWER_HASH),
      contentEditorConfigured: Boolean(CONTENT_EDITOR_HASH),
      metrics: getMetricsSnapshot(),
      warnings: [...startupWarnings(), 'Could not load system status from HANA.'],
      error: 'admin_system_status_failed'
    });
  }
});

app.get('/api/admin/audit', requireAdmin, requirePermission('audit:read'), async (req, res) => {
  const limit = Math.min(Math.max(Number(req.query?.limit) || 20, 1), 100);
  try {
    const entries = await withDb(async (conn) => {
      if (!(await hasAuditLogTable(conn))) return [];
      const rows = await execQuery(
        conn,
        `SELECT AUDIT_ID, ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT
           FROM ADMIN_AUDIT_LOG
          ORDER BY CREATED_AT DESC
          LIMIT ${limit}`
      );
      return rows.map((row) => ({
        id: row.AUDIT_ID,
        action: row.ACTION,
        targetCode: row.TARGET_CODE || '',
        actor: row.ACTOR || ROLES.ADMIN,
        clientIp: row.CLIENT_IP || '',
        createdAt: row.CREATED_AT ? new Date(row.CREATED_AT).toISOString() : null,
        details: parseJsonOrNull(row.DETAILS_JSON) || null
      }));
    });
    res.json({ entries });
  } catch (err) {
    appLog('error', 'admin_audit_fetch_failed', { message: err.message });
    res.status(500).json({ error: 'admin_audit_fetch_failed' });
  }
});

app.get('/api/admin/metrics', requireAdmin, requirePermission('dashboard:read'), (_req, res) => {
  res.json({ ok: true, metrics: getMetricsSnapshot() });
});

app.get('/api/admin/notifications', requireAdmin, requirePermission('notifications:read'), async (_req, res) => {
  try {
    const notifications = await withDb(async (conn) => buildAdminNotifications(conn));
    res.json({ ok: true, notifications });
  } catch (err) {
    appLog('error', 'admin_notifications_failed', { requestId: _req.requestId, message: err.message });
    res.status(500).json({ error: 'admin_notifications_failed' });
  }
});

app.get('/api/admin/audit/export.json', requireAdmin, requirePermission('audit:export'), async (req, res) => {
  const limit = Math.min(Math.max(Number(req.query?.limit) || 500, 1), 5000);
  try {
    const envelope = await withDb(async (conn) => {
      if (!(await hasAuditLogTable(conn))) return buildSignedEnvelopeLocal({ entries: [], generatedAt: new Date().toISOString(), limit });
      const rows = await execQuery(
        conn,
        `SELECT AUDIT_ID, ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT
           FROM ADMIN_AUDIT_LOG
          ORDER BY CREATED_AT DESC
          LIMIT ${limit}`
      );
      const payload = {
        generatedAt: new Date().toISOString(),
        limit,
        entries: rows.map((row) => ({
          id: row.AUDIT_ID,
          action: row.ACTION,
          targetCode: row.TARGET_CODE || '',
          actor: row.ACTOR || '',
          clientIp: row.CLIENT_IP || '',
          createdAt: row.CREATED_AT ? new Date(row.CREATED_AT).toISOString() : null,
          details: parseJsonOrNull(row.DETAILS_JSON) || null
        }))
      };
      return buildSignedEnvelopeLocal(payload);
    });
    res.json({ ok: true, ...envelope });
  } catch (err) {
    appLog('error', 'admin_audit_export_failed', { requestId: req.requestId, message: err.message });
    res.status(500).json({ error: 'admin_audit_export_failed' });
  }
});

app.post('/api/admin/exam-availability', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  const enabled = req.body?.enabled !== false;
  try {
    await withDb(async (conn) => {
      await setAppSetting(conn, APP_SETTING_EXAMS_ENABLED, enabled ? 'true' : 'false');
      await writeAdminAudit(conn, {
        action: 'admin_exam_availability_updated',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { enabled }
      });
    });
    res.json({ ok: true, enabled });
  } catch (err) {
    appLog('error', 'admin_exam_availability_failed', { message: err.message });
    res.status(500).json({ error: 'admin_exam_availability_failed' });
  }
});

app.get('/api/admin/results/:code/review', requireAdmin, requirePermission('results:read'), async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  try {
    const payload = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) throw new Error('code_not_found');
      const result = await getResultRecord(conn, code);
      if (!result) throw new Error('result_not_found');
      return {
        code,
        label: codeRow.label || '',
        status: codeRow.status || 'completed',
        result
      };
    });
    res.json({
      ok: true,
      code: payload.code,
      label: payload.label,
      status: payload.status,
      result: payload.result,
      reviewAvailable: Array.isArray(payload.result?.questionResults) && payload.result.questionResults.length > 0
    });
  } catch (err) {
    const status = err.message === 'code_not_found' || err.message === 'result_not_found' ? 404 : 500;
    res.status(status).json({ error: err.message });
  }
});

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((p / 100) * sorted.length)));
  return sorted[idx];
}

app.get('/api/admin/question-sets/:id/analytics', requireAdmin, async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    const payload = await withDb(async (conn) => {
      const questionSet = await loadQuestionSet(conn, id, { allowEmpty: true });
      const rows = await execQuery(
        conn,
        `SELECT r.ACCESS_CODE, r.SCORE, r.TOTAL, r.PCT, r.PASS, r.DURATION_SECS, r.RESULT_JSON, r.SUBMITTED_AT,
                c.LABEL, c.QUESTION_SET_ID
           FROM EXAM_RESULTS r
           LEFT JOIN ACCESS_CODES c ON c.ACCESS_CODE = r.ACCESS_CODE
          ORDER BY r.SUBMITTED_AT DESC`,
        []
      );

      const attempts = rows
        .map((row) => {
          const result = parseJsonOrNull(row.RESULT_JSON) || {};
          const score = Number(row.SCORE ?? result.score);
          const total = Number(row.TOTAL ?? result.total);
          const pct = Number(row.PCT ?? result.pct);
          const durationSecs = Number(row.DURATION_SECS ?? result.durationSecs);
          return {
            code: row.ACCESS_CODE,
            label: row.LABEL || '',
            questionSetId: Number(row.QUESTION_SET_ID ?? result.questionSetId ?? 0),
            score: Number.isFinite(score) ? score : null,
            total: Number.isFinite(total) ? total : null,
            pct: Number.isFinite(pct) ? pct : null,
            pass: row.PASS == null ? Boolean(result.pass) : Boolean(row.PASS),
            durationSecs: Number.isFinite(durationSecs) ? durationSecs : null,
            examMode: result.examMode || questionSet.examMode || EXAM_MODE.GRADED,
            questionResults: Array.isArray(result.questionResults) ? result.questionResults : [],
            sectionResults: Array.isArray(result.sectionResults) ? result.sectionResults : [],
            submittedAt: row.SUBMITTED_AT ? new Date(row.SUBMITTED_AT).toISOString() : result.submittedAt || null
          };
        })
        .filter((attempt) => Number(attempt.questionSetId || 0) === id);

      const completed = attempts.filter((item) => item.score != null && item.total != null && item.pct != null);
      const pctValues = completed.map((item) => item.pct).filter(Number.isFinite);
      const durationValues = completed.map((item) => item.durationSecs).filter(Number.isFinite);
      const avg = (values) => values.length ? Math.round(values.reduce((sum, value) => sum + value, 0) / values.length) : null;

      const questionMap = new Map();
      const sectionMap = new Map();
      for (const attempt of attempts) {
        for (const qr of attempt.questionResults) {
          const key = qr.questionId != null ? `id:${qr.questionId}` : `idx:${qr.questionIndex}`;
          if (!questionMap.has(key)) {
            questionMap.set(key, {
              questionId: qr.questionId ?? null,
              questionIndex: qr.questionIndex ?? null,
              stem: qr.stem || 'Question',
              sectionName: qr.sectionName || '',
              answered: 0,
              correct: 0,
              wrong: 0
            });
          }
          const item = questionMap.get(key);
          item.answered += 1;
          if (qr.correct) item.correct += 1;
          else item.wrong += 1;
        }
        for (const sr of attempt.sectionResults) {
          const key = sr.sectionId != null ? String(sr.sectionId) : sr.name || 'Section';
          if (!sectionMap.has(key)) {
            sectionMap.set(key, {
              sectionId: sr.sectionId ?? null,
              name: sr.name || 'Section',
              correct: 0,
              total: 0
            });
          }
          const item = sectionMap.get(key);
          item.correct += Number(sr.correct || 0);
          item.total += Number(sr.total || 0);
        }
      }

      const questionStats = [...questionMap.values()]
        .map((item) => ({
          ...item,
          pctCorrect: item.answered ? Math.round((item.correct / item.answered) * 100) : null
        }))
        .filter((item) => item.answered > 0);

      const sectionStats = [...sectionMap.values()]
        .map((item) => ({
          ...item,
          wrong: Math.max(0, item.total - item.correct),
          pctCorrect: item.total ? Math.round((item.correct / item.total) * 100) : null
        }))
        .sort((a, b) => String(a.name).localeCompare(String(b.name)));

      return {
        questionSet: {
          id: questionSet.id,
          name: questionSet.name,
          examMode: questionSet.examMode,
          isPractice: questionSet.examMode === EXAM_MODE.PRACTICE,
          questionCount: questionSet.totalQuestions
        },
        summary: {
          attempts: attempts.length,
          completed: completed.length,
          gradedAttempts: attempts.filter((item) => item.examMode !== EXAM_MODE.PRACTICE).length,
          practiceAttempts: attempts.filter((item) => item.examMode === EXAM_MODE.PRACTICE).length,
          averageScore: completed.length ? Number((completed.reduce((sum, item) => sum + item.score, 0) / completed.length).toFixed(1)) : null,
          averagePct: avg(pctValues),
          passRate: completed.length ? Math.round((completed.filter((item) => item.pass).length / completed.length) * 100) : null,
          averageDurationSecs: avg(durationValues),
          medianDurationSecs: percentile(durationValues, 50)
        },
        hardestQuestions: [...questionStats].sort((a, b) => a.pctCorrect - b.pctCorrect).slice(0, 10),
        easiestQuestions: [...questionStats].sort((a, b) => b.pctCorrect - a.pctCorrect).slice(0, 10),
        sectionStats,
        recentAttempts: attempts.slice(0, 20).map((item) => ({
          code: item.code,
          label: item.label,
          score: item.score,
          total: item.total,
          pct: item.pct,
          pass: item.pass,
          durationSecs: item.durationSecs,
          examMode: item.examMode,
          submittedAt: item.submittedAt
        }))
      };
    });
    res.json({ ok: true, ...payload });
  } catch (err) {
    appLog('error', 'admin_question_set_analytics_failed', { id, message: err.message });
    res.status(500).json({ error: 'admin_question_set_analytics_failed', message: err.message });
  }
});

app.get('/api/admin/analytics/overview', requireAdmin, requirePermission('analytics:read'), async (req, res) => {
  const days = Math.min(Math.max(Number(req.query?.days) || 30, 7), 180);
  try {
    const payload = await withDb(async (conn) => {
      const rows = await execQuery(
        conn,
        `SELECT r.ACCESS_CODE, r.SCORE, r.TOTAL, r.PCT, r.PASS, r.DURATION_SECS, r.RESULT_JSON, r.SUBMITTED_AT,
                c.LABEL, c.QUESTION_SET_ID, qs.NAME AS QUESTION_SET_NAME
           FROM EXAM_RESULTS r
           LEFT JOIN ACCESS_CODES c ON c.ACCESS_CODE = r.ACCESS_CODE
           LEFT JOIN QUESTION_SETS qs ON qs.QUESTION_SET_ID = c.QUESTION_SET_ID
          WHERE r.SUBMITTED_AT >= ADD_DAYS(CURRENT_UTCTIMESTAMP, ?)
          ORDER BY r.SUBMITTED_AT DESC`,
        [-1 * days]
      );
      const attempts = rows.map((row) => {
        const result = parseJsonOrNull(row.RESULT_JSON) || {};
        return {
          code: row.ACCESS_CODE,
          questionSetId: Number(row.QUESTION_SET_ID ?? result.questionSetId ?? 0),
          questionSetName: normalizeExamTitle(row.QUESTION_SET_NAME || result.questionSetName || 'Exam'),
          pct: Number(row.PCT ?? result.pct ?? 0),
          score: Number(row.SCORE ?? result.score ?? 0),
          total: Number(row.TOTAL ?? result.total ?? 0),
          pass: row.PASS == null ? Boolean(result.pass) : Boolean(row.PASS),
          durationSecs: Number(row.DURATION_SECS ?? result.durationSecs ?? 0),
          examMode: result.examMode || EXAM_MODE.GRADED,
          sectionResults: Array.isArray(result.sectionResults) ? result.sectionResults : [],
          submittedAt: row.SUBMITTED_AT ? new Date(row.SUBMITTED_AT).toISOString() : null
        };
      });
      const byDay = new Map();
      const bySet = new Map();
      const weakSections = new Map();
      for (const attempt of attempts) {
        const day = String(attempt.submittedAt || '').slice(0, 10);
        if (day) {
          const item = byDay.get(day) || { day, attempts: 0, passCount: 0, avgPctTotal: 0 };
          item.attempts += 1;
          item.avgPctTotal += Number(attempt.pct || 0);
          if (attempt.pass) item.passCount += 1;
          byDay.set(day, item);
        }
        const setKey = String(attempt.questionSetId || 0);
        const setItem = bySet.get(setKey) || { questionSetId: attempt.questionSetId, name: attempt.questionSetName, attempts: 0, avgPctTotal: 0, passCount: 0 };
        setItem.attempts += 1;
        setItem.avgPctTotal += Number(attempt.pct || 0);
        if (attempt.pass) setItem.passCount += 1;
        bySet.set(setKey, setItem);
        for (const section of attempt.sectionResults) {
          const key = `${attempt.questionSetId}:${section.sectionId ?? section.name}`;
          const sectionItem = weakSections.get(key) || { questionSetId: attempt.questionSetId, questionSetName: attempt.questionSetName, sectionId: section.sectionId ?? null, name: section.name || 'Section', totalPct: 0, attempts: 0 };
          sectionItem.totalPct += Number(section.pct || 0);
          sectionItem.attempts += 1;
          weakSections.set(key, sectionItem);
        }
      }
      return {
        days,
        summary: {
          attempts: attempts.length,
          passRate: attempts.length ? Math.round((attempts.filter((item) => item.pass).length / attempts.length) * 100) : null,
          averagePct: attempts.length ? Math.round(attempts.reduce((sum, item) => sum + Number(item.pct || 0), 0) / attempts.length) : null,
          averageDurationSecs: attempts.length ? Math.round(attempts.reduce((sum, item) => sum + Number(item.durationSecs || 0), 0) / attempts.length) : null
        },
        trend: [...byDay.values()].sort((a, b) => String(a.day).localeCompare(String(b.day))).map((item) => ({
          day: item.day,
          attempts: item.attempts,
          averagePct: item.attempts ? Math.round(item.avgPctTotal / item.attempts) : null,
          passRate: item.attempts ? Math.round((item.passCount / item.attempts) * 100) : null
        })),
        byQuestionSet: [...bySet.values()].sort((a, b) => b.attempts - a.attempts).map((item) => ({
          questionSetId: item.questionSetId,
          name: item.name,
          attempts: item.attempts,
          averagePct: item.attempts ? Math.round(item.avgPctTotal / item.attempts) : null,
          passRate: item.attempts ? Math.round((item.passCount / item.attempts) * 100) : null
        })),
        weakestSections: [...weakSections.values()]
          .map((item) => ({ ...item, averagePct: item.attempts ? Math.round(item.totalPct / item.attempts) : null }))
          .sort((a, b) => a.averagePct - b.averagePct)
          .slice(0, 12)
      };
    });
    res.json({ ok: true, ...payload });
  } catch (err) {
    appLog('error', 'admin_analytics_overview_failed', { requestId: req.requestId, message: err.message });
    res.status(500).json({ error: 'admin_analytics_overview_failed' });
  }
});

app.post('/api/admin/clear-stale-sessions', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  try {
    const payload = await withDb(async (conn) => {
      const cleared = await clearStaleSessionsWithConn(conn);

      await writeAdminAudit(conn, {
        action: 'admin_stale_sessions_cleared',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { count: cleared.length, codes: cleared.slice(0, 20) }
      });

      return { ok: true, clearedCount: cleared.length, clearedCodes: cleared };
    });

    res.json(payload);
  } catch (err) {
    appLog('error', 'admin_clear_stale_sessions_failed', { message: err.message });
    res.status(500).json({ error: 'admin_clear_stale_sessions_failed' });
  }
});

// Read-only: returns the sweeper's last-tick metadata. Used to surface
// "stuck" states (timer started but no recent tick) and silent crashes
// (HANA connection failure logged on every attempt).
app.get('/api/admin/sweeper-status', requireAdmin, requirePermission('dashboard:read'), (_req, res) => {
  res.json({ ok: true, sweeper: getSweeperStatus() });
});

app.post('/api/admin/note', requireAdmin, requirePermission('codes:note'), async (req, res) => {
  const code = String(req.body?.code || '').trim().toUpperCase();
  const notes = String(req.body?.notes || '');
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    await withDb(async (conn) => {
      if (await hasNotesColumn(conn)) {
        await execQuery(conn, 'UPDATE ACCESS_CODES SET NOTES = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE ACCESS_CODE = ?', [notes, code]);
      }
      await writeAdminAudit(conn, {
        action: 'admin_note_saved',
        targetCode: code,
        actor: req.adminRole || ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { noteLength: notes.length }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'admin_note_failed', { code, message: err.message });
    res.status(500).json({ error: 'admin_note_failed', message: err.message });
  }
});

app.post('/api/admin/reset', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  const code = String(req.body?.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    await withDb(async (conn) => {
      await deleteSession(conn, code);
      await execQuery(conn, 'DELETE FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
      await execQuery(conn, 'UPDATE ACCESS_CODES SET STATUS = ?, SCORE = NULL, PCT = NULL, PASS = NULL, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE ACCESS_CODE = ?', ['unused', code]);
      await writeAdminAudit(conn, {
        action: 'admin_code_reset',
        targetCode: code,
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { status: 'unused' }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'admin_reset_failed', { code, message: err.message });
    res.status(500).json({ error: 'admin_reset_failed', message: err.message });
  }
});

app.delete('/api/admin/codes/:code', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) throw new Error('code_not_found');
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      await deleteSession(conn, code);
      if (hasDeletedAt) {
        await execQuery(
          conn,
          `UPDATE ACCESS_CODES
              SET STATUS = 'deleted',
                  DELETED_AT = CURRENT_UTCTIMESTAMP,
                  DELETED_BY = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE ACCESS_CODE = ?`,
          [req.adminRole || 'admin', code]
        );
      } else {
        await execQuery(conn, 'DELETE FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
        await execQuery(conn, 'DELETE FROM ACCESS_CODES WHERE ACCESS_CODE = ?', [code]);
      }
      await writeAdminAudit(conn, {
        action: 'admin_code_deleted',
        targetCode: code,
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { previousStatus: codeRow.status || 'unknown' }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    const status = err.message === 'code_not_found' ? 404 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.post('/api/admin/codes/bulk-delete', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  const codes = Array.isArray(req.body?.codes)
    ? [...new Set(req.body.codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => /^[A-Z2-9]{6}$/.test(code)))]
    : [];
  if (!codes.length) return res.status(400).json({ error: 'codes_required' });
  if (codes.length > 500) return res.status(400).json({ error: 'too_many_codes' });

  try {
    const payload = await withDb(async (conn) => {
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const deleted = [];
      const notFound = [];
      const summary = { unused: 0, active: 0, completed: 0, other: 0 };
      for (const code of codes) {
        const codeRow = await getCodeRow(conn, code);
        if (!codeRow) {
          notFound.push(code);
          continue;
        }
        summary[summary[codeRow.status] == null ? 'other' : codeRow.status] += 1;
        await deleteSession(conn, code);
        if (hasDeletedAt) {
          await execQuery(
            conn,
            `UPDATE ACCESS_CODES
                SET STATUS = 'deleted',
                    DELETED_AT = CURRENT_UTCTIMESTAMP,
                    DELETED_BY = ?,
                    UPDATED_AT = CURRENT_UTCTIMESTAMP
              WHERE ACCESS_CODE = ?`,
            [req.adminRole || 'admin', code]
          );
        } else {
          await execQuery(conn, 'DELETE FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
          await execQuery(conn, 'DELETE FROM ACCESS_CODES WHERE ACCESS_CODE = ?', [code]);
        }
        deleted.push(code);
      }
      await writeAdminAudit(conn, {
        action: 'admin_codes_bulk_deleted',
        actor: req.adminRole || ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { count: deleted.length, summary, codes: deleted.slice(0, 50), notFound: notFound.slice(0, 50) }
      });
      return { ok: true, deletedCount: deleted.length, deleted, notFound, summary };
    });
    res.json(payload);
  } catch (err) {
    appLog('error', 'admin_bulk_delete_codes_failed', { message: err.message });
    res.status(500).json({ error: 'admin_bulk_delete_codes_failed' });
  }
});

app.post('/api/admin/generate', requireAdmin, requirePermission('codes:generate'), async (req, res) => {
  const count = Math.min(Math.max(Number(req.body?.count) || 10, 1), 200);
  const chars = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

  try {
    const added = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const existingRows = await execQuery(conn, 'SELECT ACCESS_CODE FROM ACCESS_CODES');
      const activeRows = hasDeletedAt ? await execQuery(conn, 'SELECT ACCESS_CODE FROM ACCESS_CODES WHERE DELETED_AT IS NULL') : existingRows;
      const used = new Set(existingRows.map((r) => r.ACCESS_CODE));
      const seatBase = activeRows.length + 1;
      const created = [];

      while (created.length < count) {
        let code = '';
        for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
        if (!used.has(code)) {
          used.add(code);
          created.push(code);
        }
      }

      const sql = hasNotes
        ? `INSERT INTO ACCESS_CODES (ACCESS_CODE, LABEL, NOTES, STATUS, CREATED_AT, UPDATED_AT)
             VALUES (?, ?, ?, 'unused', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`
        : `INSERT INTO ACCESS_CODES (ACCESS_CODE, LABEL, STATUS, CREATED_AT, UPDATED_AT)
             VALUES (?, ?, 'unused', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`;

      for (let i = 0; i < created.length; i++) {
        const label = `Seat ${String(seatBase + i).padStart(3, '0')}`;
        const params = hasNotes ? [created[i], label, ''] : [created[i], label];
        await execQuery(conn, sql, params);
      }
      await writeAdminAudit(conn, {
        action: 'admin_codes_generated',
        actor: req.adminRole || ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { count: created.length, firstCode: created[0] || null, lastCode: created[created.length - 1] || null }
      });
      return created.length;
    });

    res.json({ ok: true, added });
  } catch (err) {
    appLog('error', 'admin_generate_failed', { message: err.message });
    res.status(500).json({ error: 'admin_generate_failed', message: err.message });
  }
});

app.post('/api/admin/results/repair-summaries', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  try {
    const payload = await withDb(async (conn) => {
      const rows = await execQuery(conn, 'SELECT ACCESS_CODE, RESULT_JSON FROM EXAM_RESULTS');
      let repaired = 0;
      let skipped = 0;
      for (const row of rows) {
        const result = parseJsonOrNull(row.RESULT_JSON);
        if (!result || typeof result.score !== 'number' || typeof result.pct !== 'number') {
          skipped += 1;
          continue;
        }
        if (result.countsTowardResults === false) {
          skipped += 1;
          continue;
        }
        await syncAccessCodeSummaryFromResult(conn, row.ACCESS_CODE, result);
        repaired += 1;
      }
      await writeAdminAudit(conn, {
        action: 'admin_result_summaries_repaired',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { repaired, skipped }
      });
      return { repaired, skipped };
    });
    res.json({ ok: true, ...payload });
  } catch (err) {
    appLog('error', 'admin_repair_result_summaries_failed', { message: err.message });
    res.status(500).json({ error: 'admin_repair_result_summaries_failed' });
  }
});

app.post('/api/admin/results/clear-summaries', requireAdmin, requireAdminRole(ROLES.ADMIN), async (req, res) => {
  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'UPDATE ACCESS_CODES SET SCORE = NULL, PCT = NULL, PASS = NULL, UPDATED_AT = CURRENT_UTCTIMESTAMP');
      await execQuery(conn, 'UPDATE EXAM_RESULTS SET SCORE = NULL, PCT = NULL, PASS = NULL');
      await writeAdminAudit(conn, {
        action: 'admin_result_summaries_cleared',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { scope: 'all' }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'admin_clear_result_summaries_failed', { message: err.message });
    res.status(500).json({ error: 'admin_clear_result_summaries_failed' });
  }
});

app.post('/api/admin/codes/:code/question-set', requireAdmin, requirePermission('codes:assign'), async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  const questionSetIdRaw = req.body?.questionSetId;
  const questionSetId = questionSetIdRaw == null || questionSetIdRaw === '' ? null : Number(questionSetIdRaw);
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  if (questionSetIdRaw != null && questionSetIdRaw !== '' && !Number.isInteger(questionSetId)) {
    return res.status(400).json({ error: 'invalid_question_set_id' });
  }

  try {
    await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) throw new Error('code_not_found');
      if (codeRow.status !== 'unused') throw new Error('code_assignment_requires_unused_status');
      if (questionSetId != null) {
        const qs = await execQuery(conn, 'SELECT QUESTION_SET_ID FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [questionSetId]);
        if (!qs.length) throw new Error('question_set_not_found');
      }
      await execQuery(
        conn,
        'UPDATE ACCESS_CODES SET QUESTION_SET_ID = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE ACCESS_CODE = ?',
        [questionSetId, code]
      );
      await writeAdminAudit(conn, {
        action: 'admin_code_question_set_assigned',
        targetCode: code,
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { questionSetId }
      });
    });
    res.json({ ok: true, questionSetId });
  } catch (err) {
    const status =
      err.message === 'code_not_found' ? 404 :
      err.message === 'question_set_not_found' ? 404 :
      err.message === 'code_assignment_requires_unused_status' ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.get('/api/admin/question-sets', requireAdmin, requirePermission('content:read'), async (_req, res) => {
  try {
    const sets = await withDb(async (conn) => getQuestionSetRows(conn, { includeCounts: true }));
    res.json({ sets });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_sets_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/clone', requireAdmin, requirePermission('content:write'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    const cloned = await withDb(async (conn) => {
      const questionSet = await cloneQuestionSetWithChildren(conn, id, {
        name: req.body?.name,
        lifecycleStatus: QUESTION_SET_LIFECYCLE.DRAFT,
        importSource: 'clone'
      });
      await writeAdminAudit(conn, {
        action: 'admin_question_set_cloned',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { sourceId: id, clonedId: questionSet.id }
      });
      return questionSet;
    });
    res.json({ ok: true, questionSet: cloned });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_clone_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/publish', requireAdmin, requirePermission('content:publish'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    await withDb(async (conn) => {
      const questionSet = await loadQuestionSet(conn, id);
      const groupId = questionSet.versionGroupId || questionSet.id;
      if (await hasQuestionSetVersionColumns(conn)) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SETS
              SET LIFECYCLE_STATUS = CASE WHEN QUESTION_SET_ID = ? THEN '${QUESTION_SET_LIFECYCLE.PUBLISHED}' ELSE CASE WHEN VERSION_GROUP_ID = ? THEN '${QUESTION_SET_LIFECYCLE.ARCHIVED}' ELSE LIFECYCLE_STATUS END END,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_SET_ID = ? OR VERSION_GROUP_ID = ?`,
          [id, groupId, id, groupId]
        );
      }
      await writeAdminAudit(conn, {
        action: 'admin_question_set_published',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { questionSetId: id, versionGroupId: groupId }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_publish_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/archive', requireAdmin, requirePermission('content:publish'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    await withDb(async (conn) => {
      const rows = await execQuery(conn, 'SELECT IS_ACTIVE FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      if (!rows.length) throw new Error('question_set_not_found');
      if (Boolean(rows[0].IS_ACTIVE)) throw new Error('cannot_archive_active_set');
      if (await hasQuestionSetVersionColumns(conn)) {
        await execQuery(conn, `UPDATE QUESTION_SETS SET LIFECYCLE_STATUS = '${QUESTION_SET_LIFECYCLE.ARCHIVED}', UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE QUESTION_SET_ID = ?`, [id]);
      }
      await writeAdminAudit(conn, {
        action: 'admin_question_set_archived',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { questionSetId: id }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    const status = ['question_set_not_found', 'cannot_archive_active_set'].includes(err.message) ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.get('/api/admin/question-sets/:id/export.json', requireAdmin, requirePermission('results:export'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    const envelope = await withDb(async (conn) => {
      const questionSet = await loadQuestionSet(conn, id, { allowEmpty: true });
      return buildSignedEnvelopeLocal({
        exportedAt: new Date().toISOString(),
        questionSet
      });
    });
    res.json({ ok: true, ...envelope });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_export_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets', requireAdmin, requirePermission('content:write'), async (req, res) => {
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  if (!name) return res.status(400).json({ error: 'name_required' });

  try {
    const created = await withDb(async (conn) => {
      const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
      if (hasVersionColumns) {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SETS
            (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS, NUM_QUESTIONS, VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID, IMPORT_SOURCE, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, FALSE, 45, 80, TRUE, '${EXAM_MODE.GRADED}', FALSE, TRUE, NULL, NULL, 1, '${QUESTION_SET_LIFECYCLE.DRAFT}', NULL, 'manual', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [name, description || null]
        );
      } else {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SETS
            (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, NUM_QUESTIONS, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, FALSE, 45, 80, TRUE, NULL, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [name, description || null]
        );
      }
      const rows = await execQuery(
        conn,
        `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, NUM_QUESTIONS, CREATED_AT, UPDATED_AT
           FROM QUESTION_SETS
          WHERE NAME = ?
          ORDER BY QUESTION_SET_ID DESC
          LIMIT 1`,
        [name]
      );
      const createdId = Number(rows[0].QUESTION_SET_ID);
      if (hasVersionColumns) {
        await execQuery(conn, 'UPDATE QUESTION_SETS SET VERSION_GROUP_ID = COALESCE(VERSION_GROUP_ID, QUESTION_SET_ID), UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE QUESTION_SET_ID = ?', [createdId]);
      }
      const normalized = await loadQuestionSet(conn, createdId, { allowEmpty: true });
      await writeAdminAudit(conn, {
        action: 'admin_question_set_created',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { name }
      });
      return normalized;
    });
    res.json({ ok: true, questionSet: created });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_create_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/config', requireAdmin, requirePermission('content:write'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  const durationMinutes = Math.max(1, Math.min(Number(req.body?.durationMinutes) || 45, 240));
  const passPct = Math.max(1, Math.min(Number(req.body?.passPct) || 80, 100));
  const proctorEnabled = req.body?.proctorEnabled !== false;
  const examMode = String(req.body?.examMode || EXAM_MODE.GRADED).toUpperCase() === EXAM_MODE.PRACTICE ? EXAM_MODE.PRACTICE : EXAM_MODE.GRADED;
  const showCorrectAnswers = examMode === EXAM_MODE.PRACTICE && req.body?.showCorrectAnswers !== false;
  const countsTowardResults = examMode === EXAM_MODE.PRACTICE ? false : req.body?.countsTowardResults !== false;
  const numQuestionsRaw = req.body?.numQuestions;
  const numQuestions = numQuestionsRaw == null || numQuestionsRaw === '' ? null : Math.max(1, Number(numQuestionsRaw));
  if (!name) return res.status(400).json({ error: 'name_required' });

  try {
    await withDb(async (conn) => {
      if (await hasQuestionSetModeColumns(conn)) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SETS
              SET NAME = ?,
                  DESCRIPTION = ?,
                  DURATION_MINUTES = ?,
                  PASS_PCT = ?,
                  PROCTOR_ENABLED = ?,
                  EXAM_MODE = ?,
                  SHOW_CORRECT_ANSWERS = ?,
                  COUNTS_TOWARD_RESULTS = ?,
                  NUM_QUESTIONS = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_SET_ID = ?`,
          [name, description || null, durationMinutes, passPct, proctorEnabled ? 1 : 0, examMode, showCorrectAnswers ? 1 : 0, countsTowardResults ? 1 : 0, numQuestions, id]
        );
      } else {
        await execQuery(
          conn,
          `UPDATE QUESTION_SETS
              SET NAME = ?,
                  DESCRIPTION = ?,
                  DURATION_MINUTES = ?,
                  PASS_PCT = ?,
                  PROCTOR_ENABLED = ?,
                  NUM_QUESTIONS = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_SET_ID = ?`,
          [name, description || null, durationMinutes, passPct, proctorEnabled ? 1 : 0, numQuestions, id]
        );
      }
      clearQuestionSetCache(id);
      await writeAdminAudit(conn, {
        action: 'admin_question_set_config_updated',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { questionSetId: id, name, durationMinutes, passPct, proctorEnabled, examMode, showCorrectAnswers, countsTowardResults, numQuestions }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_config_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/activate', requireAdmin, requirePermission('content:publish'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    await withDb(async (conn) => {
      const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
      let versionGroupId = null;
      if (hasVersionColumns) {
        const rows = await execQuery(conn, 'SELECT VERSION_GROUP_ID FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
        versionGroupId = rows.length ? Number(rows[0].VERSION_GROUP_ID || id) : id;
      }
      await execQuery(conn, 'UPDATE QUESTION_SETS SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_UTCTIMESTAMP');
      await execQuery(
        conn,
        'UPDATE QUESTION_SETS SET IS_ACTIVE = TRUE, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE QUESTION_SET_ID = ?',
        [id]
      );
      if (hasVersionColumns) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SETS
              SET LIFECYCLE_STATUS = CASE WHEN QUESTION_SET_ID = ? THEN '${QUESTION_SET_LIFECYCLE.PUBLISHED}' WHEN VERSION_GROUP_ID = ? THEN '${QUESTION_SET_LIFECYCLE.ARCHIVED}' ELSE LIFECYCLE_STATUS END,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_SET_ID = ? OR VERSION_GROUP_ID = ?`,
          [id, versionGroupId, id, versionGroupId]
        );
      }
      clearQuestionSetCache();
      await writeAdminAudit(conn, {
        action: 'admin_question_set_activated',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { questionSetId: id }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_activate_failed', message: err.message });
  }
});

app.delete('/api/admin/question-sets/:id', requireAdmin, requirePermission('content:publish'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    await withDb(async (conn) => {
      const rows = await execQuery(conn, 'SELECT IS_ACTIVE FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      if (!rows.length) throw new Error('question_set_not_found');
      if (rows[0].IS_ACTIVE) throw new Error('cannot_delete_active_question_set');
      await execQuery(conn, 'DELETE FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      clearQuestionSetCache(id);
      await writeAdminAudit(conn, {
        action: 'admin_question_set_deleted',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { questionSetId: id }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    const status =
      err.message === 'question_set_not_found' ? 404 :
      err.message === 'cannot_delete_active_question_set' ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.get('/api/admin/question-sets/:id/questions', requireAdmin, requirePermission('content:read'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    const payload = await withDb(async (conn) => {
      const questionSet = await loadQuestionSet(conn, id, { allowEmpty: true });
      return {
        questionSet: {
          id: questionSet.id,
          name: questionSet.name,
          description: questionSet.description,
          isActive: questionSet.isActive,
          versionGroupId: questionSet.versionGroupId,
          versionNumber: questionSet.versionNumber,
          lifecycleStatus: questionSet.lifecycleStatus,
          parentQuestionSetId: questionSet.parentQuestionSetId,
          importSource: questionSet.importSource,
          examMode: questionSet.examMode,
          showCorrectAnswers: questionSet.showCorrectAnswers,
          countsTowardResults: questionSet.countsTowardResults,
          proctorEnabled: questionSet.proctorEnabled,
          durationMinutes: questionSet.durationMinutes,
          passPct: questionSet.passPct,
          numQuestions: questionSet.numQuestions
        },
        questions: questionSet.questions.map((question) => ({
          id: question.questionId,
          qNum: question.questionIndex,
          stem: question.stem,
          note: question.note || '',
          opts: question.opts,
          correctIndices: question.answer,
          multi: Boolean(question.multi),
          sectionId: question.sectionId
        }))
      };
    });
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_questions_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:setId/questions', requireAdmin, requirePermission('content:write'), async (req, res) => {
  const setId = Number(req.params.setId);
  if (!Number.isInteger(setId)) return res.status(400).json({ error: 'invalid_question_set_id' });

  const questionId = req.body?.id == null || req.body?.id === '' ? null : Number(req.body.id);
  const qNum = Number(req.body?.qNum);
  const stem = String(req.body?.stem || '').trim();
  const note = String(req.body?.note || '').trim();
  const opts = Array.isArray(req.body?.opts) ? req.body.opts.map((item) => String(item || '').trim()).filter(Boolean) : [];
  const correctIndices = Array.isArray(req.body?.correctIndices) ? req.body.correctIndices.map((item) => Number(item)).filter((n) => Number.isInteger(n) && n >= 0) : [];
  const multi = Boolean(req.body?.multi);
  const sectionId = req.body?.sectionId == null || req.body?.sectionId === '' ? null : Number(req.body.sectionId);

  if (!Number.isInteger(qNum) || qNum < 1) return res.status(400).json({ error: 'invalid_question_number' });
  if (!stem) return res.status(400).json({ error: 'stem_required' });
  if (opts.length < 2) return res.status(400).json({ error: 'at_least_two_options_required' });
  if (!correctIndices.length) return res.status(400).json({ error: 'correct_indices_required' });
  if (!multi && correctIndices.length !== 1) return res.status(400).json({ error: 'single_select_requires_exactly_one_correct_option' });
  if (correctIndices.some((idx) => idx >= opts.length)) return res.status(400).json({ error: 'correct_index_out_of_range' });

  try {
    await withDb(async (conn) => {
      if (sectionId != null) {
        const sections = await execQuery(conn, 'SELECT SECTION_ID FROM QUESTION_SECTIONS WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?', [sectionId, setId]);
        if (!sections.length) throw new Error('section_not_found');
      }
      if (questionId != null) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SET_QUESTIONS
              SET QUESTION_INDEX = ?,
                  STEM = ?,
                  NOTE = ?,
                  OPTS_JSON = ?,
                  ANSWER_JSON = ?,
                  MULTI = ?,
                  SECTION_ID = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_ID = ?
              AND QUESTION_SET_ID = ?`,
          [qNum, stem, note || null, JSON.stringify(opts), JSON.stringify(correctIndices), multi ? 1 : 0, sectionId, questionId, setId]
        );
      } else {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SET_QUESTIONS
            (QUESTION_SET_ID, SECTION_ID, QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [setId, sectionId, qNum, stem, note || null, JSON.stringify(opts), JSON.stringify(correctIndices), multi ? 1 : 0]
        );
      }
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: questionId != null ? 'admin_question_updated' : 'admin_question_created',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { questionSetId: setId, questionId, qNum }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_save_failed', message: err.message });
  }
});

app.delete('/api/admin/question-sets/:setId/questions/:questionId', requireAdmin, requirePermission('content:write'), async (req, res) => {
  const setId = Number(req.params.setId);
  const questionId = Number(req.params.questionId);
  if (!Number.isInteger(setId) || !Number.isInteger(questionId)) return res.status(400).json({ error: 'invalid_identifier' });

  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'DELETE FROM QUESTION_SET_QUESTIONS WHERE QUESTION_ID = ? AND QUESTION_SET_ID = ?', [questionId, setId]);
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: 'admin_question_deleted',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { questionSetId: setId, questionId }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_delete_failed', message: err.message });
  }
});

app.get('/api/admin/question-sets/:setId/sections', requireAdmin, requirePermission('content:read'), async (req, res) => {
  const setId = Number(req.params.setId);
  if (!Number.isInteger(setId)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    const sections = await withDb(async (conn) => {
      const rows = await execQuery(
        conn,
        `SELECT s.SECTION_ID, s.QUESTION_SET_ID, s.NAME, s.DESCRIPTION, s.DISPLAY_ORDER, s.DRAW_COUNT,
                s.CREATED_AT, s.UPDATED_AT, COUNT(q.QUESTION_ID) AS QUESTION_COUNT
           FROM QUESTION_SECTIONS s
           LEFT JOIN QUESTION_SET_QUESTIONS q ON q.SECTION_ID = s.SECTION_ID
          WHERE s.QUESTION_SET_ID = ?
          GROUP BY s.SECTION_ID, s.QUESTION_SET_ID, s.NAME, s.DESCRIPTION, s.DISPLAY_ORDER, s.DRAW_COUNT, s.CREATED_AT, s.UPDATED_AT
          ORDER BY s.DISPLAY_ORDER ASC, s.SECTION_ID ASC`,
        [setId]
      );
      return rows.map((row) => ({
        id: Number(row.SECTION_ID),
        questionSetId: Number(row.QUESTION_SET_ID),
        name: String(row.NAME || ''),
        description: row.DESCRIPTION || '',
        displayOrder: Number(row.DISPLAY_ORDER || 0),
        drawCount: row.DRAW_COUNT == null ? null : Number(row.DRAW_COUNT),
        questionCount: Number(row.QUESTION_COUNT || 0)
      }));
    });
    res.json({ sections });
  } catch (err) {
    res.status(500).json({ error: 'admin_sections_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:setId/sections', requireAdmin, requirePermission('content:write'), async (req, res) => {
  const setId = Number(req.params.setId);
  if (!Number.isInteger(setId)) return res.status(400).json({ error: 'invalid_question_set_id' });

  const sectionId = req.body?.id == null || req.body?.id === '' ? null : Number(req.body.id);
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  const displayOrder = Number(req.body?.displayOrder) || 0;
  const drawCountRaw = req.body?.drawCount;
  const drawCount = drawCountRaw == null || drawCountRaw === '' ? null : Math.max(1, Number(drawCountRaw));
  if (!name) return res.status(400).json({ error: 'name_required' });

  try {
    await withDb(async (conn) => {
      if (sectionId != null) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SECTIONS
              SET NAME = ?, DESCRIPTION = ?, DISPLAY_ORDER = ?, DRAW_COUNT = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?`,
          [name, description || null, displayOrder, drawCount, sectionId, setId]
        );
      } else {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SECTIONS
            (QUESTION_SET_ID, NAME, DESCRIPTION, DISPLAY_ORDER, DRAW_COUNT, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [setId, name, description || null, displayOrder, drawCount]
        );
      }
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: sectionId != null ? 'admin_section_updated' : 'admin_section_created',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { questionSetId: setId, sectionId, name }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_section_save_failed', message: err.message });
  }
});

app.delete('/api/admin/question-sets/:setId/sections/:sectionId', requireAdmin, requirePermission('content:write'), async (req, res) => {
  const setId = Number(req.params.setId);
  const sectionId = Number(req.params.sectionId);
  if (!Number.isInteger(setId) || !Number.isInteger(sectionId)) return res.status(400).json({ error: 'invalid_identifier' });

  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'UPDATE QUESTION_SET_QUESTIONS SET SECTION_ID = NULL, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?', [sectionId, setId]);
      await execQuery(conn, 'DELETE FROM QUESTION_SECTIONS WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?', [sectionId, setId]);
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: 'admin_section_deleted',
        actor: ROLES.ADMIN,
        clientIp: getClientIp(req),
        details: { questionSetId: setId, sectionId }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_section_delete_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/upload/preview', requireAdmin, requirePermission('imports:write'), async (req, res) => {
  const questions = Array.isArray(req.body?.questions) ? req.body.questions : [];
  const validation = validateQuestionUploadEntries(questions);
  if (!validation.ok) {
    return res.status(400).json({ error: 'invalid_question_upload', errors: validation.errors, warnings: validation.warnings });
  }
  try {
    const preview = await withDb(async (conn) => {
      const dbRows = await execQuery(conn, 'SELECT STEM FROM QUESTION_SET_QUESTIONS');
      const existing = new Set(dbRows.map((row) => String(row.STEM || '').trim().toLowerCase()).filter(Boolean));
      const duplicateStems = validation.normalized
        .filter((question) => existing.has(String(question.stem || '').trim().toLowerCase()))
        .slice(0, 50)
        .map((question) => question.stem);
      return {
        count: validation.normalized.length,
        duplicatesAgainstDatabase: duplicateStems,
        warnings: validation.warnings,
        sample: validation.normalized.slice(0, 3)
      };
    });
    res.json({ ok: true, ...preview });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_upload_preview_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/upload', requireAdmin, requirePermission('imports:write'), async (req, res) => {
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  const questions = Array.isArray(req.body?.questions) ? req.body.questions : [];
  if (!name) return res.status(400).json({ error: 'name_required' });
  if (!questions.length) return res.status(400).json({ error: 'questions_required' });
  const validation = validateQuestionUploadEntries(questions);
  if (!validation.ok) {
    return res.status(400).json({
      error: 'invalid_question_upload',
      errors: validation.errors,
      warnings: validation.warnings
    });
  }

  try {
    const result = await withDb(async (conn) => {
      const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
      if (hasVersionColumns) {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SETS
            (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS, NUM_QUESTIONS, VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID, IMPORT_SOURCE, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, FALSE, 45, 80, TRUE, '${EXAM_MODE.GRADED}', FALSE, TRUE, NULL, NULL, 1, '${QUESTION_SET_LIFECYCLE.DRAFT}', NULL, 'csv_upload', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [name, description || null]
        );
      } else {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SETS
            (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, NUM_QUESTIONS, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, FALSE, 45, 80, TRUE, NULL, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [name, description || null]
        );
      }
      const createdRows = await execQuery(
        conn,
        `SELECT QUESTION_SET_ID
           FROM QUESTION_SETS
          WHERE NAME = ?
          ORDER BY QUESTION_SET_ID DESC
          LIMIT 1`,
        [name]
      );
      const setId = Number(createdRows[0].QUESTION_SET_ID);
      if (hasVersionColumns) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SETS
              SET VERSION_GROUP_ID = COALESCE(VERSION_GROUP_ID, QUESTION_SET_ID),
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_SET_ID = ?`,
          [setId]
        );
      }

      for (const entry of validation.normalized) {
        const { qNum, stem, note, opts, correctIndices, multi } = entry;
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SET_QUESTIONS
            (QUESTION_SET_ID, QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [setId, qNum, stem, note || null, JSON.stringify(opts), JSON.stringify(correctIndices), multi ? 1 : 0]
        );
      }
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: 'admin_question_set_uploaded',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { questionSetId: setId, name, count: questions.length }
      });
      return { setId, count: questions.length };
    });
    res.json({ ok: true, questionSetId: result.setId, count: result.count });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_upload_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/rollback-import', requireAdmin, requirePermission('imports:rollback'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    await withDb(async (conn) => {
      const rows = await execQuery(conn, 'SELECT QUESTION_SET_ID, IMPORT_SOURCE, IS_ACTIVE FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      if (!rows.length) throw new Error('question_set_not_found');
      if (Boolean(rows[0].IS_ACTIVE)) throw new Error('cannot_rollback_active_set');
      const hasVersionColumns = await hasQuestionSetVersionColumns(conn);
      if (hasVersionColumns && String(rows[0].IMPORT_SOURCE || '') !== 'csv_upload') throw new Error('rollback_not_allowed');
      const resultCountRows = await execQuery(conn, `SELECT COUNT(*) AS CNT FROM EXAM_RESULTS WHERE JSON_VALUE(RESULT_JSON, '$.questionSetId') = ?`, [String(id)]);
      if (Number(resultCountRows?.[0]?.CNT || 0) > 0) throw new Error('rollback_has_results');
      await execQuery(conn, 'DELETE FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      clearQuestionSetCache();
      await writeAdminAudit(conn, {
        action: 'admin_question_set_import_rolled_back',
        actor: req.adminRole,
        clientIp: getClientIp(req),
        details: { questionSetId: id }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    const status = ['question_set_not_found', 'cannot_rollback_active_set', 'rollback_not_allowed', 'rollback_has_results'].includes(err.message) ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.get('/api/admin/export.csv', requireAdmin, requirePermission('results:export'), async (_req, res) => {
  try {
    const rows = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const clauses = [];
      const params = [];
      const questionSetId = _req.query?.questionSetId == null || _req.query.questionSetId === ''
        ? null
        : Number(_req.query.questionSetId);
      const statusFilter = String(_req.query?.status || '').trim().toLowerCase();
      const modeFilter = String(_req.query?.mode || '').trim().toUpperCase();
      const dateFrom = parseDateFilter(_req.query?.dateFrom, false);
      const dateTo = parseDateFilter(_req.query?.dateTo, true);
      if (hasDeletedAt) clauses.push('c.DELETED_AT IS NULL');
      if (Number.isInteger(questionSetId)) {
        clauses.push('c.QUESTION_SET_ID = ?');
        params.push(questionSetId);
      }
      if (['unused', 'active', 'completed'].includes(statusFilter)) {
        clauses.push('c.STATUS = ?');
        params.push(statusFilter);
      }
      if (modeFilter === EXAM_MODE.PRACTICE) {
        clauses.push(`COALESCE(JSON_VALUE(r.RESULT_JSON, '$.examMode'), '${EXAM_MODE.GRADED}') = '${EXAM_MODE.PRACTICE}'`);
      } else if (modeFilter === EXAM_MODE.GRADED) {
        clauses.push(`COALESCE(JSON_VALUE(r.RESULT_JSON, '$.examMode'), '${EXAM_MODE.GRADED}') = '${EXAM_MODE.GRADED}'`);
      }
      if (dateFrom) {
        clauses.push('r.SUBMITTED_AT >= ?');
        params.push(dateFrom);
      }
      if (dateTo) {
        clauses.push('r.SUBMITTED_AT <= ?');
        params.push(dateTo);
      }
      return execQuery(
        conn,
        `SELECT c.ACCESS_CODE, c.LABEL, ${hasNotes ? 'c.NOTES,' : `'' AS NOTES,`} c.STATUS,
                qs.NAME AS QUESTION_SET_NAME,
                r.SCORE, r.PCT, r.PASS, r.DURATION_SECS, r.TAB_SWITCHES, r.INCIDENT_COUNT, r.SUBMITTED_AT, r.RESULT_JSON
           FROM ACCESS_CODES c
           LEFT JOIN QUESTION_SETS qs ON qs.QUESTION_SET_ID = c.QUESTION_SET_ID
           LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
          ${clauses.length ? `WHERE ${clauses.join(' AND ')}` : ''}
          ORDER BY c.ACCESS_CODE ASC`,
        params
      );
    });

    const lines = ['Code,Seat,Notes,QuestionSet,Mode,Status,Score,Pct,Result,Duration,TabSwitches,Incidents,SubmittedAt'];
    for (const r of rows) {
      const parsedResult = parseJsonOrNull(r.RESULT_JSON);
      const countsTowardResults = parsedResult?.countsTowardResults !== false;
      const mode = parsedResult?.examMode === EXAM_MODE.PRACTICE ? 'Practice' : 'Graded';
      const resultLabel = !countsTowardResults || r.PASS === null || r.PASS === undefined ? '' : (r.PASS ? 'PASS' : 'FAIL');
      const duration = r.DURATION_SECS == null ? '' : `${Math.floor(r.DURATION_SECS / 60)}m ${String(r.DURATION_SECS % 60).padStart(2, '0')}s`;
      lines.push([
        toCsvCell(r.ACCESS_CODE),
        toCsvCell(r.LABEL || ''),
        toCsvCell(r.NOTES || ''),
        toCsvCell(r.QUESTION_SET_NAME || ''),
        toCsvCell(mode),
        toCsvCell(r.STATUS || ''),
        toCsvCell(countsTowardResults ? (r.SCORE ?? '') : ''),
        toCsvCell(countsTowardResults ? (r.PCT == null ? '' : `${r.PCT}%`) : ''),
        toCsvCell(resultLabel),
        toCsvCell(duration),
        toCsvCell(r.TAB_SWITCHES ?? ''),
        toCsvCell(r.INCIDENT_COUNT ?? ''),
        toCsvCell(r.SUBMITTED_AT ? new Date(r.SUBMITTED_AT).toISOString() : '')
      ].join(','));
    }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="Academy_Exam_App_Results.csv"');
    res.send(lines.join('\n'));
  } catch (err) {
    appLog('error', 'admin_export_failed', { message: err.message });
    res.status(500).json({ error: 'admin_export_failed', message: err.message });
  }
});

app.get('/client-app.js', (_req, res) => {
  // Legacy single-file bundle, kept as a 404 shim for old clients
  // that may have the URL cached. The active app loads /client/*.js.
  res.status(404).send('Not found');
});

app.get('/shared/constants.js', (_req, res) => {
  res.type('application/javascript').sendFile(path.join(__dirname, 'shared', 'constants.js'));
});

// Serve any /client/*.js file (the refactored SPA modules).
// Reject paths that try to escape the client/ directory.
app.get(/^\/client\/([A-Za-z0-9_-]+\.js)$/, (req, res) => {
  const fileName = req.params[0];
  const filePath = path.join(__dirname, 'client', fileName);
  if (!filePath.startsWith(path.join(__dirname, 'client') + path.sep)) {
    return res.status(400).send('Bad path');
  }
  if (!fs.existsSync(filePath)) return res.status(404).send('Not found');
  res.type('application/javascript').sendFile(filePath);
});

app.get('/favicon.svg', (_req, res) => {
  res.type('image/svg+xml').sendFile(FAVICON_PATH);
});

app.get('/', (_req, res) => {
  res.sendFile(INDEX_PATH);
});

app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api/')) return next();
  if (req.path === '/shared/constants.js' && fs.existsSync(path.join(__dirname, 'shared', 'constants.js'))) return res.type('application/javascript').sendFile(path.join(__dirname, 'shared', 'constants.js'));
  if (req.path === '/favicon.svg' && fs.existsSync(FAVICON_PATH)) return res.type('image/svg+xml').sendFile(FAVICON_PATH);
  if (fs.existsSync(INDEX_PATH)) return res.sendFile(INDEX_PATH);
  return res.status(404).send('Not found');
});

app.use((err, _req, res, _next) => {
  appLog('error', 'server_error', { requestId: _req.requestId, message: err.message });
  res.status(500).json({ error: 'server_error', message: err.message, requestId: _req.requestId });
});

function startBackgroundJobs() {
  if (!HAS_DB_CONFIG || !AUTO_CLEAR_STALE_SESSIONS || _runtimeState.staleSessionSweepTimer) return;
  _runtimeState.sweeperEnabled = true;
  _runtimeState.sweeperStartedAt = Date.now();
  _runtimeState.staleSessionSweepTimer = setInterval(() => {
    const startedAt = Date.now();
    withDb(async (conn) => clearStaleSessionsWithConn(conn))
      .then((cleared) => {
        _runtimeState.sweeperTickCount += 1;
        _runtimeState.sweeperLastTickAt = Date.now();
        _runtimeState.sweeperLastDurationMs = Date.now() - startedAt;
        _runtimeState.sweeperLastCleared = cleared;
        _runtimeState.sweeperLastError = null;
        _runtimeState.sweeperTotalCleared += cleared.length;
        // Always log on tick — silent ticks are how 7WGME9 happened.
        if (cleared.length) {
          appLog('info', 'stale_sessions_auto_cleared', {
            count: cleared.length,
            durationMs: _runtimeState.sweeperLastDurationMs,
            tickNumber: _runtimeState.sweeperTickCount
          });
        } else {
          appLog('debug', 'stale_session_sweep_tick', {
            durationMs: _runtimeState.sweeperLastDurationMs,
            tickNumber: _runtimeState.sweeperTickCount
          });
        }
      })
      .catch((err) => {
        _runtimeState.sweeperTickCount += 1;
        _runtimeState.sweeperLastTickAt = Date.now();
        _runtimeState.sweeperLastDurationMs = Date.now() - startedAt;
        _runtimeState.sweeperLastError = { message: err.message, at: Date.now() };
        appLog('warn', 'stale_session_sweep_failed', {
          message: err.message,
          tickNumber: _runtimeState.sweeperTickCount,
          durationMs: _runtimeState.sweeperLastDurationMs
        });
      });
  }, STALE_SESSION_SWEEP_MINUTES * 60 * 1000);
  appLog('info', 'stale_session_sweeper_started', {
    intervalMinutes: STALE_SESSION_SWEEP_MINUTES,
    thresholdMinutes: STALE_SESSION_MINUTES,
    autoClearEnabled: AUTO_CLEAR_STALE_SESSIONS,
    hasDbConfig: HAS_DB_CONFIG
  });
}

function stopBackgroundJobs() {
  if (_runtimeState.staleSessionSweepTimer) {
    clearInterval(_runtimeState.staleSessionSweepTimer);
    _runtimeState.staleSessionSweepTimer = null;
  }
  _runtimeState.sweeperEnabled = false;
}

function startServer(port = PORT) {
  const summary = startupSummary();
  summary.errors.forEach((error) => appLog('error', 'startup_error', { error }));
  summary.warnings.forEach((warning) => appLog('warn', 'startup_warning', { warning }));
  if (STARTUP_STRICT && summary.errors.length) {
    const error = new Error(`Startup validation failed: ${summary.errors.join(' | ')}`);
    appLog('error', 'startup_validation_failed', { message: error.message });
    throw error;
  }
  startBackgroundJobs();
  return app.listen(port, () => {
    console.log(`Academy Exam App server listening on port ${port}`);
  });
}

module.exports = {
  app,
  startServer,
  stopBackgroundJobs,
  normalizeExamTitle,
  validateQuestionUploadEntries,
  startupSummary,
  // startupErrors is exported for tests so they can assert that
  // an invalid signing-key config is surfaced in the boot path
  // (rather than only on first use). Not part of the public API.
  startupErrors,
  getSweeperStatus,
  signPayload,
  buildSignedEnvelope: buildSignedEnvelopeLocal,
  getSigningKeyMap,
  // Pure helpers used by both runtime + startup + tests.
  parseSigningConfig,
  validateSigningConfig
};

if (require.main === module) {
  startServer();
}
