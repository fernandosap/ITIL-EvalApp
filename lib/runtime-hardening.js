'use strict';

const crypto = require('node:crypto');

const SESSION_START_MAX = Math.max(1, Number(process.env.SESSION_START_MAX || 10));
const SESSION_START_WINDOW_MS = Math.max(1000, Number(process.env.SESSION_START_WINDOW_MS || 10 * 60 * 1000));
const SUBMISSION_CACHE_TTL_MS = Math.max(60_000, Number(process.env.SUBMISSION_CACHE_TTL_MS || 2 * 60 * 60 * 1000));
const REVOCATION_REFRESH_MS = 30_000;

const state = {
  installed: false,
  originalConsoleLog: console.log,
  sessionStartBuckets: new Map(),
  submitLocks: new Set(),
  submissionResults: new Map(),
  questionMutationLocked: false,
  xsuaaRevokedAt: 0,
  xsuaaSessions: new Map(),
  revocationFetchedAt: 0
};

function secureMathRandom() {
  const bytes = crypto.randomBytes(6);
  let value = 0;
  for (const byte of bytes) value = (value * 256) + byte;
  return value / 281474976710656;
}

function clientIp(req) {
  const xff = String(req.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
  return xff || String(req.socket?.remoteAddress || 'unknown');
}

function parseCookies(header) {
  const result = {};
  for (const part of String(header || '').split(';')) {
    const idx = part.indexOf('=');
    if (idx <= 0) continue;
    const key = part.slice(0, idx).trim();
    const raw = part.slice(idx + 1).trim();
    try { result[key] = decodeURIComponent(raw); } catch (_e) { result[key] = raw; }
  }
  return result;
}

function decodeJwtPayload(token) {
  try {
    const parts = String(token || '').split('.');
    if (parts.length !== 3) return null;
    return JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
  } catch (_e) { return null; }
}

function decodeLegacyAdminIssuedAt(token) {
  try {
    const parts = Buffer.from(String(token || ''), 'base64url').toString('utf8').split(':');
    if (parts.length !== 5) return 0;
    const issuedAt = Number(parts[3]);
    return Number.isFinite(issuedAt) ? issuedAt : 0;
  } catch (_e) { return 0; }
}

function dbConfig() {
  const schema = String(process.env.HANA_SCHEMA || 'ITIL_EXAM');
  if (!process.env.HANA_HOST || !process.env.HANA_USER || !process.env.HANA_PASSWORD) return null;
  if (!/^[A-Za-z0-9_]+$/.test(schema)) return null;
  return {
    serverNode: `${process.env.HANA_HOST}:${process.env.HANA_PORT || '443'}`,
    uid: process.env.HANA_USER,
    pwd: process.env.HANA_PASSWORD,
    encrypt: String(process.env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
    sslValidateCertificate: String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'true').toLowerCase() === 'true',
    schema
  };
}

function exec(conn, sql, params = []) {
  return new Promise((resolve, reject) => {
    conn.exec(sql, params, (err, rows) => err ? reject(err) : resolve(rows || []));
  });
}

async function withHana(fn) {
  const cfg = dbConfig();
  if (!cfg) return null;
  const hana = require('@sap/hana-client');
  const conn = hana.createConnection();
  conn.connect(cfg);
  try {
    await exec(conn, `SET SCHEMA "${cfg.schema}"`);
    return await fn(conn);
  } finally {
    try { conn.disconnect(); } catch (_e) { /* ignore */ }
  }
}

async function refreshPersistentRevocation() {
  const now = Date.now();
  if (state.revocationFetchedAt && now - state.revocationFetchedAt < REVOCATION_REFRESH_MS) return;
  state.revocationFetchedAt = now;
  try {
    const rows = await withHana((conn) => exec(conn,
      "SELECT SETTING_VALUE FROM APP_SETTINGS WHERE SETTING_KEY = 'ADMIN_TOKEN_NOT_BEFORE'"));
    const value = Number(rows?.[0]?.SETTING_VALUE || 0);
    if (Number.isFinite(value) && value > state.xsuaaRevokedAt) state.xsuaaRevokedAt = value;
  } catch (_e) {
    // Best-effort: server auth will still perform its own DB-backed checks.
  }
}

function shouldBlockRevokedAdminRequest(req) {
  if (!state.xsuaaRevokedAt) return false;
  if (req.path === '/api/admin/logout') return false;
  if (!String(req.path || '').startsWith('/api/admin/')) return false;

  const auth = String(req.headers?.authorization || '').trim();
  if (auth.startsWith('Bearer ')) {
    const issuedAtMs = Number(decodeJwtPayload(auth.slice(7).trim())?.iat || 0) * 1000;
    return !issuedAtMs || issuedAtMs <= state.xsuaaRevokedAt;
  }

  const opaqueId = parseCookies(req.headers?.cookie).xsuaa_session;
  if (opaqueId) {
    const issuedAt = Number(state.xsuaaSessions.get(opaqueId) || 0);
    return !issuedAt || issuedAt <= state.xsuaaRevokedAt;
  }

  const legacyToken = String(req.headers?.['x-admin-token'] || '').trim();
  if (legacyToken) {
    const issuedAt = decodeLegacyAdminIssuedAt(legacyToken);
    return !issuedAt || issuedAt <= state.xsuaaRevokedAt;
  }
  return false;
}

function sessionStartGuard(req, res, next) {
  const now = Date.now();
  const code = String(req.body?.code || '').trim().toUpperCase();
  const key = `${clientIp(req)}:${code || 'no-code'}`;
  let entry = state.sessionStartBuckets.get(key);
  if (!entry || entry.resetAt <= now) {
    entry = { count: 0, resetAt: now + SESSION_START_WINDOW_MS };
    state.sessionStartBuckets.set(key, entry);
  }
  entry.count += 1;
  if (state.sessionStartBuckets.size > 5000) {
    for (const [bucketKey, value] of state.sessionStartBuckets) {
      if (value.resetAt <= now) state.sessionStartBuckets.delete(bucketKey);
    }
  }
  if (entry.count > SESSION_START_MAX) {
    res.setHeader('Retry-After', Math.max(1, Math.ceil((entry.resetAt - now) / 1000)));
    return res.status(429).json({ error: 'too_many_attempts' });
  }
  next();
}

function submitGuard(req, res, next) {
  const token = String(req.headers?.['x-exam-token'] || '').trim();
  if (!token) return next();
  const key = crypto.createHash('sha256').update(token).digest('hex');
  const now = Date.now();
  const cached = state.submissionResults.get(key);
  if (cached && cached.expiresAt > now) return res.json(cached.body);
  if (cached) state.submissionResults.delete(key);
  if (state.submitLocks.has(key)) return res.status(409).json({ error: 'submission_in_progress' });

  state.submitLocks.add(key);
  const originalJson = res.json.bind(res);
  res.json = function hardenedSubmitJson(body) {
    if (body?.ok === true && body?.result) {
      state.submissionResults.set(key, { body, expiresAt: Date.now() + SUBMISSION_CACHE_TTL_MS });
      if (state.submissionResults.size > 5000) {
        for (const [cacheKey, entry] of state.submissionResults) {
          if (entry.expiresAt <= Date.now()) state.submissionResults.delete(cacheKey);
        }
      }
    }
    return originalJson(body);
  };
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    state.submitLocks.delete(key);
  };
  res.once('finish', release);
  res.once('close', release);
  next();
}

async function activationReadiness(req) {
  const match = String(req.path || '').match(/^\/api\/admin\/question-sets\/(\d+)\/activate$/);
  if (!match) return { ok: true, previousActiveId: null };
  const targetId = Number(match[1]);
  try {
    const result = await withHana(async (conn) => {
      const target = await exec(conn,
        `SELECT qs.QUESTION_SET_ID, COUNT(q.QUESTION_ID) AS QUESTION_COUNT
           FROM QUESTION_SETS qs
           LEFT JOIN QUESTION_SET_QUESTIONS q ON q.QUESTION_SET_ID = qs.QUESTION_SET_ID
          WHERE qs.QUESTION_SET_ID = ?
          GROUP BY qs.QUESTION_SET_ID`, [targetId]);
      const active = await exec(conn,
        'SELECT QUESTION_SET_ID FROM QUESTION_SETS WHERE IS_ACTIVE = TRUE ORDER BY QUESTION_SET_ID LIMIT 1');
      return { target, active };
    });
    if (!result?.target?.length) return { ok: false, status: 404, error: 'question_set_not_found' };
    if (Number(result.target[0].QUESTION_COUNT || 0) < 1) return { ok: false, status: 409, error: 'question_set_has_no_questions' };
    return { ok: true, previousActiveId: result.active?.[0]?.QUESTION_SET_ID == null ? null : Number(result.active[0].QUESTION_SET_ID) };
  } catch (_e) {
    return { ok: true, previousActiveId: null };
  }
}

async function restoreActiveQuestionSet(id) {
  if (!Number.isInteger(id)) return;
  try {
    await withHana(async (conn) => {
      await exec(conn, 'UPDATE QUESTION_SETS SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_UTCTIMESTAMP');
      await exec(conn, 'UPDATE QUESTION_SETS SET IS_ACTIVE = TRUE, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE QUESTION_SET_ID = ?', [id]);
    });
  } catch (_e) { /* best effort */ }
}

async function questionSetMutationGuard(req, res, next) {
  if (state.questionMutationLocked) return res.status(409).json({ error: 'question_set_mutation_in_progress' });
  state.questionMutationLocked = true;
  const readiness = await activationReadiness(req);
  if (!readiness.ok) {
    state.questionMutationLocked = false;
    return res.status(readiness.status).json({ error: readiness.error });
  }
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    state.questionMutationLocked = false;
    if (res.statusCode >= 500 && Number.isInteger(readiness.previousActiveId)) {
      void restoreActiveQuestionSet(readiness.previousActiveId);
    }
  };
  res.once('finish', release);
  res.once('close', release);
  next();
}

function trackOAuthSessionCookie(req, res) {
  if (req.path !== '/oauth/callback') return;
  const original = res.setHeader.bind(res);
  res.setHeader = function hardenedSetHeader(name, value) {
    if (String(name).toLowerCase() === 'set-cookie') {
      const values = Array.isArray(value) ? value : [value];
      for (const item of values) {
        const match = String(item || '').match(/(?:^|;\s*)xsuaa_session=([^;]+)/);
        if (match?.[1]) state.xsuaaSessions.set(match[1], Date.now());
      }
    }
    return original(name, value);
  };
}

function hardeningMiddleware(req, res, next) {
  trackOAuthSessionCookie(req, res);
  const proceed = async () => {
    if (String(req.path || '').startsWith('/api/admin/')) await refreshPersistentRevocation();
    if (shouldBlockRevokedAdminRequest(req)) return res.status(401).json({ error: 'session_revoked' });
    if (req.method === 'POST' && req.path === '/api/admin/sessions/revoke-all') {
      res.once('finish', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          state.xsuaaRevokedAt = Date.now();
          state.revocationFetchedAt = Date.now();
          state.xsuaaSessions.clear();
        }
      });
    }
    next();
  };
  proceed().catch(() => next());
}

function injectBeforeFinalHandler(handlers, guard) {
  const copy = handlers.slice();
  copy.splice(Math.max(0, copy.length - 1), 0, guard);
  return copy;
}

function wrapRouteRegistration(app, methodName) {
  const original = app[methodName].bind(app);
  app[methodName] = function hardenedRoute(path, ...handlers) {
    let nextHandlers = handlers;
    const method = methodName.toUpperCase();
    if (method === 'POST' && path === '/api/session/start') nextHandlers = [sessionStartGuard, ...nextHandlers];
    if (method === 'POST' && path === '/api/submit') nextHandlers = [submitGuard, ...nextHandlers];
    if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)
        && typeof path === 'string'
        && path.startsWith('/api/admin/question-sets')) {
      nextHandlers = injectBeforeFinalHandler(nextHandlers, questionSetMutationGuard);
    }
    return original(path, ...nextHandlers);
  };
}

function installExpressWrapper() {
  const expressPath = require.resolve('express');
  const originalExpress = require(expressPath);
  if (originalExpress.__itilHardeningWrapped) return;
  function hardenedExpress(...args) {
    const app = originalExpress(...args);
    app.use(hardeningMiddleware);
    for (const method of ['post', 'put', 'patch', 'delete']) wrapRouteRegistration(app, method);
    return app;
  }
  Object.assign(hardenedExpress, originalExpress);
  Object.defineProperty(hardenedExpress, '__itilHardeningWrapped', { value: true });
  require.cache[expressPath].exports = hardenedExpress;
}

function installConsoleSanitizer() {
  console.log = function hardenedConsoleLog(...args) {
    if (typeof args[0] === 'string' && args[0].startsWith('{')) {
      try {
        const parsed = JSON.parse(args[0]);
        if (parsed?.event === 'slow_request' && typeof parsed.path === 'string') {
          parsed.path = parsed.path.split('?')[0];
          args[0] = JSON.stringify(parsed);
        }
      } catch (_e) { /* leave non-JSON logs untouched */ }
    }
    return state.originalConsoleLog(...args);
  };
}

function install() {
  if (state.installed) return;
  state.installed = true;
  Math.random = secureMathRandom;
  installConsoleSanitizer();
  installExpressWrapper();
}

function resetForTests() {
  state.sessionStartBuckets.clear();
  state.submitLocks.clear();
  state.submissionResults.clear();
  state.questionMutationLocked = false;
  state.xsuaaRevokedAt = 0;
  state.xsuaaSessions.clear();
  state.revocationFetchedAt = 0;
}

install();

module.exports = {
  install,
  secureMathRandom,
  parseCookies,
  decodeJwtPayload,
  decodeLegacyAdminIssuedAt,
  hardeningMiddleware,
  sessionStartGuard,
  submitGuard,
  questionSetMutationGuard,
  resetForTests,
  _state: state
};
