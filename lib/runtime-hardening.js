'use strict';

const crypto = require('node:crypto');

const SESSION_START_MAX = Math.max(1, Number(process.env.SESSION_START_MAX || 10));
const SESSION_START_WINDOW_MS = Math.max(1000, Number(process.env.SESSION_START_WINDOW_MS || 10 * 60 * 1000));
const SUBMISSION_CACHE_TTL_MS = Math.max(60_000, Number(process.env.SUBMISSION_CACHE_TTL_MS || 2 * 60 * 60 * 1000));
const REVOCATION_REFRESH_MS = 30_000;
const FALLBACK_LOGIN_MAX = 8;
const FALLBACK_LOGIN_WINDOW_MS = 15 * 60 * 1000;
const ADMIN_TTL_MS = 8 * 60 * 60 * 1000;

const state = {
  installed: false,
  originalConsoleLog: console.log,
  sessionStartBuckets: new Map(),
  fallbackLoginBuckets: new Map(),
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
    connection: {
      serverNode: `${process.env.HANA_HOST}:${process.env.HANA_PORT || '443'}`,
      uid: process.env.HANA_USER,
      pwd: process.env.HANA_PASSWORD,
      encrypt: String(process.env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
      sslValidateCertificate: String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'true').toLowerCase() === 'true'
    },
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
  conn.connect(cfg.connection);
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
    // Best-effort. The server's own auth still applies its normal checks.
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

function incrementWindowBucket(map, key, now, windowMs) {
  let entry = map.get(key);
  if (!entry || entry.resetAt <= now) {
    entry = { count: 0, resetAt: now + windowMs };
    map.set(key, entry);
  }
  entry.count += 1;
  return entry;
}

function sessionStartGuard(req, res, next) {
  const now = Date.now();
  const ip = clientIp(req);
  const code = String(req.body?.code || '').trim().toUpperCase();
  const ipEntry = incrementWindowBucket(state.sessionStartBuckets, `ip:${ip}`, now, SESSION_START_WINDOW_MS);
  const pairEntry = incrementWindowBucket(state.sessionStartBuckets, `pair:${ip}:${code || 'no-code'}`, now, SESSION_START_WINDOW_MS);

  if (state.sessionStartBuckets.size > 5000) {
    for (const [bucketKey, value] of state.sessionStartBuckets) {
      if (value.resetAt <= now) state.sessionStartBuckets.delete(bucketKey);
    }
  }

  if (ipEntry.count > SESSION_START_MAX || pairEntry.count > SESSION_START_MAX) {
    const resetAt = Math.max(ipEntry.resetAt, pairEntry.resetAt);
    res.setHeader('Retry-After', Math.max(1, Math.ceil((resetAt - now) / 1000)));
    return res.status(429).json({ error: 'too_many_attempts' });
  }
  next();
}

function safeHashEqual(a, b) {
  const left = String(a || '').trim().toLowerCase();
  const right = String(b || '').trim().toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(left) || !/^[a-f0-9]{64}$/.test(right)) return false;
  return crypto.timingSafeEqual(Buffer.from(left), Buffer.from(right));
}

function createLegacyRoleToken(role, secret) {
  const issuedAt = Date.now();
  const expiry = issuedAt + ADMIN_TTL_MS;
  const nonce = crypto.randomBytes(16).toString('hex');
  const payload = `${expiry}:${nonce}:${role}:${issuedAt}`;
  const sig = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}

function fallbackRoleOnlyLoginGuard(req, res, next) {
  const adminHash = String(process.env.ADMIN_HASH || '').trim().toLowerCase();
  const managerHash = String(process.env.MANAGER_HASH || '').trim().toLowerCase();
  const reviewerHash = String(process.env.REVIEWER_HASH || '').trim().toLowerCase();
  const contentEditorHash = String(process.env.CONTENT_EDITOR_HASH || '').trim().toLowerCase();

  // Normal deployments use the existing server handler. This shim only fixes
  // the edge configuration where reviewer/content-editor password auth is
  // configured without admin/manager hashes (the legacy handler otherwise 503s).
  if (adminHash || managerHash || (!reviewerHash && !contentEditorHash)) return next();

  const now = Date.now();
  const ip = clientIp(req);
  const entry = incrementWindowBucket(state.fallbackLoginBuckets, ip, now, FALLBACK_LOGIN_WINDOW_MS);
  if (entry.count > FALLBACK_LOGIN_MAX) {
    res.setHeader('Retry-After', Math.max(1, Math.ceil((entry.resetAt - now) / 1000)));
    return res.status(429).json({ ok: false, error: 'too_many_attempts' });
  }

  const hash = String(req.body?.hash || '').trim().toLowerCase();
  let role = null;
  let secret = null;
  if (reviewerHash && safeHashEqual(hash, reviewerHash)) {
    role = 'reviewer';
    secret = reviewerHash;
  } else if (contentEditorHash && safeHashEqual(hash, contentEditorHash)) {
    role = 'content_editor';
    secret = contentEditorHash;
  }
  if (!role) return res.status(401).json({ ok: false, error: 'invalid_credentials' });
  return res.json({ ok: true, token: createLegacyRoleToken(role, secret), role });
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

function questionSetMutationGuard(_req, res, next) {
  if (state.questionMutationLocked) return res.status(409).json({ error: 'question_set_mutation_in_progress' });
  state.questionMutationLocked = true;
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    state.questionMutationLocked = false;
  };
  res.once('finish', release);
  res.once('close', release);
  next();
}

async function activateQuestionSetHandler(req, res) {
  const id = Number(req.params?.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    const result = await withHana(async (conn) => {
      let target;
      let hasVersionColumns = true;
      try {
        target = await exec(conn,
          `SELECT qs.QUESTION_SET_ID, qs.VERSION_GROUP_ID, qs.LIFECYCLE_STATUS, COUNT(q.QUESTION_ID) AS QUESTION_COUNT
             FROM QUESTION_SETS qs
             LEFT JOIN QUESTION_SET_QUESTIONS q ON q.QUESTION_SET_ID = qs.QUESTION_SET_ID
            WHERE qs.QUESTION_SET_ID = ?
            GROUP BY qs.QUESTION_SET_ID, qs.VERSION_GROUP_ID, qs.LIFECYCLE_STATUS`, [id]);
      } catch (_e) {
        hasVersionColumns = false;
        target = await exec(conn,
          `SELECT qs.QUESTION_SET_ID, COUNT(q.QUESTION_ID) AS QUESTION_COUNT
             FROM QUESTION_SETS qs
             LEFT JOIN QUESTION_SET_QUESTIONS q ON q.QUESTION_SET_ID = qs.QUESTION_SET_ID
            WHERE qs.QUESTION_SET_ID = ?
            GROUP BY qs.QUESTION_SET_ID`, [id]);
      }
      if (!target.length) return { status: 404, body: { error: 'question_set_not_found' } };
      if (Number(target[0].QUESTION_COUNT || 0) < 1) {
        return { status: 409, body: { error: 'question_set_has_no_questions' } };
      }

      if (hasVersionColumns) {
        const groupId = Number(target[0].VERSION_GROUP_ID || id);
        await exec(conn,
          `UPDATE QUESTION_SETS
              SET IS_ACTIVE = CASE WHEN QUESTION_SET_ID = ? THEN TRUE ELSE FALSE END,
                  LIFECYCLE_STATUS = CASE
                    WHEN QUESTION_SET_ID = ? THEN 'PUBLISHED'
                    WHEN VERSION_GROUP_ID = ? THEN 'ARCHIVED'
                    ELSE LIFECYCLE_STATUS
                  END,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE IS_ACTIVE = TRUE OR QUESTION_SET_ID = ? OR VERSION_GROUP_ID = ?`,
          [id, id, groupId, id, groupId]);
      } else {
        await exec(conn,
          `UPDATE QUESTION_SETS
              SET IS_ACTIVE = CASE WHEN QUESTION_SET_ID = ? THEN TRUE ELSE FALSE END,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE IS_ACTIVE = TRUE OR QUESTION_SET_ID = ?`, [id, id]);
      }

      try {
        await exec(conn,
          `INSERT INTO ADMIN_AUDIT_LOG
            (ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT)
           VALUES ('admin_question_set_activated', NULL, ?, ?, ?, CURRENT_UTCTIMESTAMP)`,
          [JSON.stringify({ questionSetId: id }), String(req.adminRole || 'admin'), clientIp(req)]);
      } catch (_e) { /* audit is best-effort */ }
      return { status: 200, body: { ok: true } };
    });
    if (!result) return res.status(500).json({ error: 'admin_question_set_activate_failed' });
    return res.status(result.status).json(result.body);
  } catch (err) {
    return res.status(500).json({ error: 'admin_question_set_activate_failed', message: err.message });
  }
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
    if (method === 'POST' && path === '/api/admin/login') nextHandlers = [fallbackRoleOnlyLoginGuard, ...nextHandlers];
    if (method === 'POST' && path === '/api/admin/question-sets/:id/activate') {
      nextHandlers = handlers.slice(0, -1).concat(activateQuestionSetHandler);
    }
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
  state.fallbackLoginBuckets.clear();
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
  fallbackRoleOnlyLoginGuard,
  submitGuard,
  questionSetMutationGuard,
  activateQuestionSetHandler,
  resetForTests,
  _state: state
};
