'use strict';

// Transitional bootstrap: server.js remains the legacy composition root, but
// the actual behavior now lives in explicit core services. This file only
// wires those services into Express route registration until server.js is
// physically split into route modules.

const { withConnection, exec } = require('./core/db.js');
const auth = require('./core/auth.js');
const submit = require('./core/submit.js');
const proctor = require('./core/proctor.js');
const questionSets = require('./core/question-sets.js');
const accessCodes = require('./core/access-codes.js');

const SESSION_START_MAX = Math.max(1, Number(process.env.SESSION_START_MAX || 10));
const SESSION_START_WINDOW_MS = Math.max(1000, Number(process.env.SESSION_START_WINDOW_MS || 10 * 60 * 1000));
const REVOCATION_REFRESH_MS = 30_000;

const state = {
  installed: false,
  originalConsoleLog: console.log,
  sessionStartBuckets: new Map(),
  questionMutationLocked: false,
  revokedAt: 0,
  revocationFetchedAt: 0
};

function clientIp(req) {
  const xff = String(req.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
  return xff || String(req.ip || req.socket?.remoteAddress || 'unknown');
}

function incrementBucket(key, now) {
  let entry = state.sessionStartBuckets.get(key);
  if (!entry || entry.resetAt <= now) {
    entry = { count: 0, resetAt: now + SESSION_START_WINDOW_MS };
    state.sessionStartBuckets.set(key, entry);
  }
  entry.count += 1;
  return entry;
}

function sessionStartGuard(req, res, next) {
  const now = Date.now();
  const ip = clientIp(req);
  const code = String(req.body?.code || '').trim().toUpperCase();
  const ipEntry = incrementBucket(`ip:${ip}`, now);
  const pairEntry = incrementBucket(`pair:${ip}:${code || 'none'}`, now);
  if (state.sessionStartBuckets.size > 5000) {
    for (const [key, value] of state.sessionStartBuckets) if (value.resetAt <= now) state.sessionStartBuckets.delete(key);
  }
  if (ipEntry.count > SESSION_START_MAX || pairEntry.count > SESSION_START_MAX) {
    const resetAt = Math.max(ipEntry.resetAt, pairEntry.resetAt);
    res.setHeader('Retry-After', Math.max(1, Math.ceil((resetAt - now) / 1000)));
    return res.status(429).json({ error: 'too_many_attempts' });
  }
  next();
}

async function refreshRevocation() {
  const now = Date.now();
  if (state.revocationFetchedAt && now - state.revocationFetchedAt < REVOCATION_REFRESH_MS) return;
  state.revocationFetchedAt = now;
  try {
    const value = await withConnection(async (conn) => {
      const rows = await exec(conn,
        `SELECT SETTING_VALUE FROM APP_SETTINGS WHERE SETTING_KEY = 'ADMIN_TOKEN_NOT_BEFORE'`);
      return Number(rows?.[0]?.SETTING_VALUE || 0);
    });
    if (Number.isFinite(value) && value > state.revokedAt) state.revokedAt = value;
  } catch (_e) { /* normal server auth remains fallback */ }
}

function jwtIatMs(header) {
  const authz = String(header || '').trim();
  if (!authz.startsWith('Bearer ')) return 0;
  try {
    const payload = JSON.parse(Buffer.from(authz.slice(7).trim().split('.')[1], 'base64url').toString('utf8'));
    return Number(payload.iat || 0) * 1000;
  } catch (_e) { return 0; }
}

async function revocationMiddleware(req, res, next) {
  if (!String(req.path || '').startsWith('/api/admin/') || req.path === '/api/admin/logout') return next();
  await refreshRevocation();
  if (!state.revokedAt) return next();
  const issuedAt = jwtIatMs(req.headers.authorization);
  if (issuedAt && issuedAt <= state.revokedAt) return res.status(401).json({ error: 'session_revoked' });
  next();
}

function questionMutationGuard(_req, res, next) {
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

function sanitizeSlowLogs() {
  console.log = function coreLog(...args) {
    if (typeof args[0] === 'string' && args[0].startsWith('{')) {
      try {
        const parsed = JSON.parse(args[0]);
        if (parsed?.event === 'slow_request' && typeof parsed.path === 'string') {
          parsed.path = parsed.path.split('?')[0];
          args[0] = JSON.stringify(parsed);
        }
      } catch (_e) { /* leave non-JSON output alone */ }
    }
    return state.originalConsoleLog(...args);
  };
}

function replaceLast(handlers, handler) {
  return handlers.length ? handlers.slice(0, -1).concat(handler) : [handler];
}

function wrapRouteRegistration(app, methodName) {
  const original = app[methodName].bind(app);
  app[methodName] = function coreRoute(path, ...handlers) {
    const method = methodName.toUpperCase();
    let nextHandlers = handlers;

    if (method === 'GET' && path === '/oauth/callback') nextHandlers = [auth.oauthCallbackHandler];
    if (method === 'POST' && path === '/api/session/start') nextHandlers = [sessionStartGuard, ...handlers];
    if (method === 'POST' && path === '/api/submit') {
      // Keep requireExamSession (first handler), append latest client incidents,
      // then use the DB-idempotent durable submission service.
      const authHandler = handlers[0];
      nextHandlers = [authHandler, async (req, _res, next) => {
        try { await proctor.appendIncidents(req.examSession?.code, req.body?.incidents); } catch (_e) { /* non-blocking */ }
        next();
      }, submit.submitHandler];
    }
    if (method === 'POST' && path === '/api/progress' && handlers.length >= 2) {
      nextHandlers = [handlers[0], proctor.progressIncidentMiddleware, ...handlers.slice(1)];
    }
    if (method === 'POST' && path === '/api/admin/generate') nextHandlers = replaceLast(handlers, accessCodes.generateHandler);
    if (method === 'POST' && path === '/api/admin/question-sets') nextHandlers = replaceLast(handlers, questionSets.createHandler);
    if (method === 'POST' && path === '/api/admin/question-sets/upload') nextHandlers = replaceLast(handlers, questionSets.uploadHandler);
    if (method === 'POST' && path === '/api/admin/question-sets/:id/clone') nextHandlers = replaceLast(handlers, questionSets.cloneHandler);
    if (method === 'POST' && path === '/api/admin/question-sets/:id/activate') nextHandlers = replaceLast(handlers, questionSets.activateHandler);

    if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)
        && typeof path === 'string' && path.startsWith('/api/admin/question-sets')) {
      nextHandlers = [questionMutationGuard, ...nextHandlers];
    }

    const result = original(path, ...nextHandlers);

    // Reuse the exact auth/permission middleware already declared by server.js
    // for adjacent APIs, so new endpoints inherit existing RBAC policy.
    if (method === 'GET' && path === '/api/admin/question-sets' && handlers.length >= 2) {
      original('/api/admin/question-sets/:id/readiness', handlers[0], handlers[1], questionSets.readinessHandler);
    }
    if (method === 'GET' && path === '/api/admin/sweeper-status' && handlers.length >= 2) {
      original('/api/admin/live-sessions', handlers[0], handlers[1], proctor.liveSessionsHandler);
    }
    if (method === 'GET' && path === '/api/admin/results/:code/review' && handlers.length >= 2) {
      original('/api/admin/proctor/incidents/:code', handlers[0], handlers[1], proctor.timelineHandler);
    }
    return result;
  };
}

function installExpressWrapper() {
  const expressPath = require.resolve('express');
  const originalExpress = require(expressPath);
  if (originalExpress.__itilCoreWrapped) return;
  function coreExpress(...args) {
    const app = originalExpress(...args);
    // Shared SSO hydration must run before revocation and server auth.
    app.use(auth.sharedSessionMiddleware);
    app.use(revocationMiddleware);
    for (const method of ['get', 'post', 'put', 'patch', 'delete']) wrapRouteRegistration(app, method);
    // Keep the shared session table coherent with logout/revoke-all.
    app.use((req, res, next) => {
      if (req.path === '/api/admin/sessions/revoke-all') {
        res.once('finish', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            state.revokedAt = Date.now();
            state.revocationFetchedAt = Date.now();
            void auth.clearSessions();
          }
        });
      }
      next();
    });
    return app;
  }
  Object.assign(coreExpress, originalExpress);
  Object.defineProperty(coreExpress, '__itilCoreWrapped', { value: true });
  require.cache[expressPath].exports = coreExpress;
}

function install() {
  if (state.installed) return;
  state.installed = true;
  sanitizeSlowLogs();
  installExpressWrapper();
}

function resetForTests() {
  state.sessionStartBuckets.clear();
  state.questionMutationLocked = false;
  state.revokedAt = 0;
  state.revocationFetchedAt = 0;
}

install();

module.exports = { install, sessionStartGuard, questionMutationGuard, revocationMiddleware, resetForTests, _state: state };
