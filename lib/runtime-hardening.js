'use strict';

// Transitional application installer. The old require-cache replacement of the
// Express factory is gone; preload mode now hooks the Express app lifecycle and
// installs explicit core services on each newly-created app.

const { withConnection, exec, acquireRowLock } = require('./core/db.js');
const auth = require('./core/auth.js');
const submit = require('./core/submit.js');
const proctor = require('./core/proctor.js');
const questionSets = require('./core/question-sets.js');
const accessCodes = require('./core/access-codes.js');
const legacyLogin = require('./core/legacy-login.js');
const sharedRateLimit = require('./core/rate-limit-shared.js');

const SESSION_START_MAX = Math.max(1, Number(process.env.SESSION_START_MAX || 10));
const SESSION_START_WINDOW_MS = Math.max(1000, Number(process.env.SESSION_START_WINDOW_MS || 10 * 60 * 1000));
const REVOCATION_REFRESH_MS = 30_000;

const state = {
  installedApps: new WeakSet(),
  revokedAt: 0,
  revocationFetchedAt: 0
};

function clientIp(req) {
  const xff = String(req.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
  return xff || String(req.ip || req.socket?.remoteAddress || 'unknown');
}

async function sessionStartGuard(req, res, next) {
  const ip = clientIp(req);
  const code = String(req.body?.code || '').trim().toUpperCase();
  const ipResult = await sharedRateLimit.consumeShared(`session-start:ip:${ip}`, SESSION_START_MAX, SESSION_START_WINDOW_MS);
  const pairResult = await sharedRateLimit.consumeShared(`session-start:pair:${ip}:${code || 'none'}`, SESSION_START_MAX, SESSION_START_WINDOW_MS);
  if (!ipResult.allowed || !pairResult.allowed) {
    const resetAt = Math.max(ipResult.resetAt, pairResult.resetAt);
    res.setHeader('Retry-After', Math.max(1, Math.ceil((resetAt - Date.now()) / 1000)));
    return res.status(429).json({ error: 'too_many_attempts' });
  }
  return next();
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
  const issuedAt = Number(req.xsuaaSessionAuth?.issuedAt || 0) || jwtIatMs(req.headers.authorization);
  if (issuedAt && issuedAt <= state.revokedAt) return res.status(401).json({ error: 'session_revoked' });
  return next();
}

async function questionMutationGuard(_req, res, next) {
  let release;
  try { release = await acquireRowLock('question_sets'); }
  catch (err) { return res.status(503).json({ error: 'question_set_lock_unavailable', message: err.message }); }
  let released = false;
  const finish = async (ok) => {
    if (released) return;
    released = true;
    try { await release(ok); } catch (_e) { /* response already completed */ }
  };
  res.once('finish', () => { void finish(res.statusCode < 500); });
  res.once('close', () => { void finish(false); });
  return next();
}

function replaceLast(handlers, handler) {
  return handlers.length ? handlers.slice(0, -1).concat(handler) : [handler];
}

function insertBeforeLast(handlers, handler) {
  if (!handlers.length) return [handler];
  return handlers.slice(0, -1).concat(handler, handlers[handlers.length - 1]);
}

function wrapRouteRegistration(app, methodName) {
  const original = app[methodName].bind(app);
  app[methodName] = function coreRoute(path, ...handlers) {
    const method = methodName.toUpperCase();
    let nextHandlers = handlers;

    if (method === 'GET' && path === '/oauth/callback') nextHandlers = [auth.oauthCallbackHandler];
    if (method === 'POST' && path === '/api/admin/login') nextHandlers = [legacyLogin.fallbackLoginGuard, ...handlers];
    if (method === 'POST' && path === '/api/session/start') nextHandlers = [sessionStartGuard, ...handlers];
    if (method === 'POST' && path === '/api/submit') {
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
      nextHandlers = insertBeforeLast(nextHandlers, questionMutationGuard);
    }

    const result = original(path, ...nextHandlers);
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

function install(app) {
  if (!app || typeof app.use !== 'function') throw new Error('express_app_required');
  if (state.installedApps.has(app)) return app;
  state.installedApps.add(app);
  app.use(auth.sharedSessionMiddleware);
  app.use(revocationMiddleware);
  for (const method of ['get', 'post', 'put', 'patch', 'delete']) wrapRouteRegistration(app, method);
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

// Compatibility hook for the current large server.js composition root. Unlike
// the previous implementation this does not replace require('express') or
// mutate require.cache. New apps call install(app) explicitly; the preload path
// only intercepts application.init so legacy server.js gets the same wiring.
function installPreloadHook() {
  const express = require('express');
  const proto = express.application;
  if (proto.__itilCoreInitHooked) return;
  const originalInit = proto.init;
  proto.init = function itilCoreInit(...args) {
    const result = originalInit.apply(this, args);
    install(this);
    return result;
  };
  Object.defineProperty(proto, '__itilCoreInitHooked', { value: true, configurable: false });
}

function resetForTests() {
  state.revokedAt = 0;
  state.revocationFetchedAt = 0;
  sharedRateLimit.resetForTests();
  legacyLogin.resetForTests();
}

installPreloadHook();

module.exports = {
  install,
  installPreloadHook,
  sessionStartGuard,
  questionMutationGuard,
  revocationMiddleware,
  resetForTests,
  _state: state
};
