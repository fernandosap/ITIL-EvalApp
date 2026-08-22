'use strict';

// Transitional bootstrap for the legacy server.js composition root. This file
// does not wrap app.get/post/put/patch/delete and does not alter require.cache.
// It performs a one-time route migration immediately before the real app
// listens, then owns shared middleware, housekeeping, CSP and shutdown.

const path = require('node:path');
const express = require('express');
const auth = require('./core/auth.js');
const revocation = require('./core/revocation.js');
const csp = require('./core/csp.js');
const housekeeping = require('./core/housekeeping.js');
const { config } = require('./core/db.js');
const sharedRateLimit = require('./core/rate-limit-shared.js');
const legacyLogin = require('./core/legacy-login.js');
const oauthRoutes = require('./routes/oauth.js');
const examRoutes = require('./routes/exam.js');
const adminRoutes = require('./routes/admin.js');
const { shutdownPool } = require('../shared/db-pool.js');

const installed = new WeakSet();
const servers = new Set();
let consoleWrapped = false;
let signalsInstalled = false;
let shuttingDown = false;

function middlewareLayer(fn) {
  const router = express.Router();
  router.use(fn);
  return router.stack[0];
}

function installGlobalMiddleware(app) {
  if (!app._router?.stack) throw new Error('express_router_not_initialized');
  const layers = [
    middlewareLayer(csp.middleware(path.join(__dirname, '..'))),
    middlewareLayer(auth.sharedSessionMiddleware),
    middlewareLayer(revocation.middleware),
    middlewareLayer(revocation.watchRevokeAll)
  ];
  app._router.stack.unshift(...layers);
}

function sanitizeSlowLogs() {
  if (consoleWrapped) return;
  consoleWrapped = true;
  const original = console.log.bind(console);
  console.log = function sanitizedLog(...args) {
    if (typeof args[0] === 'string' && args[0].startsWith('{')) {
      try {
        const parsed = JSON.parse(args[0]);
        if (parsed?.event === 'slow_request' && typeof parsed.path === 'string') {
          parsed.path = parsed.path.split('?')[0];
          args[0] = JSON.stringify(parsed);
        }
      } catch (_e) { /* non-JSON application output */ }
    }
    return original(...args);
  };
}

function validateRuntimeConfig() {
  const xsuaa = require('../shared/xsuaa.js').getXsuaaConfig();
  if (xsuaa) auth.sessionKeyRing(process.env);
}

function routeExists(app, method, routePath) {
  return Boolean(app?._router?.stack?.some((layer) => layer.route
    && layer.route.path === routePath
    && layer.route.methods?.[method]));
}

function isApplicationRoot(app) {
  // Node's test suite creates several small Express fixtures and calls
  // listen() on them. Only the production composition root owns this complete
  // set of anchors; fixtures must remain untouched by the migration layer.
  return routeExists(app, 'get', '/oauth/callback')
    && routeExists(app, 'post', '/api/session/start')
    && routeExists(app, 'post', '/api/submit')
    && routeExists(app, 'get', '/api/admin/question-sets')
    && routeExists(app, 'get', '/api/admin/sweeper-status');
}

function finalizeApp(app) {
  if (installed.has(app)) return app;
  if (!app?._router?.stack) throw new Error('express_app_not_ready');
  if (!isApplicationRoot(app)) return app;
  validateRuntimeConfig();
  installGlobalMiddleware(app);
  oauthRoutes.migrate(app);
  examRoutes.migrate(app);
  adminRoutes.migrate(app);
  installed.add(app);
  if (config(process.env)) {
    housekeeping.startHousekeeping({
      log(level, event, meta) {
        console.log(JSON.stringify({ ts: new Date().toISOString(), level, event, ...meta }));
      }
    });
  }
  return app;
}

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  const timeoutMs = Math.max(1000, Number(process.env.GRACEFUL_SHUTDOWN_MS || 15000));
  console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', event: 'graceful_shutdown_started', signal, timeoutMs }));
  housekeeping.stopHousekeeping();
  try {
    if (require.main?.exports?.stopBackgroundJobs) require.main.exports.stopBackgroundJobs();
  } catch (_e) { /* best effort */ }

  const closes = [...servers].map((server) => new Promise((resolve) => {
    try { server.close(() => resolve()); } catch (_e) { resolve(); }
  }));
  const timeout = new Promise((resolve) => setTimeout(resolve, timeoutMs));
  await Promise.race([Promise.allSettled(closes), timeout]);
  for (const server of servers) {
    try { if (typeof server.closeAllConnections === 'function') server.closeAllConnections(); } catch (_e) { /* ignore */ }
  }
  shutdownPool();
  console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', event: 'graceful_shutdown_completed', signal }));
  if (process.env.NODE_ENV !== 'test') process.exit(0);
}

function installSignals() {
  if (signalsInstalled || process.env.NODE_ENV === 'test') return;
  signalsInstalled = true;
  process.once('SIGTERM', () => { void gracefulShutdown('SIGTERM'); });
  process.once('SIGINT', () => { void gracefulShutdown('SIGINT'); });
}

function installListenHook() {
  const proto = express.application;
  if (proto.__itilBootstrapListenHooked) return;
  const originalListen = proto.listen;
  proto.listen = function itilBootstrapListen(...args) {
    const mainApp = isApplicationRoot(this);
    if (mainApp) finalizeApp(this);
    const server = originalListen.apply(this, args);
    if (mainApp) {
      servers.add(server);
      server.once('close', () => servers.delete(server));
      installSignals();
    }
    return server;
  };
  Object.defineProperty(proto, '__itilBootstrapListenHooked', { value: true, configurable: false });
}

function resetForTests() {
  sharedRateLimit.resetForTests();
  legacyLogin.resetForTests();
  revocation.resetForTests();
  housekeeping.stopHousekeeping();
  shuttingDown = false;
}

sanitizeSlowLogs();
installListenHook();

module.exports = {
  finalizeApp,
  validateRuntimeConfig,
  routeExists,
  isApplicationRoot,
  sanitizeSlowLogs,
  gracefulShutdown,
  resetForTests,
  _servers: servers
};