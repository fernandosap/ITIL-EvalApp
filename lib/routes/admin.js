'use strict';

const express = require('express');
const { acquireRowLock } = require('../core/db.js');
const questionSets = require('../core/question-sets.js');
const accessCodes = require('../core/access-codes.js');
const proctor = require('../core/proctor.js');
const legacyLogin = require('../core/legacy-login.js');
const concurrency = require('../core/concurrency.js');
const {
  findRoute,
  prepend,
  insertBeforeLast,
  replaceLast,
  insertRouterBeforeFallback
} = require('./helpers.js');

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

function handlers(layer, count = 2) {
  if (!layer?.route?.stack?.length) return [];
  return layer.route.stack.slice(0, count).map((entry) => entry.handle);
}

function needsConcurrency(routePath) {
  if (typeof routePath !== 'string' || !routePath.includes('/:id')) return false;
  if (routePath.endsWith('/clone')) return false;
  return true;
}

function addSupplementalRoutes(app) {
  const router = express.Router();
  const setList = findRoute(app, 'get', '/api/admin/question-sets');
  const sweeper = findRoute(app, 'get', '/api/admin/sweeper-status');
  const review = findRoute(app, 'get', '/api/admin/results/:code/review');
  if (!setList || !sweeper || !review) throw new Error('admin_route_anchor_missing');

  router.get('/api/admin/question-sets/:id/readiness', ...handlers(setList), concurrency.exposeVersion, questionSets.readinessHandler);
  router.get('/api/admin/live-sessions', ...handlers(sweeper), proctor.liveSessionsHandler);
  router.get('/api/admin/proctor/incidents/:code', ...handlers(review), proctor.timelineHandler);
  insertRouterBeforeFallback(app, router);
}

function migrate(app) {
  const login = findRoute(app, 'post', '/api/admin/login');
  const generate = findRoute(app, 'post', '/api/admin/generate');
  const createSet = findRoute(app, 'post', '/api/admin/question-sets');
  const upload = findRoute(app, 'post', '/api/admin/question-sets/upload');
  const clone = findRoute(app, 'post', '/api/admin/question-sets/:id/clone');
  const activate = findRoute(app, 'post', '/api/admin/question-sets/:id/activate');
  if (!login || !generate || !createSet || !upload || !clone || !activate) throw new Error('admin_core_route_missing');

  prepend(login, 'post', '/api/admin/login', legacyLogin.fallbackLoginGuard);
  replaceLast(generate, accessCodes.generateHandler);
  replaceLast(createSet, questionSets.createHandler);
  replaceLast(upload, questionSets.uploadHandler);
  replaceLast(clone, questionSets.cloneHandler);
  replaceLast(activate, questionSets.activateHandler);

  for (const layer of app._router.stack) {
    const routePath = layer.route?.path;
    const method = Object.keys(layer.route?.methods || {}).find((name) => ['post', 'put', 'patch', 'delete'].includes(name));
    if (!method || typeof routePath !== 'string' || !routePath.startsWith('/api/admin/question-sets')) continue;
    insertBeforeLast(layer, method, routePath, questionMutationGuard);
    if (needsConcurrency(routePath)) insertBeforeLast(layer, method, routePath, concurrency.requireCurrentVersion);
  }

  for (const layer of app._router.stack) {
    const routePath = layer.route?.path;
    if (!layer.route?.methods?.get || typeof routePath !== 'string' || !routePath.startsWith('/api/admin/question-sets/:id')) continue;
    prepend(layer, 'get', routePath, concurrency.exposeVersion);
  }

  addSupplementalRoutes(app);
}

module.exports = { questionMutationGuard, needsConcurrency, addSupplementalRoutes, migrate };
