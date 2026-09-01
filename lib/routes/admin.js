'use strict';

const express = require('express');
const { acquireRowLock } = require('../core/db.js');
const questionSets = require('../core/question-sets.js');
const accessCodes = require('../core/access-codes.js');
const proctor = require('../core/proctor.js');
const legacyLogin = require('../core/legacy-login.js');
const concurrency = require('../core/concurrency.js');
const adminFeedback = require('../core/admin-feedback.js');
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

function clarifyLegacyLoginWarnings(req, res, next) {
  const originalJson = res.json.bind(res);
  res.json = function clarifiedStatusJson(body) {
    if (body && Array.isArray(body.warnings)) {
      body.warnings = body.warnings.map((warning) => {
        const text = String(warning || '');
        if (/REVIEWER_HASH.*not configured|Reviewer login is disabled/i.test(text)) {
          return 'Reviewer legacy password login is not configured. SAP SSO/RBAC access is unaffected.';
        }
        if (/CONTENT_EDITOR_HASH.*not configured|Content editor login is disabled/i.test(text)) {
          return 'Content Editor legacy password login is not configured. SAP SSO/RBAC access is unaffected.';
        }
        if (/MANAGER_HASH.*not configured|Manager login is disabled/i.test(text)) {
          return 'Manager legacy password login is not configured. SAP SSO/RBAC access is unaffected.';
        }
        return warning;
      });
    }
    return originalJson(body);
  };
  return next();
}

function addSupplementalRoutes(app) {
  const router = express.Router();
  const setList = findRoute(app, 'get', '/api/admin/question-sets');
  const sweeper = findRoute(app, 'get', '/api/admin/sweeper-status');
  const review = findRoute(app, 'get', '/api/admin/results/:code/review');
  const bulkDelete = findRoute(app, 'post', '/api/admin/codes/bulk-delete');
  if (!setList || !sweeper || !review || !bulkDelete) throw new Error('admin_route_anchor_missing');

  router.get('/api/admin/question-sets/:id/readiness', ...handlers(setList), concurrency.exposeVersion, questionSets.readinessHandler);
  router.get('/api/admin/live-sessions', ...handlers(sweeper), proctor.liveSessionsHandler);
  router.get('/api/admin/proctor/incidents/:code', ...handlers(review), proctor.timelineHandler);
  // Archiving is intentionally guarded with the same admin-only + write-rate
  // middleware as destructive bulk delete. Archive itself is reversible and
  // never deletes a result, progress audit record, or signed summary.
  router.post('/api/admin/codes/archive', ...handlers(bulkDelete, 3), adminFeedback.archiveHandler);
  router.post('/api/admin/codes/unarchive', ...handlers(bulkDelete, 3), adminFeedback.unarchiveHandler);
  insertRouterBeforeFallback(app, router);
}

function migrate(app) {
  const login = findRoute(app, 'post', '/api/admin/login');
  const generate = findRoute(app, 'post', '/api/admin/generate');
  const systemStatus = findRoute(app, 'get', '/api/admin/system-status');
  const codes = findRoute(app, 'get', '/api/admin/codes');
  const exportCsv = findRoute(app, 'get', '/api/admin/export.csv');
  const resetCode = findRoute(app, 'post', '/api/admin/reset');
  const createSet = findRoute(app, 'post', '/api/admin/question-sets');
  const upload = findRoute(app, 'post', '/api/admin/question-sets/upload');
  const clone = findRoute(app, 'post', '/api/admin/question-sets/:id/clone');
  const activate = findRoute(app, 'post', '/api/admin/question-sets/:id/activate');
  if (!login || !generate || !systemStatus || !codes || !exportCsv || !resetCode || !createSet || !upload || !clone || !activate) {
    throw new Error('admin_core_route_missing');
  }

  prepend(login, 'post', '/api/admin/login', legacyLogin.fallbackLoginGuard);
  // Keep operational warnings precise: missing legacy hashes do not mean SAP
  // SSO role collections are disabled.
  insertBeforeLast(systemStatus, 'get', '/api/admin/system-status', clarifyLegacyLoginWarnings);
  // Keep the legacy /codes payload shape for compatibility, then replace its
  // exam identity with the immutable attempt snapshot for active/completed
  // rows. This prevents a later default-exam change from rewriting history.
  insertBeforeLast(codes, 'get', '/api/admin/codes', adminFeedback.enrichCodesResponse);
  replaceLast(exportCsv, adminFeedback.exportHandler);
  insertBeforeLast(resetCode, 'post', '/api/admin/reset', adminFeedback.clearSnapshotOnReset);
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

module.exports = { questionMutationGuard, clarifyLegacyLoginWarnings, needsConcurrency, addSupplementalRoutes, migrate };
