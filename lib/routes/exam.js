'use strict';

const submit = require('../core/submit.js');
const proctor = require('../core/proctor.js');
const sharedRateLimit = require('../core/rate-limit-shared.js');
const { findRoute, handlerLayer, prepend, replaceAfterFirst } = require('./helpers.js');

const SESSION_START_MAX = Math.max(1, Number(process.env.SESSION_START_MAX || 10));
const SESSION_START_WINDOW_MS = Math.max(1000, Number(process.env.SESSION_START_WINDOW_MS || 10 * 60 * 1000));

function clientIp(req) {
  const xff = String(req.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
  return xff || String(req.ip || req.socket?.remoteAddress || 'unknown');
}

async function sessionStartGuard(req, res, next) {
  const ip = clientIp(req);
  const code = String(req.body?.code || '').trim().toUpperCase();
  const [ipResult, pairResult] = await Promise.all([
    sharedRateLimit.consumeShared(`session-start:ip:${ip}`, SESSION_START_MAX, SESSION_START_WINDOW_MS),
    sharedRateLimit.consumeShared(`session-start:pair:${ip}:${code || 'none'}`, SESSION_START_MAX, SESSION_START_WINDOW_MS)
  ]);
  if (!ipResult.allowed || !pairResult.allowed) {
    const resetAt = Math.max(ipResult.resetAt, pairResult.resetAt);
    res.setHeader('Retry-After', Math.max(1, Math.ceil((resetAt - Date.now()) / 1000)));
    return res.status(429).json({ error: 'too_many_attempts' });
  }
  return next();
}

async function appendSubmitIncidents(req, _res, next) {
  try { await proctor.appendIncidents(req.examSession?.code, req.body?.incidents); }
  catch (_e) { /* telemetry must not block a legitimate submission */ }
  return next();
}

function migrate(app) {
  const sessionStart = findRoute(app, 'post', '/api/session/start');
  const submitRoute = findRoute(app, 'post', '/api/submit');
  const progress = findRoute(app, 'post', '/api/progress');
  if (!sessionStart || !submitRoute || !progress) throw new Error('candidate_core_route_missing');

  prepend(sessionStart, 'post', '/api/session/start', sessionStartGuard);
  replaceAfterFirst(submitRoute, 'post', '/api/submit', [appendSubmitIncidents, submit.submitHandler]);

  // Progress may accumulate middleware over time (auth, validation, metrics,
  // persistence). Insert incident capture immediately after auth and preserve
  // every existing layer rather than rebuilding the route from assumptions.
  progress.route.stack.splice(1, 0,
    handlerLayer('post', '/api/progress', proctor.progressIncidentMiddleware));
}

module.exports = { clientIp, sessionStartGuard, appendSubmitIncidents, migrate };