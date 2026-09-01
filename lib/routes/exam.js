'use strict';

const submit = require('../core/submit.js');
const proctor = require('../core/proctor.js');
const sharedRateLimit = require('../core/rate-limit-shared.js');
const adminFeedback = require('../core/admin-feedback.js');
const selectionPolicy = require('../../client/selection-policy.js');
const { findRoute, handlerLayer, prepend, insertBeforeLast, replaceAfterFirst } = require('./helpers.js');

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

function questionForDisplay(session, displayIdx) {
  if (!session || !Number.isInteger(displayIdx) || !Array.isArray(session.qOrder)) return null;
  const questionIdx = session.qOrder[displayIdx];
  const question = Array.isArray(session.questions) ? session.questions[questionIdx] : null;
  if (!question) return null;
  return { ...question, optionCount: Array.isArray(question.opts) ? question.opts.length : 0 };
}

function exposeRequiredSelections(req, res, next) {
  const originalJson = res.json.bind(res);
  res.json = function selectionAwareJson(body) {
    if (body && typeof body === 'object' && Number.isInteger(Number(body.displayIdx))) {
      const displayIdx = Number(body.displayIdx);
      const question = questionForDisplay(req.examSession, displayIdx);
      if (question) body.requiredSelections = selectionPolicy.requiredSelections(question);
    }
    return originalJson(body);
  };
  return next();
}

function selectionGuard(options = {}) {
  return function exactSelectionGuard(req, res, next) {
    const session = req.examSession;
    if (!session) return next();
    const answers = Array.isArray(req.body?.answers) ? req.body.answers : [];
    const requireComplete = options.requireComplete === true && req.body?.autoSubmit !== true;
    const total = Array.isArray(session.qOrder) ? session.qOrder.length : Number(session.total || 0);
    for (let displayIdx = 0; displayIdx < total; displayIdx += 1) {
      const question = questionForDisplay(session, displayIdx);
      if (!question) continue;
      const result = selectionPolicy.validateAnswer(answers[displayIdx] || [], question, { requireComplete });
      if (!result.ok) {
        return res.status(400).json({
          error: result.error,
          question: displayIdx + 1,
          selectedCount: result.selected,
          requiredSelections: result.required
        });
      }
    }
    return next();
  };
}

async function appendSubmitIncidents(req, _res, next) {
  try { await proctor.appendIncidents(req.examSession?.code, req.body?.incidents); }
  catch (_e) { /* telemetry must not block a legitimate submission */ }
  return next();
}

function migrate(app) {
  const sessionStart = findRoute(app, 'post', '/api/session/start');
  const questionRoute = findRoute(app, 'get', '/api/question/:displayIdx');
  const submitRoute = findRoute(app, 'post', '/api/submit');
  const progress = findRoute(app, 'post', '/api/progress');
  if (!sessionStart || !questionRoute || !submitRoute || !progress) throw new Error('candidate_core_route_missing');

  prepend(sessionStart, 'post', '/api/session/start', sessionStartGuard);
  // Capture the resolved set/version before the successful start response is
  // sent. The snapshot is immutable for the life of the attempt, so changing
  // the app's default exam later cannot relabel active/completed history.
  insertBeforeLast(sessionStart, 'post', '/api/session/start', adminFeedback.freezeAttemptOnStart);
  // The legacy question handler already owns auth and randomized option order.
  // Wrap only its JSON response so the browser learns the required count, never
  // the correct answer indexes.
  questionRoute.route.stack.splice(1, 0,
    handlerLayer('get', '/api/question/:displayIdx', exposeRequiredSelections));

  replaceAfterFirst(submitRoute, 'post', '/api/submit', [
    selectionGuard({ requireComplete: true }),
    appendSubmitIncidents,
    submit.submitHandler
  ]);

  // Progress may accumulate middleware over time (auth, validation, metrics,
  // persistence). Validate cardinality and capture incidents immediately after
  // auth while preserving every existing persistence layer.
  progress.route.stack.splice(1, 0,
    handlerLayer('post', '/api/progress', selectionGuard({ requireComplete: false })),
    handlerLayer('post', '/api/progress', proctor.progressIncidentMiddleware));
}

module.exports = {
  clientIp,
  sessionStartGuard,
  questionForDisplay,
  exposeRequiredSelections,
  selectionGuard,
  appendSubmitIncidents,
  migrate
};
