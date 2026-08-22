'use strict';

// Runtime hardening installed before server.js loads. It deliberately stays
// small and dependency-free so it can guard route registration/runtime edges
// that cut across the legacy monolithic server without reopening unrelated
// business logic.

const crypto = require('node:crypto');

const SESSION_START_MAX = Math.max(1, Number(process.env.SESSION_START_MAX || 10));
const SESSION_START_WINDOW_MS = Math.max(1000, Number(process.env.SESSION_START_WINDOW_MS || 10 * 60 * 1000));

const state = {
  installed: false,
  originalMathRandom: Math.random,
  originalConsoleLog: console.log,
  sessionStartBuckets: new Map(),
  submitLocks: new Set(),
  questionMutationLocked: false,
  xsuaaRevokedAt: 0,
  xsuaaSessions: new Map()
};

function secureMathRandom() {
  // 48 random bits are enough to produce a high-quality [0, 1) double and
  // avoid Math.random() for access-code generation in server.js.
  const bytes = crypto.randomBytes(6);
  let value = 0;
  for (const byte of bytes) value = (value * 256) + byte;
  return value / 281474976710656; // 2^48
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
  } catch (_e) {
    return null;
  }
}

function decodeLegacyAdminIssuedAt(token) {
  try {
    const decoded = Buffer.from(String(token || ''), 'base64url').toString('utf8');
    const parts = decoded.split(':');
    if (parts.length !== 5) return 0;
    const issuedAt = Number(parts[3]);
    return Number.isFinite(issuedAt) ? issuedAt : 0;
  } catch (_e) {
    return 0;
  }
}

function shouldBlockRevokedAdminRequest(req) {
  if (!state.xsuaaRevokedAt) return false;
  if (req.path === '/api/admin/logout') return false;
  if (!String(req.path || '').startsWith('/api/admin/')) return false;

  const auth = String(req.headers?.authorization || '').trim();
  if (auth.startsWith('Bearer ')) {
    const payload = decodeJwtPayload(auth.slice(7).trim());
    const issuedAtMs = Number(payload?.iat || 0) * 1000;
    return !issuedAtMs || issuedAtMs <= state.xsuaaRevokedAt;
  }

  const cookies = parseCookies(req.headers?.cookie);
  const opaqueId = cookies.xsuaa_session;
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

function checkSessionStartRate(req) {
  const now = Date.now();
  const key = clientIp(req);
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
  return entry.count <= SESSION_START_MAX;
}

function isQuestionSetMutation(req) {
  if (!String(req.path || '').startsWith('/api/admin/question-sets')) return false;
  return ['POST', 'PUT', 'PATCH', 'DELETE'].includes(String(req.method || '').toUpperCase());
}

function trackOAuthSessionCookie(req, res) {
  if (req.path !== '/oauth/callback') return;
  const original = res.setHeader.bind(res);
  res.setHeader = function hardenedSetHeader(name, value) {
    if (String(name).toLowerCase() === 'set-cookie') {
      const values = Array.isArray(value) ? value : [value];
      for (const item of values) {
        const match = String(item || '').match(/(?:^|;\s*)xsuaa_session=([^;]+)/);
        if (match && match[1]) state.xsuaaSessions.set(match[1], Date.now());
      }
    }
    return original(name, value);
  };
}

function hardeningMiddleware(req, res, next) {
  trackOAuthSessionCookie(req, res);

  if (shouldBlockRevokedAdminRequest(req)) {
    return res.status(401).json({ error: 'session_revoked' });
  }

  if (req.method === 'POST' && req.path === '/api/session/start') {
    if (!checkSessionStartRate(req)) {
      res.setHeader('Retry-After', Math.ceil(SESSION_START_WINDOW_MS / 1000));
      return res.status(429).json({ error: 'too_many_attempts' });
    }
  }

  if (req.method === 'POST' && req.path === '/api/submit') {
    const token = String(req.headers?.['x-exam-token'] || '').trim();
    if (token) {
      const lockKey = crypto.createHash('sha256').update(token).digest('hex');
      if (state.submitLocks.has(lockKey)) {
        return res.status(409).json({ error: 'submission_in_progress' });
      }
      state.submitLocks.add(lockKey);
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        state.submitLocks.delete(lockKey);
      };
      res.once('finish', release);
      res.once('close', release);
    }
  }

  if (isQuestionSetMutation(req)) {
    if (state.questionMutationLocked) {
      return res.status(409).json({ error: 'question_set_mutation_in_progress' });
    }
    state.questionMutationLocked = true;
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      state.questionMutationLocked = false;
    };
    res.once('finish', release);
    res.once('close', release);
  }

  if (req.method === 'POST' && req.path === '/api/admin/sessions/revoke-all') {
    res.once('finish', () => {
      if (res.statusCode >= 200 && res.statusCode < 300) {
        state.xsuaaRevokedAt = Date.now();
        state.xsuaaSessions.clear();
      }
    });
  }

  next();
}

function installExpressWrapper() {
  const expressPath = require.resolve('express');
  const originalExpress = require(expressPath);
  if (originalExpress.__itilHardeningWrapped) return;

  function hardenedExpress(...args) {
    const app = originalExpress(...args);
    // Registered before server.js body parsers/routes, so it protects every
    // request and cannot be bypassed by a later route definition.
    app.use(hardeningMiddleware);
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
      } catch (_e) {
        // Non-JSON application output: leave untouched.
      }
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
  state.questionMutationLocked = false;
  state.xsuaaRevokedAt = 0;
  state.xsuaaSessions.clear();
}

install();

module.exports = {
  install,
  secureMathRandom,
  parseCookies,
  decodeJwtPayload,
  decodeLegacyAdminIssuedAt,
  hardeningMiddleware,
  resetForTests,
  _state: state
};
