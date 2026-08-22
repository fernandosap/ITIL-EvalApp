'use strict';

const { withConnection, exec } = require('./db.js');
const auth = require('./auth.js');

const REFRESH_MS = 30_000;
let revokedAt = 0;
let fetchedAt = 0;

async function refresh() {
  const now = Date.now();
  if (fetchedAt && now - fetchedAt < REFRESH_MS) return revokedAt;
  fetchedAt = now;
  try {
    const value = await withConnection(async (conn) => {
      const rows = await exec(conn, "SELECT SETTING_VALUE FROM APP_SETTINGS WHERE SETTING_KEY = 'ADMIN_TOKEN_NOT_BEFORE'");
      return Number(rows?.[0]?.SETTING_VALUE || 0);
    });
    if (Number.isFinite(value) && value > revokedAt) revokedAt = value;
  } catch (_e) { /* existing auth remains fallback */ }
  return revokedAt;
}

function bearerIatMs(header) {
  const authz = String(header || '').trim();
  if (!authz.startsWith('Bearer ')) return 0;
  try {
    const payload = JSON.parse(Buffer.from(authz.slice(7).trim().split('.')[1], 'base64url').toString('utf8'));
    return Number(payload.iat || 0) * 1000;
  } catch (_e) { return 0; }
}

async function middleware(req, res, next) {
  if (!String(req.path || '').startsWith('/api/admin/') || req.path === '/api/admin/logout') return next();
  await refresh();
  if (!revokedAt) return next();
  const issuedAt = Number(req.xsuaaSessionAuth?.issuedAt || 0) || bearerIatMs(req.headers.authorization);
  if (issuedAt && issuedAt <= revokedAt) return res.status(401).json({ error: 'session_revoked' });
  return next();
}

function watchRevokeAll(req, res, next) {
  if (req.path === '/api/admin/sessions/revoke-all') {
    res.once('finish', () => {
      if (res.statusCode >= 200 && res.statusCode < 300) {
        revokedAt = Date.now();
        fetchedAt = revokedAt;
        void auth.clearSessions();
      }
    });
  }
  return next();
}

function resetForTests() { revokedAt = 0; fetchedAt = 0; }

module.exports = { refresh, bearerIatMs, middleware, watchRevokeAll, resetForTests };
