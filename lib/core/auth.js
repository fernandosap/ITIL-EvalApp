'use strict';

const crypto = require('node:crypto');
const { getXsuaaConfig, exchangeCodeForToken, parseCookieHeader } = require('../../shared/xsuaa.js');
const { withConnection, exec, ensureRuntimeTables } = require('./db.js');

function redirectUri(req) {
  const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'https').split(',')[0].trim();
  const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
  return `${proto}://${host}/oauth/callback`;
}

function jwtIssuedAt(token) {
  try {
    const parts = String(token || '').split('.');
    if (parts.length !== 3) return Date.now();
    const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
    return Number(payload.iat || 0) > 0 ? Number(payload.iat) * 1000 : Date.now();
  } catch (_e) { return Date.now(); }
}

async function saveSession(id, token, expiresAt) {
  return withConnection(async (conn) => {
    await ensureRuntimeTables(conn);
    await exec(conn,
      `INSERT INTO ADMIN_SSO_SESSIONS
        (SESSION_ID, JWT, ISSUED_AT_MS, EXPIRES_AT_MS, CREATED_AT, LAST_SEEN_AT)
       VALUES (?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
      [id, token, jwtIssuedAt(token), expiresAt]);
  });
}

async function readSession(id) {
  if (!id) return null;
  return withConnection(async (conn) => {
    await ensureRuntimeTables(conn);
    const rows = await exec(conn,
      'SELECT JWT, ISSUED_AT_MS, EXPIRES_AT_MS FROM ADMIN_SSO_SESSIONS WHERE SESSION_ID = ?', [String(id)]);
    if (!rows.length) return null;
    const row = rows[0];
    if (Number(row.EXPIRES_AT_MS || 0) <= Date.now()) {
      await exec(conn, 'DELETE FROM ADMIN_SSO_SESSIONS WHERE SESSION_ID = ?', [String(id)]);
      return null;
    }
    return { token: String(row.JWT), issuedAt: Number(row.ISSUED_AT_MS || 0), expiresAt: Number(row.EXPIRES_AT_MS || 0) };
  });
}

async function deleteSession(id) {
  if (!id) return;
  try {
    await withConnection(async (conn) => {
      await ensureRuntimeTables(conn);
      await exec(conn, 'DELETE FROM ADMIN_SSO_SESSIONS WHERE SESSION_ID = ?', [String(id)]);
    });
  } catch (_e) { /* best effort */ }
}

async function clearSessions() {
  try {
    await withConnection(async (conn) => {
      await ensureRuntimeTables(conn);
      await exec(conn, 'DELETE FROM ADMIN_SSO_SESSIONS');
    });
  } catch (_e) { /* best effort */ }
}

async function sharedSessionMiddleware(req, _res, next) {
  // The opaque cookie is sent on same-origin static requests as well. Do not
  // turn every JS/SVG/HTML asset request into a HANA lookup; only admin APIs
  // need JWT hydration. OAuth callback/login manage their own cookies.
  const path = String(req.path || '');
  if (!path.startsWith('/api/admin/')) return next();
  try {
    const cookies = parseCookieHeader(req.headers.cookie);
    const sessionId = cookies.xsuaa_session;
    if (path === '/api/admin/logout' && sessionId) void deleteSession(sessionId);
    if (!req.headers.authorization && sessionId) {
      const session = await readSession(sessionId);
      if (session) req.headers.authorization = `Bearer ${session.token}`;
    }
  } catch (_e) { /* server's normal auth path remains the fallback */ }
  next();
}

async function oauthCallbackHandler(req, res) {
  const xsuaa = getXsuaaConfig();
  if (!xsuaa) return res.status(503).json({ error: 'xsuaa_not_bound' });
  const code = String(req.query?.code || '');
  const state = String(req.query?.state || '');
  if (!code) return res.status(400).json({ error: 'missing_code' });
  const cookies = parseCookieHeader(req.headers.cookie);
  if (!cookies.xsuaa_state || !state || cookies.xsuaa_state !== state) return res.status(400).json({ error: 'state_mismatch' });

  const tokenResp = await exchangeCodeForToken(xsuaa, code, redirectUri(req));
  if (!tokenResp?.ok || !tokenResp.accessToken) return res.status(502).json({ error: 'token_exchange_failed' });

  const maxAge = Math.max(60, Number(tokenResp.expiresIn || 3600));
  const id = crypto.randomBytes(32).toString('base64url');
  const expiresAt = Date.now() + maxAge * 1000;
  try {
    await saveSession(id, tokenResp.accessToken, expiresAt);
  } catch (err) {
    return res.status(503).json({ error: 'sso_session_store_unavailable', message: err.message });
  }

  const secure = req.secure || String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() === 'https';
  const sessionCookie = [`xsuaa_session=${id}`, 'Path=/', `Max-Age=${maxAge}`, 'HttpOnly', 'SameSite=Lax', secure ? 'Secure' : ''].filter(Boolean).join('; ');
  const clearState = `xsuaa_state=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax${secure ? '; Secure' : ''}`;
  res.setHeader('Set-Cookie', [sessionCookie, clearState]);
  return res.redirect(302, '/?admin=1&auth=ok');
}

module.exports = { redirectUri, jwtIssuedAt, saveSession, readSession, deleteSession, clearSessions, sharedSessionMiddleware, oauthCallbackHandler };
