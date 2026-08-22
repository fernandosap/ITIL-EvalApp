'use strict';

const crypto = require('node:crypto');
const {
  getXsuaaConfig,
  exchangeCodeForToken,
  parseCookieHeader,
  inspectXsuaaJwt,
  roleFromClaims
} = require('../../shared/xsuaa.js');
const { withConnection, exec } = require('./db.js');

const KEY_ID_RE = /^[A-Za-z0-9._-]{1,64}$/;

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

function normalizeKey(id, value, label) {
  const keyId = String(id || '').trim();
  const secret = String(value || '').trim();
  if (!keyId && !secret) return null;
  if (!keyId || !secret) throw new Error(`${label}_id_and_key_required`);
  if (!KEY_ID_RE.test(keyId)) throw new Error(`${label}_id_invalid`);
  if (Buffer.byteLength(secret, 'utf8') < 32) throw new Error(`${label}_key_too_short`);
  return { id: keyId, key: crypto.createHash('sha256').update(secret, 'utf8').digest() };
}

function sessionKeyRing(env = process.env) {
  const current = normalizeKey(
    env.SSO_SESSION_KEY_ID || 'v1',
    env.SSO_SESSION_ENCRYPTION_KEY,
    'sso_session'
  );
  if (!current) throw new Error('sso_session_encryption_key_required');
  const previous = normalizeKey(
    env.SSO_SESSION_PREVIOUS_KEY_ID,
    env.SSO_SESSION_PREVIOUS_KEY,
    'sso_session_previous'
  );
  if (previous && previous.id === current.id) throw new Error('sso_session_key_ids_must_differ');
  return { current, previous, byId: new Map([current, previous].filter(Boolean).map((entry) => [entry.id, entry])) };
}

function sessionKey(env = process.env) {
  return sessionKeyRing(env).current.key;
}

function encryptToken(token, env = process.env) {
  const ring = sessionKeyRing(env);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', ring.current.key, iv);
  const ciphertext = Buffer.concat([cipher.update(String(token), 'utf8'), cipher.final()]);
  return {
    keyId: ring.current.id,
    ciphertext: ciphertext.toString('base64'),
    iv: iv.toString('base64'),
    tag: cipher.getAuthTag().toString('base64')
  };
}

function decryptToken(row, env = process.env) {
  const ring = sessionKeyRing(env);
  const keyId = String(row.KEY_ID || '').trim();
  const entry = ring.byId.get(keyId);
  if (!entry) throw new Error('sso_session_unknown_key_id');
  const decipher = crypto.createDecipheriv('aes-256-gcm', entry.key, Buffer.from(String(row.TOKEN_IV), 'base64'));
  decipher.setAuthTag(Buffer.from(String(row.TOKEN_TAG), 'base64'));
  return Buffer.concat([
    decipher.update(Buffer.from(String(row.TOKEN_CIPHERTEXT), 'base64')),
    decipher.final()
  ]).toString('utf8');
}

function principalFromToken(token, xsuaa = getXsuaaConfig()) {
  if (!xsuaa) return null;
  const inspection = inspectXsuaaJwt(token, xsuaa);
  if (!inspection?.claims) return null;
  const role = roleFromClaims(inspection.claims, xsuaa.xsappname);
  if (!role) return null;
  return {
    role,
    sub: inspection.claims.sub ? String(inspection.claims.sub) : null,
    issuedAt: Number(inspection.claims.iat || 0) > 0 ? Number(inspection.claims.iat) * 1000 : Date.now()
  };
}

async function saveSession(id, token, expiresAt) {
  const principal = principalFromToken(token);
  if (!principal) throw new Error('invalid_sso_principal');
  const encrypted = encryptToken(token);
  return withConnection((conn) => exec(conn,
    `INSERT INTO ADMIN_SSO_SESSIONS_V3
      (SESSION_ID, KEY_ID, TOKEN_CIPHERTEXT, TOKEN_IV, TOKEN_TAG, SUBJECT, ROLE_NAME, ISSUED_AT_MS, EXPIRES_AT_MS, CREATED_AT, LAST_SEEN_AT)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
    [id, encrypted.keyId, encrypted.ciphertext, encrypted.iv, encrypted.tag, principal.sub || null,
      principal.role, principal.issuedAt, expiresAt]));
}

async function readSession(id) {
  if (!id) return null;
  return withConnection(async (conn) => {
    const rows = await exec(conn,
      `SELECT KEY_ID, TOKEN_CIPHERTEXT, TOKEN_IV, TOKEN_TAG, SUBJECT, ROLE_NAME, ISSUED_AT_MS, EXPIRES_AT_MS
         FROM ADMIN_SSO_SESSIONS_V3 WHERE SESSION_ID = ?`, [String(id)]);
    if (!rows.length) return null;
    const row = rows[0];
    if (Number(row.EXPIRES_AT_MS || 0) <= Date.now()) {
      await exec(conn, 'DELETE FROM ADMIN_SSO_SESSIONS_V3 WHERE SESSION_ID = ?', [String(id)]);
      return null;
    }
    const token = decryptToken(row);
    await exec(conn, 'UPDATE ADMIN_SSO_SESSIONS_V3 SET LAST_SEEN_AT = CURRENT_UTCTIMESTAMP WHERE SESSION_ID = ?', [String(id)]);
    return {
      token,
      role: String(row.ROLE_NAME),
      sub: row.SUBJECT == null ? null : String(row.SUBJECT),
      issuedAt: Number(row.ISSUED_AT_MS || 0),
      expiresAt: Number(row.EXPIRES_AT_MS || 0)
    };
  });
}

async function deleteSession(id) {
  if (!id) return;
  try { await withConnection((conn) => exec(conn, 'DELETE FROM ADMIN_SSO_SESSIONS_V3 WHERE SESSION_ID = ?', [String(id)])); }
  catch (_e) { /* best effort */ }
}

async function clearSessions() {
  try { await withConnection((conn) => exec(conn, 'DELETE FROM ADMIN_SSO_SESSIONS_V3')); }
  catch (_e) { /* best effort */ }
}

async function sharedSessionMiddleware(req, _res, next) {
  const requestPath = String(req.path || '');
  if (!requestPath.startsWith('/api/admin/')) return next();
  try {
    const cookies = parseCookieHeader(req.headers.cookie);
    const sessionId = cookies.xsuaa_session;
    if (requestPath === '/api/admin/logout' && sessionId) void deleteSession(sessionId);
    if (!req.headers.authorization && sessionId) {
      const session = await readSession(sessionId);
      if (session) {
        req.xsuaaSessionAuth = session;
        req.headers.authorization = `Bearer ${session.token}`;
      }
    }
  } catch (_e) { /* normal auth remains the fallback */ }
  return next();
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
  if (!principalFromToken(tokenResp.accessToken, xsuaa)) return res.status(403).json({ error: 'sso_role_not_authorized' });

  const maxAge = Math.max(60, Number(tokenResp.expiresIn || 3600));
  const id = crypto.randomBytes(32).toString('base64url');
  const expiresAt = Date.now() + maxAge * 1000;
  try { await saveSession(id, tokenResp.accessToken, expiresAt); }
  catch (err) { return res.status(503).json({ error: 'sso_session_store_unavailable', message: err.message }); }

  const secure = req.secure || String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() === 'https';
  const sessionCookie = [`xsuaa_session=${id}`, 'Path=/', `Max-Age=${maxAge}`, 'HttpOnly', 'SameSite=Lax', secure ? 'Secure' : ''].filter(Boolean).join('; ');
  const clearState = `xsuaa_state=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax${secure ? '; Secure' : ''}`;
  res.setHeader('Set-Cookie', [sessionCookie, clearState]);
  return res.redirect(302, '/?admin=1&auth=ok');
}

module.exports = {
  redirectUri,
  jwtIssuedAt,
  sessionKey,
  sessionKeyRing,
  encryptToken,
  decryptToken,
  principalFromToken,
  saveSession,
  readSession,
  deleteSession,
  clearSessions,
  sharedSessionMiddleware,
  oauthCallbackHandler
};