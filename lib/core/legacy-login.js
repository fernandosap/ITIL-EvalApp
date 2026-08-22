'use strict';

const crypto = require('node:crypto');

const TTL_MS = 8 * 60 * 60 * 1000;
const MAX = 8;
const WINDOW_MS = 15 * 60 * 1000;
const buckets = new Map();

function roleSecret(role) {
  if (role === 'reviewer') return String(process.env.REVIEWER_HASH || '').trim().toLowerCase();
  if (role === 'content_editor') return String(process.env.CONTENT_EDITOR_HASH || '').trim().toLowerCase();
  return '';
}

function createToken(role) {
  const secret = roleSecret(role);
  if (!secret) throw new Error('role_secret_missing');
  const issuedAt = Date.now();
  const expiry = issuedAt + TTL_MS;
  const nonce = crypto.randomBytes(16).toString('hex');
  const payload = `${expiry}:${nonce}:${role}:${issuedAt}`;
  const sig = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}

function ipOf(req) {
  return String(req.headers?.['x-forwarded-for'] || '').split(',')[0].trim()
    || String(req.ip || req.socket?.remoteAddress || 'unknown');
}

function underLimit(req) {
  const now = Date.now();
  const key = ipOf(req);
  let entry = buckets.get(key);
  if (!entry || entry.resetAt <= now) entry = { count: 0, resetAt: now + WINDOW_MS };
  entry.count += 1;
  buckets.set(key, entry);
  return entry.count <= MAX;
}

function fallbackLoginGuard(req, res, next) {
  const admin = String(process.env.ADMIN_HASH || '').trim();
  const manager = String(process.env.MANAGER_HASH || '').trim();
  // Normal server.js login handles all configurations that include admin/manager.
  if (admin || manager) return next();

  const reviewer = roleSecret('reviewer');
  const editor = roleSecret('content_editor');
  if (!reviewer && !editor) return next();
  if (!underLimit(req)) return res.status(429).json({ ok: false, error: 'too_many_attempts' });

  const hash = String(req.body?.hash || '').trim().toLowerCase();
  const role = hash && reviewer && hash === reviewer
    ? 'reviewer'
    : (hash && editor && hash === editor ? 'content_editor' : null);
  if (!role) return res.status(401).json({ ok: false, error: 'invalid_credentials' });
  return res.json({ ok: true, token: createToken(role), role });
}

function resetForTests() { buckets.clear(); }

module.exports = { createToken, fallbackLoginGuard, resetForTests };
