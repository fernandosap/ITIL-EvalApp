'use strict';

const crypto = require('node:crypto');
const { withConnection, exec } = require('./db.js');

const fallback = new Map();

function keyHash(key) {
  return crypto.createHash('sha256').update(String(key)).digest('hex');
}

function consumeFallback(key, max, windowMs, now = Date.now()) {
  let row = fallback.get(key);
  if (!row || row.resetAt <= now) row = { count: 0, resetAt: now + windowMs };
  row.count += 1;
  fallback.set(key, row);
  return { allowed: row.count <= max, count: row.count, resetAt: row.resetAt, shared: false };
}

async function consumeShared(key, max, windowMs, now = Date.now()) {
  const hash = keyHash(key);
  try {
    return await withConnection(async (conn) => {
      let rows = await exec(conn,
        'SELECT BUCKET_KEY, HIT_COUNT, RESET_AT_MS FROM APP_RATE_LIMITS WHERE BUCKET_KEY = ? FOR UPDATE', [hash]);
      if (!rows.length) {
        try {
          await exec(conn,
            `INSERT INTO APP_RATE_LIMITS (BUCKET_KEY, HIT_COUNT, RESET_AT_MS, UPDATED_AT)
             VALUES (?, 1, ?, CURRENT_UTCTIMESTAMP)`, [hash, now + windowMs]);
          return { allowed: true, count: 1, resetAt: now + windowMs, shared: true };
        } catch (_e) {
          rows = await exec(conn,
            'SELECT BUCKET_KEY, HIT_COUNT, RESET_AT_MS FROM APP_RATE_LIMITS WHERE BUCKET_KEY = ? FOR UPDATE', [hash]);
        }
      }
      const row = rows[0];
      const resetAt = Number(row.RESET_AT_MS || 0);
      const expired = resetAt <= now;
      const nextCount = expired ? 1 : Number(row.HIT_COUNT || 0) + 1;
      const nextReset = expired ? now + windowMs : resetAt;
      await exec(conn,
        `UPDATE APP_RATE_LIMITS SET HIT_COUNT = ?, RESET_AT_MS = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP
          WHERE BUCKET_KEY = ?`, [nextCount, nextReset, hash]);
      return { allowed: nextCount <= max, count: nextCount, resetAt: nextReset, shared: true };
    }, { transaction: true });
  } catch (_e) {
    return consumeFallback(key, max, windowMs, now);
  }
}

function resetForTests() { fallback.clear(); }

module.exports = { consumeShared, consumeFallback, keyHash, resetForTests };
