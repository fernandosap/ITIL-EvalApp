// HANA connection pool, opt-in via HANA_POOL_SIZE env var.
// When HANA_POOL_SIZE is unset or 0, callers should fall back to opening
// a fresh connection per request (the original behavior of withDb()).
// When HANA_POOL_SIZE > 0, callers can use acquireConn()/releaseConn()
// to reuse a pool of connections.
'use strict';

const hana = require('@sap/hana-client');

// Read pool config from env. Returns null if pooling is disabled.
function readPoolConfig(env = process.env) {
  const size = Number(env.HANA_POOL_SIZE || 0);
  if (!Number.isFinite(size) || size <= 0) return null;
  return {
    maxConnectedOrPooled: Math.max(1, Math.floor(size)),
    pingCheck: String(env.HANA_POOL_PING_CHECK || '').toLowerCase() !== 'false',
    // Default 5 min — HANA sometimes kills idle conns, so don't keep them
    // open forever.
    expirationInSeconds: Math.max(60, Number(env.HANA_POOL_TTL_SECONDS || 300))
  };
}

function readConnConfig(env = process.env) {
  return {
    serverNode: `${env.HANA_HOST || ''}:${env.HANA_PORT || '443'}`,
    uid: env.HANA_USER || '',
    pwd: env.HANA_PASSWORD || '',
    encrypt: String(env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
    sslValidateCertificate: String(env.HANA_SSL_VALIDATE_CERTIFICATE || 'false').toLowerCase() === 'true'
  };
}

let _pool = null;
let _poolConfigHash = null;

// Lazy init. Returns a promise that resolves to the pool, or null if
// pooling is disabled. Re-creates the pool if config has changed since
// the last call (so changing HANA_POOL_SIZE picks up on next request).
function getPool(env = process.env) {
  const poolOpts = readPoolConfig(env);
  if (!poolOpts) return Promise.resolve(null);
  const connOpts = readConnConfig(env);
  const configHash = JSON.stringify({ p: poolOpts, c: connOpts });
  if (_pool && _poolConfigHash === configHash) return Promise.resolve(_pool);
  if (_pool) {
    // Config changed — close old pool, create new one.
    const oldPool = _pool;
    _pool = null;
    _poolConfigHash = null;
    clearPool(oldPool).catch(() => { /* swallow close errors */ });
  }
  return new Promise((resolve, reject) => {
    hana.createPool(Object.assign({}, connOpts, { pool: poolOpts }), (err, pool) => {
      if (err) return reject(err);
      _pool = pool;
      _poolConfigHash = configHash;
      resolve(pool);
    });
  });
}

function acquireConn(pool) {
  return new Promise((resolve, reject) => {
    pool.getConnection((err, conn) => err ? reject(err) : resolve(conn));
  });
}

function releaseConn(pool, conn) {
  return new Promise((resolve) => {
    // Per the @sap/hana-client docs: if releaseConnection returns false,
    // the connection is already broken and will be discarded by the pool.
    pool.releaseConnection(conn, () => resolve());
  });
}

function clearPool(pool) {
  return new Promise((resolve) => {
    if (!pool || typeof pool.clear !== 'function') return resolve();
    pool.clear(() => resolve());
  });
}

// For tests: reset the module-level state so each test starts clean.
function _resetForTests() {
  _pool = null;
  _poolConfigHash = null;
}

module.exports = {
  readPoolConfig,
  readConnConfig,
  getPool,
  acquireConn,
  releaseConn,
  clearPool,
  _resetForTests
};
