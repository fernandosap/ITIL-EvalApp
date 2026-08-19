// HANA connection pool, opt-in via HANA_POOL_SIZE env var.
//
// IMPORTANT — @sap/hana-client pool API:
//   const pool    = hana.createPool(connOpts, poolOpts);   // SYNC, returns ConnectionPool
//   const conn    = pool.getConnection();                  // SYNC, throws if full
//   const conn    = pool.getConnection(cb);                // ASYNC, QUEUES if full — preferred
//   conn.disconnect();                                     // returns conn to pool (or closes)
//
// There is no `pool.releaseConnection`. The pool reclaims connections
// when `conn.disconnect()` is called. `pool.clear(fn?)` closes everything.
//
// When HANA_POOL_SIZE is unset or 0, callers should fall back to opening
// a fresh connection per request (the original behavior of withDb()).
// When HANA_POOL_SIZE > 0, callers can use acquireConn()/releaseConn()
// to reuse a pool of connections.
//
// Concurrency: getPool() is sync; acquireConn() is async (uses the
// callback form so the pool queues requests when all slots are busy,
// instead of throwing "maxConnectedOrPool limit has been reached").
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

// Lazy init. Returns a pool, or null if pooling is disabled.
// Re-creates the pool if config has changed since the last call (so
// changing HANA_POOL_SIZE picks up on next request).
//
// SYNC: @sap/hana-client's createPool is synchronous. The previous
// implementation wrapped it in a Promise + callback, which hangs forever
// because the lib never invokes the callback.
function getPool(env = process.env) {
  const poolOpts = readPoolConfig(env);
  if (!poolOpts) return null;
  const connOpts = readConnConfig(env);
  const configHash = JSON.stringify({ p: poolOpts, c: connOpts });
  if (_pool && _poolConfigHash === configHash) return _pool;
  if (_pool) {
    // Config changed — close old pool, create new one.
    const oldPool = _pool;
    _pool = null;
    _poolConfigHash = null;
    try { oldPool.clear(); } catch (_e) { /* swallow close errors */ }
  }
  // SYNC API: createPool(connOpts, poolOpts) returns ConnectionPool directly.
  _pool = hana.createPool(connOpts, poolOpts);
  _poolConfigHash = configHash;
  return _pool;
}

// ASYNC: use the callback form so the pool can queue requests when all
// slots are in use. The SYNC form `pool.getConnection()` throws
// "maxConnectedOrPool limit has been reached" under load. The callback
// form blocks (in event-loop terms) until a slot opens, so a 10-concurrent
// burst on a 5-slot pool queues instead of 500-ing.
function acquireConn(pool) {
  if (!pool) throw new Error('acquireConn: pool is required');
  return new Promise((resolve, reject) => {
    pool.getConnection((err, conn) => {
      if (err) return reject(err);
      resolve(conn);
    });
  });
}

// SYNC: for pooled connections, disconnect() returns the conn to the
// pool. For non-pooled connections, disconnect() actually closes the
// underlying socket. We always call disconnect() because the same call
// is correct in both cases.
function releaseConn(pool, conn) {
  if (!conn) return;
  try { conn.disconnect(); } catch (_e) { /* ignore */ }
}

function clearPool(pool) {
  if (!pool || typeof pool.clear !== 'function') return;
  try { pool.clear(); } catch (_e) { /* swallow */ }
}

// For tests: reset the module-level state so each test starts clean.
function _resetForTests() {
  if (_pool) clearPool(_pool);
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
