// HANA connection pool, opt-in via HANA_POOL_SIZE env var.
'use strict';

const hana = require('@sap/hana-client');

function readPoolConfig(env = process.env) {
  const size = Number(env.HANA_POOL_SIZE || 0);
  if (!Number.isFinite(size) || size <= 0) return null;
  return {
    maxConnectedOrPooled: Math.max(1, Math.floor(size)),
    pingCheck: String(env.HANA_POOL_PING_CHECK || '').toLowerCase() !== 'false',
    expirationInSeconds: Math.max(60, Number(env.HANA_POOL_TTL_SECONDS || 300))
  };
}

function readConnConfig(env = process.env) {
  return {
    serverNode: `${env.HANA_HOST || ''}:${env.HANA_PORT || '443'}`,
    uid: env.HANA_USER || '',
    pwd: env.HANA_PASSWORD || '',
    encrypt: String(env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
    sslValidateCertificate: String(env.HANA_SSL_VALIDATE_CERTIFICATE || 'true').toLowerCase() === 'true'
  };
}

let _pool = null;
let _poolConfigHash = null;

function clearPool(pool) {
  if (!pool || typeof pool.clear !== 'function') return;
  try { pool.clear(); } catch (_e) { /* swallow */ }
}

function shutdownPool() {
  const pool = _pool;
  _pool = null;
  _poolConfigHash = null;
  clearPool(pool);
}

function getPool(env = process.env) {
  const poolOpts = readPoolConfig(env);
  if (!poolOpts) return null;
  const connOpts = readConnConfig(env);
  const configHash = JSON.stringify({ p: poolOpts, c: connOpts });
  if (_pool && _poolConfigHash === configHash) return _pool;
  if (_pool) shutdownPool();
  _pool = hana.createPool(connOpts, poolOpts);
  _poolConfigHash = configHash;
  return _pool;
}

function acquireConn(pool) {
  if (!pool) throw new Error('acquireConn: pool is required');
  return new Promise((resolve, reject) => {
    pool.getConnection((err, conn) => {
      if (err) return reject(err);
      resolve(conn);
    });
  });
}

function releaseConn(_poolArg, conn) {
  if (!conn) return;
  try { conn.disconnect(); } catch (_e) { /* ignore */ }
}

function _resetForTests() { shutdownPool(); }

module.exports = {
  readPoolConfig,
  readConnConfig,
  getPool,
  acquireConn,
  releaseConn,
  clearPool,
  shutdownPool,
  _resetForTests
};