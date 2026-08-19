'use strict';

// Admin audit log writer. Self-contained module so the server.js can
// stay focused on routes. Every admin action that has compliance or
// security implications should go through tryWriteAdminAudit() — it
// silently absorbs write failures so the request flow never breaks,
// BUT it tracks and exposes the failure count via getMetrics() and
// logs every failure. That way a missing ADMIN_AUDIT_LOG table, a HANA
// outage, or a permissions problem is *visible* in /api/admin/metrics
// instead of being silently swallowed.
//
// Schema: ADMIN_AUDIT_LOG (ACTION, TARGET_CODE, DETAILS_JSON, ACTOR,
// CLIENT_IP, CREATED_AT). The schema is detected lazily via a single
// SYS.TABLES query cached in this module — same pattern as before, just
// relocated.

const hana = require('@sap/hana-client');
const { getPool, acquireConn, releaseConn } = require('../shared/db-pool.js');

let _config = null;
let _hasAuditLogTable = null;
let _customDbFn = null;     // for tests
let _customHasTableFn = null; // for tests
let _customLog = null;       // for tests

const _metrics = {
  attempts: 0,        // every tryWriteAdminAudit() call
  writes: 0,          // successful inserts
  skippedNoTable: 0,  // ADMIN_AUDIT_LOG not present
  skippedNoDb: 0,     // HANA not configured
  failures: 0,        // exception during insert
  lastFailureAt: 0,
  lastFailureMessage: null,
  auditTablePresent: null   // last observed result of hasAuditLogTable
};

function init(config) {
  _config = {
    hasDbConfig: Boolean(config.hasDbConfig),
    hanaHost: config.hanaHost,
    hanaPort: config.hanaPort || '443',
    hanaUser: config.hanaUser,
    hanaPassword: config.hanaPassword,
    hanaSchema: config.hanaSchema || 'ITIL_EXAM',
    hanaEncrypt: config.hanaEncrypt !== false,
    hanaSslValidateCertificate: config.hanaSslValidateCertificate === true
  };
  // Reset cached schema flag when config changes (HANA rotate, env
  // update, etc).
  _hasAuditLogTable = null;
}

function _log(level, event, meta) {
  if (_customLog) return _customLog(level, event, meta);
  // Default: structured JSON to stdout. Mirrors appLog() in server.js.
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, event, ...(meta || {}) }));
}

function _dbConnect() {
  if (!_config || !_config.hasDbConfig) {
    throw new Error('HANA env vars are missing.');
  }
  const conn = hana.createConnection();
  conn.connect({
    serverNode: `${_config.hanaHost}:${_config.hanaPort}`,
    uid: _config.hanaUser,
    pwd: _config.hanaPassword,
    encrypt: _config.hanaEncrypt,
    sslValidateCertificate: _config.hanaSslValidateCertificate
  });
  return conn;
}

function _execQuery(conn, sql, params) {
  return new Promise((resolve, reject) => {
    conn.exec(sql, params || [], (err, rows) => {
      if (err) return reject(err);
      resolve(rows || []);
    });
  });
}

async function _withDb(fn) {
  // Opt-in pool via HANA_POOL_SIZE; otherwise open/close per call.
  if (_customDbFn) return _customDbFn(fn);
  const pool = getPool();
  let conn;
  let fromPool = false;
  if (pool) {
    try {
      conn = await acquireConn(pool);
      fromPool = true;
    } catch (err) {
      const msg = String((err && err.message) || err);
      if (!/maxConnectedOrPool/i.test(msg)) throw err;
      _log('warn', 'audit_pool_exhausted_fallback', { message: msg });
      conn = _dbConnect();
    }
  } else {
    conn = _dbConnect();
  }
  try {
    await _execQuery(conn, `SET SCHEMA "${_config.hanaSchema}"`);
    return await fn(conn);
  } finally {
    if (fromPool) releaseConn(pool, conn);
    else {
      try { conn.disconnect(); } catch (_e) { /* ignore */ }
    }
  }
}

async function _hasTable(conn) {
  if (_customHasTableFn) return _customHasTableFn(conn);
  if (_hasAuditLogTable !== null) return _hasAuditLogTable;
  const rows = await _execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLES
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ADMIN_AUDIT_LOG'`,
    [String(_config.hanaSchema || '').toUpperCase()]
  );
  _hasAuditLogTable = Number((rows && rows[0] && rows[0].CNT) || 0) > 0;
  return _hasAuditLogTable;
}

async function _writeOne(conn, entry) {
  const has = await _hasTable(conn);
  _metrics.auditTablePresent = has;  // observed value, even in tests
  if (!has) return 'no_table';
  await _execQuery(
    conn,
    `INSERT INTO ADMIN_AUDIT_LOG (ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT)
     VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP)`,
    [
      String(entry.action || 'unknown'),
      entry.targetCode ? String(entry.targetCode) : null,
      entry.details ? JSON.stringify(entry.details) : null,
      String(entry.actor || 'admin'),
      entry.clientIp ? String(entry.clientIp) : null
    ]
  );
  return 'ok';
}

// Public: write one audit entry. NEVER throws — silently tracks failures
// and exposes them via getMetrics() and /api/admin/metrics. The
// request flow continues normally even if the audit write fails.
//
// Returns one of: 'ok' | 'no_table' | 'no_db' | 'failed'.
async function tryWriteAdminAudit(entry) {
  _metrics.attempts += 1;
  if (!_config || !_config.hasDbConfig) {
    _metrics.skippedNoDb += 1;
    return 'no_db';
  }
  try {
    const result = await _withDb(async (conn) => _writeOne(conn, entry));
    if (result === 'no_table') {
      _metrics.skippedNoTable += 1;
      _log('warn', 'admin_audit_skipped_no_table', { action: entry && entry.action });
      return 'no_table';
    }
    _metrics.writes += 1;
    return 'ok';
  } catch (err) {
    _metrics.failures += 1;
    _metrics.lastFailureAt = Date.now();
    _metrics.lastFailureMessage = err && err.message ? err.message : String(err);
    _log('warn', 'admin_audit_write_failed', {
      action: entry && entry.action,
      message: _metrics.lastFailureMessage
    });
    return 'failed';
  }
}

// Public: snapshot of audit-write metrics. Exposed via /api/admin/metrics
// so operators can detect a quietly broken audit log (e.g. table dropped,
// HANA outage) without grepping logs.
function getMetrics() {
  return {
    attempts: _metrics.attempts,
    writes: _metrics.writes,
    skippedNoTable: _metrics.skippedNoTable,
    skippedNoDb: _metrics.skippedNoDb,
    failures: _metrics.failures,
    lastFailureAt: _metrics.lastFailureAt,
    lastFailureMessage: _metrics.lastFailureMessage,
    auditTablePresent: _metrics.auditTablePresent
  };
}

function _resetForTests() {
  _metrics.attempts = 0;
  _metrics.writes = 0;
  _metrics.skippedNoTable = 0;
  _metrics.skippedNoDb = 0;
  _metrics.failures = 0;
  _metrics.lastFailureAt = 0;
  _metrics.lastFailureMessage = null;
  _metrics.auditTablePresent = null;
  _hasAuditLogTable = null;
  _config = null;
  _customDbFn = null;
  _customHasTableFn = null;
  _customLog = null;
}

function _setDepsForTests(deps) {
  if (!deps) return;
  if (typeof deps.withDb === 'function') _customDbFn = deps.withDb;
  if (typeof deps.hasAuditLogTable === 'function') _customHasTableFn = deps.hasAuditLogTable;
  if (typeof deps.log === 'function') _customLog = deps.log;
}

module.exports = {
  init,
  tryWriteAdminAudit,
  getMetrics,
  _resetForTests,
  _setDepsForTests
};
