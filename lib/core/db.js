'use strict';

const hana = require('@sap/hana-client');
const {
  readConnConfig,
  getPool,
  acquireConn,
  releaseConn
} = require('../../shared/db-pool.js');

function config(env = process.env) {
  const schema = String(env.HANA_SCHEMA || 'ITIL_EXAM').toUpperCase();
  if (!env.HANA_HOST || !env.HANA_USER || !env.HANA_PASSWORD) return null;
  if (!/^[A-Z0-9_]+$/.test(schema)) throw new Error('invalid_hana_schema');
  return { schema, connection: readConnConfig(env) };
}

function exec(conn, sql, params = []) {
  return new Promise((resolve, reject) => conn.exec(sql, params, (err, rows) => err ? reject(err) : resolve(rows || [])));
}

function commit(conn) { return new Promise((resolve, reject) => conn.commit((err) => err ? reject(err) : resolve())); }
function rollback(conn) { return new Promise((resolve) => conn.rollback(() => resolve())); }

function openConnection(env = process.env, transaction = false) {
  const cfg = config(env);
  if (!cfg) throw new Error('hana_not_configured');
  const conn = hana.createConnection();
  if (transaction) conn.setAutoCommit(false);
  conn.connect(cfg.connection);
  return { conn, cfg };
}

async function withConnection(fn, options = {}) {
  const env = options.env || process.env;
  const cfg = config(env);
  if (!cfg) throw new Error('hana_not_configured');
  const transaction = Boolean(options.transaction);
  // Long-lived/transactional operations intentionally use a dedicated
  // connection. Read-only/short operations share the same process pool used
  // by legacy server.js so all new code follows one connection policy.
  const pool = transaction ? null : getPool(env);
  let conn;
  let pooled = false;
  if (pool) {
    conn = await acquireConn(pool);
    pooled = true;
  } else {
    ({ conn } = openConnection(env, transaction));
  }
  try {
    await exec(conn, `SET SCHEMA "${cfg.schema}"`);
    const value = await fn(conn);
    if (transaction) await commit(conn);
    return value;
  } catch (err) {
    if (transaction) await rollback(conn);
    throw err;
  } finally {
    if (pooled) releaseConn(pool, conn);
    else try { conn.disconnect(); } catch (_e) { /* ignore close errors */ }
  }
}

async function currentIdentity(conn) {
  const rows = await exec(conn, 'SELECT CURRENT_IDENTITY_VALUE() AS ID FROM DUMMY');
  const value = Number(rows?.[0]?.ID);
  if (!Number.isInteger(value) || value <= 0) throw new Error('identity_not_returned');
  return value;
}

async function tableExists(conn, tableName) {
  const rows = await exec(conn,
    'SELECT COUNT(*) AS CNT FROM SYS.TABLES WHERE SCHEMA_NAME = CURRENT_SCHEMA AND TABLE_NAME = ?',
    [String(tableName || '').toUpperCase()]);
  return Number(rows?.[0]?.CNT || 0) > 0;
}

async function columnExists(conn, tableName, columnName) {
  const rows = await exec(conn,
    'SELECT COUNT(*) AS CNT FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME = CURRENT_SCHEMA AND TABLE_NAME = ? AND COLUMN_NAME = ?',
    [String(tableName || '').toUpperCase(), String(columnName || '').toUpperCase()]);
  return Number(rows?.[0]?.CNT || 0) > 0;
}

async function createIfMissing(conn, tableName, sql) {
  if (await tableExists(conn, tableName)) return;
  try { await exec(conn, sql); }
  catch (err) { if (!(await tableExists(conn, tableName))) throw err; }
}

async function addColumnIfMissing(conn, tableName, columnName, ddl) {
  if (!(await tableExists(conn, tableName)) || await columnExists(conn, tableName, columnName)) return;
  try { await exec(conn, `ALTER TABLE ${tableName} ADD (${ddl})`); }
  catch (err) { if (!(await columnExists(conn, tableName, columnName))) throw err; }
}

function parseJson(value) {
  if (value == null || value === '') return null;
  try { return typeof value === 'string' ? JSON.parse(value) : JSON.parse(String(value)); }
  catch (_e) { return null; }
}

async function ensureAdminFeedbackColumns(conn) {
  if (!(await tableExists(conn, 'ACCESS_CODES'))) return;
  await addColumnIfMissing(conn, 'ACCESS_CODES', 'ATTEMPT_QUESTION_SET_ID', 'ATTEMPT_QUESTION_SET_ID BIGINT');
  await addColumnIfMissing(conn, 'ACCESS_CODES', 'ATTEMPT_QUESTION_SET_NAME', 'ATTEMPT_QUESTION_SET_NAME NVARCHAR(255)');
  await addColumnIfMissing(conn, 'ACCESS_CODES', 'ATTEMPT_QUESTION_SET_VERSION', 'ATTEMPT_QUESTION_SET_VERSION INTEGER');
  await addColumnIfMissing(conn, 'ACCESS_CODES', 'ARCHIVED_AT', 'ARCHIVED_AT TIMESTAMP');
}

async function backfillAttemptSnapshots(conn) {
  if (!(await tableExists(conn, 'ACCESS_CODES'))) return 0;
  if (!(await columnExists(conn, 'ACCESS_CODES', 'ATTEMPT_QUESTION_SET_ID'))) return 0;
  const hasResults = await tableExists(conn, 'EXAM_RESULTS');
  const hasSessions = await tableExists(conn, 'EXAM_SESSIONS');
  const hasSets = await tableExists(conn, 'QUESTION_SETS');
  const joins = [
    hasResults ? 'LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE' : '',
    hasSessions ? 'LEFT JOIN EXAM_SESSIONS s ON s.ACCESS_CODE = c.ACCESS_CODE' : ''
  ].filter(Boolean).join('\n');
  const rows = await exec(conn,
    `SELECT c.ACCESS_CODE, c.STATUS, c.QUESTION_SET_ID,
            ${hasResults ? 'r.RESULT_JSON' : 'NULL AS RESULT_JSON'},
            ${hasSessions ? 's.SESSION_JSON' : 'NULL AS SESSION_JSON'}
       FROM ACCESS_CODES c
       ${joins}
      WHERE c.ATTEMPT_QUESTION_SET_ID IS NULL
        AND c.STATUS IN ('active', 'completed')`);
  if (!rows.length) return 0;

  const sets = new Map();
  if (hasSets) {
    const setRows = await exec(conn, 'SELECT QUESTION_SET_ID, NAME, VERSION_NUMBER FROM QUESTION_SETS');
    for (const row of setRows) sets.set(Number(row.QUESTION_SET_ID), row);
  }

  let updated = 0;
  for (const row of rows) {
    const result = parseJson(row.RESULT_JSON) || {};
    const saved = parseJson(row.SESSION_JSON) || {};
    const session = saved.session || saved;
    const historical = row.STATUS === 'completed' ? result : session;
    const rawId = historical?.questionSetId ?? result?.questionSetId ?? session?.questionSetId ?? row.QUESTION_SET_ID;
    const id = Number(rawId);
    if (!Number.isInteger(id) || id <= 0) continue;
    const set = sets.get(id) || {};
    const name = String(historical?.questionSetName || result?.questionSetName || session?.questionSetName || set.NAME || '').trim();
    const versionRaw = historical?.questionSetVersion ?? result?.questionSetVersion ?? session?.questionSetVersion ?? set.VERSION_NUMBER;
    const version = Number(versionRaw);
    await exec(conn,
      `UPDATE ACCESS_CODES
          SET ATTEMPT_QUESTION_SET_ID = ?,
              ATTEMPT_QUESTION_SET_NAME = ?,
              ATTEMPT_QUESTION_SET_VERSION = ?,
              UPDATED_AT = CURRENT_UTCTIMESTAMP
        WHERE ACCESS_CODE = ? AND ATTEMPT_QUESTION_SET_ID IS NULL`,
      [id, name || null, Number.isInteger(version) && version > 0 ? version : null, row.ACCESS_CODE]);
    updated += 1;
  }
  return updated;
}

async function createProctorIncidentTable(conn) {
  if (await tableExists(conn, 'EXAM_PROCTOR_INCIDENTS')) return;
  const withForeignKey = `CREATE TABLE EXAM_PROCTOR_INCIDENTS (
      INCIDENT_ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      ACCESS_CODE NVARCHAR(6) NOT NULL,
      EVENT_HASH NVARCHAR(64) NOT NULL,
      EVENT_TYPE NVARCHAR(80) NOT NULL,
      DETAIL NVARCHAR(1000),
      CLIENT_TIME NVARCHAR(64),
      SERVER_TIME TIMESTAMP DEFAULT CURRENT_UTCTIMESTAMP NOT NULL,
      CONSTRAINT FK_PROCTOR_INCIDENT_CODE FOREIGN KEY (ACCESS_CODE)
        REFERENCES ACCESS_CODES (ACCESS_CODE) ON DELETE CASCADE,
      CONSTRAINT UQ_PROCTOR_INCIDENT_HASH UNIQUE (ACCESS_CODE, EVENT_HASH)
    )`;
  const withoutForeignKey = `CREATE TABLE EXAM_PROCTOR_INCIDENTS (
      INCIDENT_ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      ACCESS_CODE NVARCHAR(6) NOT NULL,
      EVENT_HASH NVARCHAR(64) NOT NULL,
      EVENT_TYPE NVARCHAR(80) NOT NULL,
      DETAIL NVARCHAR(1000),
      CLIENT_TIME NVARCHAR(64),
      SERVER_TIME TIMESTAMP DEFAULT CURRENT_UTCTIMESTAMP NOT NULL,
      CONSTRAINT UQ_PROCTOR_INCIDENT_HASH UNIQUE (ACCESS_CODE, EVENT_HASH)
    )`;
  try {
    await exec(conn, withForeignKey);
  } catch (err) {
    if (await tableExists(conn, 'EXAM_PROCTOR_INCIDENTS')) return;
    if (!/insufficient privilege/i.test(String(err.message || ''))) throw err;
    await exec(conn, withoutForeignKey);
  }
}

async function ensureRuntimeTables(conn) {
  await createProctorIncidentTable(conn);
  await ensureAdminFeedbackColumns(conn);

  await createIfMissing(conn, 'ADMIN_SSO_SESSIONS_V3',
    `CREATE TABLE ADMIN_SSO_SESSIONS_V3 (
      SESSION_ID NVARCHAR(128) PRIMARY KEY,
      KEY_ID NVARCHAR(64) NOT NULL,
      TOKEN_CIPHERTEXT NCLOB NOT NULL,
      TOKEN_IV NVARCHAR(64) NOT NULL,
      TOKEN_TAG NVARCHAR(64) NOT NULL,
      SUBJECT NVARCHAR(255),
      ROLE_NAME NVARCHAR(40) NOT NULL,
      ISSUED_AT_MS BIGINT NOT NULL,
      EXPIRES_AT_MS BIGINT NOT NULL,
      CREATED_AT TIMESTAMP DEFAULT CURRENT_UTCTIMESTAMP NOT NULL,
      LAST_SEEN_AT TIMESTAMP DEFAULT CURRENT_UTCTIMESTAMP NOT NULL
    )`);

  await createIfMissing(conn, 'APP_RATE_LIMITS',
    `CREATE TABLE APP_RATE_LIMITS (
      BUCKET_KEY NVARCHAR(64) PRIMARY KEY,
      HIT_COUNT INTEGER NOT NULL,
      RESET_AT_MS BIGINT NOT NULL,
      UPDATED_AT TIMESTAMP DEFAULT CURRENT_UTCTIMESTAMP NOT NULL
    )`);

  await createIfMissing(conn, 'APP_MUTEX',
    `CREATE TABLE APP_MUTEX (
      LOCK_NAME NVARCHAR(80) PRIMARY KEY,
      UPDATED_AT TIMESTAMP DEFAULT CURRENT_UTCTIMESTAMP NOT NULL
    )`);
  const lockRows = await exec(conn, "SELECT COUNT(*) AS CNT FROM APP_MUTEX WHERE LOCK_NAME = 'question_sets'");
  if (Number(lockRows?.[0]?.CNT || 0) === 0) {
    await exec(conn, "INSERT INTO APP_MUTEX (LOCK_NAME, UPDATED_AT) VALUES ('question_sets', CURRENT_UTCTIMESTAMP)");
  }

  await backfillAttemptSnapshots(conn);
}

async function purgeLegacySsoTables(conn) {
  for (const table of ['ADMIN_SSO_SESSIONS', 'ADMIN_SSO_SESSIONS_V2']) {
    if (await tableExists(conn, table)) {
      try { await exec(conn, `DELETE FROM ${table}`); } catch (_e) { /* migration hygiene only */ }
    }
  }
}

async function validateRuntimeSchema(conn) {
  const required = ['EXAM_PROCTOR_INCIDENTS', 'ADMIN_SSO_SESSIONS_V3', 'APP_RATE_LIMITS', 'APP_MUTEX'];
  const missing = [];
  for (const table of required) if (!(await tableExists(conn, table))) missing.push(table);
  if (missing.length) throw new Error(`runtime_schema_missing:${missing.join(',')}`);
  const lockRows = await exec(conn, "SELECT COUNT(*) AS CNT FROM APP_MUTEX WHERE LOCK_NAME = 'question_sets'");
  if (Number(lockRows?.[0]?.CNT || 0) !== 1) throw new Error('runtime_schema_missing:APP_MUTEX.question_sets');
  if (await tableExists(conn, 'ACCESS_CODES')) {
    for (const column of ['ATTEMPT_QUESTION_SET_ID', 'ATTEMPT_QUESTION_SET_NAME', 'ATTEMPT_QUESTION_SET_VERSION', 'ARCHIVED_AT']) {
      if (!(await columnExists(conn, 'ACCESS_CODES', column))) throw new Error(`runtime_schema_missing:ACCESS_CODES.${column}`);
    }
  }
  return { ok: true, tables: required };
}

async function initializeRuntimeSchema(env = process.env) {
  // HANA DDL may commit implicitly; do not wrap schema creation in an explicit
  // application transaction. Data mutations still use explicit transactions.
  return withConnection(async (conn) => {
    await ensureRuntimeTables(conn);
    await purgeLegacySsoTables(conn);
    return validateRuntimeSchema(conn);
  }, { env });
}

async function acquireRowLock(lockName, env = process.env) {
  const { conn, cfg } = openConnection(env, true);
  let released = false;
  try {
    await exec(conn, `SET SCHEMA "${cfg.schema}"`);
    const rows = await exec(conn, 'SELECT LOCK_NAME FROM APP_MUTEX WHERE LOCK_NAME = ? FOR UPDATE', [String(lockName)]);
    if (!rows.length) throw new Error('distributed_lock_not_initialized');
  } catch (err) {
    try { await rollback(conn); } catch (_e) { /* ignore */ }
    try { conn.disconnect(); } catch (_e) { /* ignore */ }
    throw err;
  }
  return async function release(ok = true) {
    if (released) return;
    released = true;
    try { if (ok) await commit(conn); else await rollback(conn); }
    finally { try { conn.disconnect(); } catch (_e) { /* ignore */ }
  };
}

module.exports = {
  config,
  exec,
  withConnection,
  currentIdentity,
  tableExists,
  columnExists,
  ensureAdminFeedbackColumns,
  backfillAttemptSnapshots,
  ensureRuntimeTables,
  purgeLegacySsoTables,
  validateRuntimeSchema,
  initializeRuntimeSchema,
  acquireRowLock
};
