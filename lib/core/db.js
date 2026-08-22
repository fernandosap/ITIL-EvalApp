'use strict';

const hana = require('@sap/hana-client');

function config(env = process.env) {
  const schema = String(env.HANA_SCHEMA || 'ITIL_EXAM').toUpperCase();
  if (!env.HANA_HOST || !env.HANA_USER || !env.HANA_PASSWORD) return null;
  if (!/^[A-Z0-9_]+$/.test(schema)) throw new Error('invalid_hana_schema');
  return {
    schema,
    connection: {
      serverNode: `${env.HANA_HOST}:${env.HANA_PORT || '443'}`,
      uid: env.HANA_USER,
      pwd: env.HANA_PASSWORD,
      encrypt: String(env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
      sslValidateCertificate: String(env.HANA_SSL_VALIDATE_CERTIFICATE || 'true').toLowerCase() === 'true'
    }
  };
}

function exec(conn, sql, params = []) {
  return new Promise((resolve, reject) => {
    conn.exec(sql, params, (err, rows) => err ? reject(err) : resolve(rows || []));
  });
}

function commit(conn) {
  return new Promise((resolve, reject) => conn.commit((err) => err ? reject(err) : resolve()));
}

function rollback(conn) {
  return new Promise((resolve) => conn.rollback(() => resolve()));
}

async function withConnection(fn, options = {}) {
  const cfg = config(options.env || process.env);
  if (!cfg) throw new Error('hana_not_configured');
  const conn = hana.createConnection();
  if (options.transaction) conn.setAutoCommit(false);
  conn.connect(cfg.connection);
  try {
    await exec(conn, `SET SCHEMA "${cfg.schema}"`);
    const value = await fn(conn);
    if (options.transaction) await commit(conn);
    return value;
  } catch (err) {
    if (options.transaction) await rollback(conn);
    throw err;
  } finally {
    try { conn.disconnect(); } catch (_e) { /* ignore close errors */ }
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
    `SELECT COUNT(*) AS CNT FROM SYS.TABLES WHERE SCHEMA_NAME = CURRENT_SCHEMA AND TABLE_NAME = ?`,
    [String(tableName || '').toUpperCase()]);
  return Number(rows?.[0]?.CNT || 0) > 0;
}

module.exports = { config, exec, withConnection, currentIdentity, tableExists };
