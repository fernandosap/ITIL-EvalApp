// Read-only HANA schema/data inspection. No writes. No DDL. No row-level PII.
// Usage:
//   node scripts/inspect-hana.mjs
// Reads HANA env vars from .env (same as server.js).
// Emits JSON to stdout.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import hana from '@sap/hana-client';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');

// Minimal dotenv loader (no extra dependency).
function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  for (const raw of fs.readFileSync(filePath, 'utf8').split(/\r?\n/)) {
    const line = String(raw || '').trim();
    if (!line || line.startsWith('#')) continue;
    const i = line.indexOf('=');
    if (i <= 0) continue;
    const key = line.slice(0, i).trim();
    if (!key || process.env[key] != null) continue;
    let value = line.slice(i + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}
loadDotEnv(path.join(root, '.env'));

const env = {
  HANA_HOST: process.env.HANA_HOST,
  HANA_PORT: process.env.HANA_PORT || '443',
  HANA_USER: process.env.HANA_USER,
  HANA_PASSWORD: process.env.HANA_PASSWORD,
  HANA_SCHEMA: process.env.HANA_SCHEMA || 'ITIL_EXAM',
  HANA_ENCRYPT: String(process.env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
  HANA_SSL_VALIDATE_CERTIFICATE: String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'false').toLowerCase() === 'true'
};
if (!env.HANA_HOST || !env.HANA_USER || !env.HANA_PASSWORD) {
  console.error('Missing HANA env vars. Set HANA_HOST, HANA_USER, HANA_PASSWORD in .env');
  process.exit(1);
}

function exec(conn, sql, params = []) {
  return new Promise((resolve, reject) => {
    conn.exec(sql, params, (err, rows) => (err ? reject(err) : resolve(rows || [])));
  });
}

const conn = hana.createConnection();
await new Promise((resolve, reject) => {
  conn.connect({
    serverNode: `${env.HANA_HOST}:${env.HANA_PORT}`,
    uid: env.HANA_USER,
    pwd: env.HANA_PASSWORD,
    encrypt: env.HANA_ENCRYPT,
    sslValidateCertificate: env.HANA_SSL_VALIDATE_CERTIFICATE
  }, (err) => (err ? reject(err) : resolve()));
});
await exec(conn, `SET SCHEMA "${env.HANA_SCHEMA}"`);

const out = {};

// 1. Tables in ITIL_EXAM
out.tables = await exec(conn,
  `SELECT TABLE_NAME, TABLE_TYPE FROM SYS.TABLES WHERE SCHEMA_NAME = ? ORDER BY TABLE_NAME`,
  [env.HANA_SCHEMA]
);

// 2. Columns per table
const tableNames = out.tables.map((r) => r.TABLE_NAME);
out.columns = {};
for (const t of tableNames) {
  out.columns[t] = await exec(conn,
    `SELECT COLUMN_NAME, DATA_TYPE_NAME, IS_NULLABLE, LENGTH
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
      ORDER BY POSITION`,
    [env.HANA_SCHEMA, t]
  );
}

// 3. Optional columns / runtime-detected features
const optChecks = {
  'ACCESS_CODES.NOTES': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='ACCESS_CODES' AND COLUMN_NAME='NOTES'`,
  'ACCESS_CODES.QUESTION_SET_ID': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='ACCESS_CODES' AND COLUMN_NAME='QUESTION_SET_ID'`,
  'ACCESS_CODES.DELETED_AT': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='ACCESS_CODES' AND COLUMN_NAME='DELETED_AT'`,
  'ACCESS_CODES.DELETED_BY': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='ACCESS_CODES' AND COLUMN_NAME='DELETED_BY'`,
  'QUESTION_SETS.EXAM_MODE': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='EXAM_MODE'`,
  'QUESTION_SETS.SHOW_CORRECT_ANSWERS': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='SHOW_CORRECT_ANSWERS'`,
  'QUESTION_SETS.COUNTS_TOWARD_RESULTS': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='COUNTS_TOWARD_RESULTS'`,
  'QUESTION_SETS.VERSION_GROUP_ID': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='VERSION_GROUP_ID'`,
  'QUESTION_SETS.VERSION_NUMBER': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='VERSION_NUMBER'`,
  'QUESTION_SETS.LIFECYCLE_STATUS': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='LIFECYCLE_STATUS'`,
  'QUESTION_SETS.PARENT_QUESTION_SET_ID': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='PARENT_QUESTION_SET_ID'`,
  'QUESTION_SETS.IMPORT_SOURCE': `SELECT COUNT(*) AS N FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME=? AND TABLE_NAME='QUESTION_SETS' AND COLUMN_NAME='IMPORT_SOURCE'`,
  'ADMIN_AUDIT_LOG present': `SELECT COUNT(*) AS N FROM SYS.TABLES WHERE SCHEMA_NAME=? AND TABLE_NAME='ADMIN_AUDIT_LOG'`,
  'APP_SETTINGS present': `SELECT COUNT(*) AS N FROM SYS.TABLES WHERE SCHEMA_NAME=? AND TABLE_NAME='APP_SETTINGS'`
};
out.optionalColumns = {};
for (const [label, sql] of Object.entries(optChecks)) {
  const r = await exec(conn, sql, [env.HANA_SCHEMA]);
  out.optionalColumns[label] = Number(r[0]?.N || 0) > 0;
}

// 4. Row counts per table
out.rowCounts = {};
for (const t of tableNames) {
  try {
    const r = await exec(conn, `SELECT COUNT(*) AS N FROM "${env.HANA_SCHEMA}"."${t}"`);
    out.rowCounts[t] = Number(r[0]?.N || 0);
  } catch (e) {
    out.rowCounts[t] = `ERR: ${String(e.message || '').slice(0, 100)}`;
  }
}

// 5. APP_SETTINGS keys + timestamps (no values)
out.appSettings = await exec(conn,
  `SELECT SETTING_KEY, UPDATED_AT FROM "${env.HANA_SCHEMA}".APP_SETTINGS ORDER BY SETTING_KEY`
);

// 6. QUESTION_SETS aggregate
out.questionSetStats = await exec(conn,
  `SELECT IS_ACTIVE, LIFECYCLE_STATUS, EXAM_MODE, COUNT(*) AS N
     FROM "${env.HANA_SCHEMA}".QUESTION_SETS
    GROUP BY IS_ACTIVE, LIFECYCLE_STATUS, EXAM_MODE
    ORDER BY IS_ACTIVE DESC, N DESC`
);

// 7. EXAM_RESULTS aggregate by mode + result
out.examResultsStats = await exec(conn,
  `SELECT
     COALESCE(JSON_VALUE(RESULT_JSON, '$.examMode'), 'GRADED') AS MODE,
     CASE WHEN PASS = TRUE THEN 'PASS' WHEN PASS = FALSE THEN 'FAIL' ELSE 'NULL' END AS RESULT,
     COUNT(*) AS N,
     AVG(CASE WHEN PCT IS NOT NULL THEN PCT END) AS AVG_PCT
     FROM "${env.HANA_SCHEMA}".EXAM_RESULTS
    GROUP BY COALESCE(JSON_VALUE(RESULT_JSON, '$.examMode'), 'GRADED'),
             CASE WHEN PASS = TRUE THEN 'PASS' WHEN PASS = FALSE THEN 'FAIL' ELSE 'NULL' END
    ORDER BY MODE, RESULT`
);

// 8. ACCESS_CODES by status
out.accessCodeStats = await exec(conn,
  `SELECT STATUS, COUNT(*) AS N FROM "${env.HANA_SCHEMA}".ACCESS_CODES
    GROUP BY STATUS ORDER BY N DESC`
);

// 9. EXAM_SESSIONS total + stale > 30min
out.examSessionsStats = await exec(conn,
  `SELECT
     COUNT(*) AS TOTAL,
     SUM(CASE WHEN UPDATED_AT < ADD_SECONDS(CURRENT_UTCTIMESTAMP, -1800) THEN 1 ELSE 0 END) AS STALE_30M
     FROM "${env.HANA_SCHEMA}".EXAM_SESSIONS`
);

// 10. ADMIN_AUDIT_LOG total + last activity + events by month (last 6)
out.auditStats = {
  total: (await exec(conn, `SELECT COUNT(*) AS N FROM "${env.HANA_SCHEMA}".ADMIN_AUDIT_LOG`))[0].N,
  lastAt: (await exec(conn, `SELECT MAX(CREATED_AT) AS LAST_AT FROM "${env.HANA_SCHEMA}".ADMIN_AUDIT_LOG`))[0].LAST_AT,
  byMonth: await exec(conn,
    `SELECT TO_VARCHAR(CREATED_AT, 'YYYY-MM') AS MONTH, COUNT(*) AS N
       FROM "${env.HANA_SCHEMA}".ADMIN_AUDIT_LOG
      WHERE CREATED_AT >= ADD_MONTHS(CURRENT_UTCTIMESTAMP, -6)
      GROUP BY TO_VARCHAR(CREATED_AT, 'YYYY-MM') ORDER BY MONTH`
  )
};

conn.disconnect();
console.log(JSON.stringify(out, null, 2));
