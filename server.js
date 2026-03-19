/* eslint-disable no-console */
'use strict';

const fs = require('fs');
const path = require('path');
const express = require('express');
const hana = require('@sap/hana-client');

const app = express();
const PORT = process.env.PORT || 3000;

const HANA_HOST = process.env.HANA_HOST;
const HANA_PORT = process.env.HANA_PORT || '443';
const HANA_USER = process.env.HANA_USER;
const HANA_PASSWORD = process.env.HANA_PASSWORD;
const HANA_SCHEMA = process.env.HANA_SCHEMA || 'ITIL_EXAM';
const HANA_ENCRYPT = String(process.env.HANA_ENCRYPT || 'true').toLowerCase() === 'true';
const HANA_SSL_VALIDATE_CERTIFICATE =
  String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'false').toLowerCase() === 'true';

const HAS_DB_CONFIG = Boolean(HANA_HOST && HANA_USER && HANA_PASSWORD && HANA_SCHEMA);

const INDEX_PATH = path.join(__dirname, 'index.html');

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: false }));

function dbConnect() {
  if (!HAS_DB_CONFIG) {
    throw new Error('HANA env vars are missing.');
  }
  const conn = hana.createConnection();
  conn.connect({
    serverNode: `${HANA_HOST}:${HANA_PORT}`,
    uid: HANA_USER,
    pwd: HANA_PASSWORD,
    encrypt: HANA_ENCRYPT,
    sslValidateCertificate: HANA_SSL_VALIDATE_CERTIFICATE
  });
  return conn;
}

function execQuery(conn, sql, params = []) {
  return new Promise((resolve, reject) => {
    conn.exec(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows || []);
    });
  });
}

function closeConn(conn) {
  return new Promise((resolve) => {
    try {
      conn.disconnect();
    } catch (_e) {
      // ignore
    }
    resolve();
  });
}

function parseJsonOrNull(s) {
  if (!s) return null;
  try {
    return JSON.parse(s);
  } catch (_e) {
    return null;
  }
}

function appLog(level, event, meta = {}) {
  const payload = {
    ts: new Date().toISOString(),
    level,
    event,
    ...meta
  };
  console.log(JSON.stringify(payload));
}

function toCsvCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return `"${s.replace(/"/g, '""')}"`;
}

let _hasNotesColumn = null;
async function hasNotesColumn(conn) {
  if (_hasNotesColumn !== null) return _hasNotesColumn;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ACCESS_CODES'
        AND COLUMN_NAME = 'NOTES'`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  const cnt = rows && rows[0] ? Number(rows[0].CNT || 0) : 0;
  _hasNotesColumn = cnt > 0;
  return _hasNotesColumn;
}

async function withDb(fn) {
  const conn = dbConnect();
  try {
    await execQuery(conn, `SET SCHEMA "${HANA_SCHEMA}"`);
    return await fn(conn);
  } finally {
    await closeConn(conn);
  }
}

app.get('/api/health', async (_req, res) => {
  if (!HAS_DB_CONFIG) {
    res.status(500).json({ ok: false, message: 'Missing HANA env vars.' });
    return;
  }
  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'SELECT 1 AS OK FROM DUMMY');
    });
    res.json({ ok: true, db: 'connected', schema: HANA_SCHEMA });
  } catch (err) {
    appLog('error', 'health_failed', { message: err.message });
    res.status(500).json({ ok: false, message: err.message });
  }
});

app.get('/api/bootstrap', async (_req, res) => {
  try {
    const payload = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const codeRows = await execQuery(
        conn,
        `SELECT ACCESS_CODE, LABEL, ${hasNotes ? 'NOTES,' : ''} STATUS, CREATED_AT, SCORE, PCT, PASS
         FROM ACCESS_CODES`
      );
      const sessionRows = await execQuery(
        conn,
        `SELECT ACCESS_CODE, SESSION_JSON
         FROM EXAM_SESSIONS`
      );
      const resultRows = await execQuery(
        conn,
        `SELECT ACCESS_CODE, RESULT_JSON
         FROM EXAM_RESULTS`
      );

      const codebook = {};
      for (const r of codeRows) {
        codebook[r.ACCESS_CODE] = {
          label: r.LABEL || null,
          notes: hasNotes ? (r.NOTES || '') : '',
          status: r.STATUS || 'unused',
          createdAt: r.CREATED_AT ? new Date(r.CREATED_AT).toISOString() : null,
          score: r.SCORE === null ? undefined : r.SCORE,
          pct: r.PCT === null ? undefined : r.PCT,
          pass: r.PASS === null ? undefined : Boolean(r.PASS)
        };
      }

      const sessions = {};
      for (const r of sessionRows) {
        const parsed = parseJsonOrNull(r.SESSION_JSON);
        if (parsed) sessions[r.ACCESS_CODE] = parsed;
      }

      const results = {};
      for (const r of resultRows) {
        const parsed = parseJsonOrNull(r.RESULT_JSON);
        if (parsed) results[r.ACCESS_CODE] = parsed;
      }

      return { codebook, sessions, results };
    });

    res.json(payload);
  } catch (err) {
    appLog('error', 'bootstrap_failed', { message: err.message });
    res.status(500).json({ error: 'bootstrap_failed', message: err.message });
  }
});

app.put('/api/codebook', async (req, res) => {
  const codebook = req.body && req.body.codebook ? req.body.codebook : null;
  if (!codebook || typeof codebook !== 'object') {
    res.status(400).json({ error: 'invalid_payload' });
    return;
  }

  try {
    await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const sql = hasNotes
        ? `
          MERGE INTO ACCESS_CODES T
          USING (SELECT ? AS ACCESS_CODE, ? AS LABEL, ? AS NOTES, ? AS STATUS, ? AS SCORE, ? AS PCT, ? AS PASS FROM DUMMY) S
          ON (T.ACCESS_CODE = S.ACCESS_CODE)
          WHEN MATCHED THEN UPDATE SET
            T.LABEL = S.LABEL,
            T.NOTES = S.NOTES,
            T.STATUS = S.STATUS,
            T.SCORE = S.SCORE,
            T.PCT = S.PCT,
            T.PASS = S.PASS,
            T.UPDATED_AT = CURRENT_UTCTIMESTAMP
          WHEN NOT MATCHED THEN INSERT
            (ACCESS_CODE, LABEL, NOTES, STATUS, SCORE, PCT, PASS, CREATED_AT, UPDATED_AT)
            VALUES (S.ACCESS_CODE, S.LABEL, S.NOTES, S.STATUS, S.SCORE, S.PCT, S.PASS, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)
        `
        : `
          MERGE INTO ACCESS_CODES T
          USING (SELECT ? AS ACCESS_CODE, ? AS LABEL, ? AS STATUS, ? AS SCORE, ? AS PCT, ? AS PASS FROM DUMMY) S
          ON (T.ACCESS_CODE = S.ACCESS_CODE)
          WHEN MATCHED THEN UPDATE SET
            T.LABEL = S.LABEL,
            T.STATUS = S.STATUS,
            T.SCORE = S.SCORE,
            T.PCT = S.PCT,
            T.PASS = S.PASS,
            T.UPDATED_AT = CURRENT_UTCTIMESTAMP
          WHEN NOT MATCHED THEN INSERT
            (ACCESS_CODE, LABEL, STATUS, SCORE, PCT, PASS, CREATED_AT, UPDATED_AT)
            VALUES (S.ACCESS_CODE, S.LABEL, S.STATUS, S.SCORE, S.PCT, S.PASS, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)
        `;

      for (const [code, v] of Object.entries(codebook)) {
        const row = v || {};
        const params = hasNotes
          ? [code, row.label || null, row.notes || '', row.status || 'unused', row.score ?? null, row.pct ?? null, row.pass ?? null]
          : [code, row.label || null, row.status || 'unused', row.score ?? null, row.pct ?? null, row.pass ?? null];
        await execQuery(conn, sql, params);
      }
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'codebook_save_failed', { message: err.message });
    res.status(500).json({ error: 'codebook_save_failed', message: err.message });
  }
});

app.put('/api/session/:code', async (req, res) => {
  const code = (req.params.code || '').toUpperCase();
  if (!code) return res.status(400).json({ error: 'invalid_code' });
  const session = req.body && req.body.session ? req.body.session : null;
  if (!session || typeof session !== 'object') return res.status(400).json({ error: 'invalid_payload' });

  try {
    await withDb(async (conn) => {
      const sql = `
        MERGE INTO EXAM_SESSIONS T
        USING (SELECT ? AS ACCESS_CODE, ? AS SESSION_JSON, ? AS ELAPSED_MS, ? AS TAB_SWITCHES FROM DUMMY) S
        ON (T.ACCESS_CODE = S.ACCESS_CODE)
        WHEN MATCHED THEN UPDATE SET
          T.SESSION_JSON = S.SESSION_JSON,
          T.ELAPSED_MS = S.ELAPSED_MS,
          T.TAB_SWITCHES = S.TAB_SWITCHES,
          T.UPDATED_AT = CURRENT_UTCTIMESTAMP
        WHEN NOT MATCHED THEN INSERT
          (ACCESS_CODE, SESSION_JSON, ELAPSED_MS, TAB_SWITCHES, UPDATED_AT)
          VALUES (S.ACCESS_CODE, S.SESSION_JSON, S.ELAPSED_MS, S.TAB_SWITCHES, CURRENT_UTCTIMESTAMP)
      `;
      await execQuery(conn, sql, [
        code,
        JSON.stringify(session),
        session.elapsed || 0,
        session.tabSwitches || 0
      ]);
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'session_save_failed', { code, message: err.message });
    res.status(500).json({ error: 'session_save_failed', message: err.message });
  }
});

app.delete('/api/session/:code', async (req, res) => {
  const code = (req.params.code || '').toUpperCase();
  if (!code) return res.status(400).json({ error: 'invalid_code' });
  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'DELETE FROM EXAM_SESSIONS WHERE ACCESS_CODE = ?', [code]);
    });
    appLog('info', 'session_deleted', { code });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'session_delete_failed', { code, message: err.message });
    res.status(500).json({ error: 'session_delete_failed', message: err.message });
  }
});

app.put('/api/result/:code', async (req, res) => {
  const code = (req.params.code || '').toUpperCase();
  const result = req.body && req.body.result ? req.body.result : null;
  if (!code) return res.status(400).json({ error: 'invalid_code' });
  if (!result || typeof result !== 'object') return res.status(400).json({ error: 'invalid_payload' });

  try {
    await withDb(async (conn) => {
      const sql = `
        MERGE INTO EXAM_RESULTS T
        USING (
          SELECT
            ? AS ACCESS_CODE,
            ? AS SCORE,
            ? AS TOTAL,
            ? AS PCT,
            ? AS PASS,
            ? AS AUTO_SUBMIT,
            ? AS DURATION_SECS,
            ? AS TAB_SWITCHES,
            ? AS INCIDENT_COUNT,
            ? AS RESULT_JSON
          FROM DUMMY
        ) S
        ON (T.ACCESS_CODE = S.ACCESS_CODE)
        WHEN MATCHED THEN UPDATE SET
          T.SCORE = S.SCORE,
          T.TOTAL = S.TOTAL,
          T.PCT = S.PCT,
          T.PASS = S.PASS,
          T.AUTO_SUBMIT = S.AUTO_SUBMIT,
          T.DURATION_SECS = S.DURATION_SECS,
          T.TAB_SWITCHES = S.TAB_SWITCHES,
          T.INCIDENT_COUNT = S.INCIDENT_COUNT,
          T.RESULT_JSON = S.RESULT_JSON,
          T.SUBMITTED_AT = CURRENT_UTCTIMESTAMP
        WHEN NOT MATCHED THEN INSERT
          (ACCESS_CODE, SCORE, TOTAL, PCT, PASS, AUTO_SUBMIT, DURATION_SECS, TAB_SWITCHES, INCIDENT_COUNT, RESULT_JSON, SUBMITTED_AT)
          VALUES (S.ACCESS_CODE, S.SCORE, S.TOTAL, S.PCT, S.PASS, S.AUTO_SUBMIT, S.DURATION_SECS, S.TAB_SWITCHES, S.INCIDENT_COUNT, S.RESULT_JSON, CURRENT_UTCTIMESTAMP)
      `;
      await execQuery(conn, sql, [
        code,
        result.score ?? 0,
        result.total ?? 30,
        result.pct ?? 0,
        result.pass ?? false,
        result.autoSubmit ?? false,
        result.durationSecs ?? 0,
        result.tabSwitches ?? 0,
        result.incidentCount ?? 0,
        JSON.stringify(result)
      ]);
    });
    appLog('info', 'result_saved', {
      code,
      score: result.score ?? 0,
      pass: Boolean(result.pass),
      incidentCount: result.incidentCount ?? 0,
      autoSubmit: Boolean(result.autoSubmit)
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'result_save_failed', { code, message: err.message });
    res.status(500).json({ error: 'result_save_failed', message: err.message });
  }
});

app.delete('/api/result/:code', async (req, res) => {
  const code = (req.params.code || '').toUpperCase();
  if (!code) return res.status(400).json({ error: 'invalid_code' });
  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'DELETE FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
    });
    appLog('info', 'result_deleted', { code });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'result_delete_failed', { code, message: err.message });
    res.status(500).json({ error: 'result_delete_failed', message: err.message });
  }
});

app.get('/api/admin/codes', async (_req, res) => {
  try {
    const rows = await withDb(async (conn) => {
      return execQuery(
        conn,
        `SELECT c.ACCESS_CODE, c.LABEL, c.STATUS, c.SCORE, c.PCT, c.PASS, r.RESULT_JSON
         FROM ACCESS_CODES c
         LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
         ORDER BY c.ACCESS_CODE ASC`
      );
    });
    res.json({ rows });
  } catch (err) {
    appLog('error', 'admin_codes_failed', { message: err.message });
    res.status(500).json({ error: 'admin_codes_failed', message: err.message });
  }
});

app.get('/api/admin/export.csv', async (_req, res) => {
  try {
    const rows = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      return execQuery(
        conn,
        `SELECT
           c.ACCESS_CODE,
           c.LABEL,
           ${hasNotes ? 'c.NOTES' : `'' AS NOTES`},
           c.STATUS,
           r.SCORE,
           r.PCT,
           r.PASS,
           r.TAB_SWITCHES,
           r.INCIDENT_COUNT,
           r.SUBMITTED_AT
         FROM ACCESS_CODES c
         LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
         ORDER BY c.ACCESS_CODE ASC`
      );
    });

    const lines = [
      'Code,Seat,Notes,Status,Score,Pct,Result,TabSwitches,Incidents,SubmittedAt'
    ];
    for (const r of rows) {
      const resultLabel = r.PASS === null || r.PASS === undefined ? '' : (r.PASS ? 'PASS' : 'FAIL');
      lines.push([
        toCsvCell(r.ACCESS_CODE),
        toCsvCell(r.LABEL || ''),
        toCsvCell(r.NOTES || ''),
        toCsvCell(r.STATUS || ''),
        toCsvCell(r.SCORE ?? ''),
        toCsvCell(r.PCT === null || r.PCT === undefined ? '' : `${r.PCT}%`),
        toCsvCell(resultLabel),
        toCsvCell(r.TAB_SWITCHES ?? ''),
        toCsvCell(r.INCIDENT_COUNT ?? ''),
        toCsvCell(r.SUBMITTED_AT ? new Date(r.SUBMITTED_AT).toISOString() : '')
      ].join(','));
    }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="ITIL4_Exam_Results.csv"');
    appLog('info', 'admin_export_csv', { rows: rows.length });
    res.send(lines.join('\n'));
  } catch (err) {
    appLog('error', 'admin_export_csv_failed', { message: err.message });
    res.status(500).json({ error: 'admin_export_csv_failed', message: err.message });
  }
});

app.get('/', (_req, res) => {
  res.sendFile(INDEX_PATH);
});

app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api/')) return next();
  if (fs.existsSync(INDEX_PATH)) return res.sendFile(INDEX_PATH);
  return res.status(404).send('Not found');
});

app.use((err, _req, res, _next) => {
  appLog('error', 'server_error', { message: err.message });
  res.status(500).json({ error: 'server_error', message: err.message });
});

app.listen(PORT, () => {
  console.log(`ITIL EvalApp server listening on port ${PORT}`);
});
