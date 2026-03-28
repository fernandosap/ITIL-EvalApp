/* eslint-disable no-console */
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const hana = require('@sap/hana-client');
const { version: APP_VERSION } = require('./package.json');

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
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-20250514';
const ANTHROPIC_VERSION = process.env.ANTHROPIC_VERSION || '2023-06-01';
const ADMIN_HASH = (process.env.ADMIN_HASH || '').trim().toLowerCase();
const EXAM_NAME = process.env.EXAM_NAME || 'ITIL 4 Foundation';
const EXAM_DURATION_SECS = Number(process.env.EXAM_DURATION_SECS || 45 * 60);
const EXAM_PASS_PCT = Number(process.env.EXAM_PASS_PCT || 80);
const EXAM_ACTIVE = String(process.env.EXAM_ACTIVE || 'true').toLowerCase() !== 'false';
const PROCTOR_ENABLED = String(process.env.PROCTOR_ENABLED || 'true').toLowerCase() !== 'false';
const APP_REVISION = process.env.APP_REVISION || 'dev';
const APP_DEPLOYED_AT = process.env.APP_DEPLOYED_AT || new Date().toISOString();
const STALE_SESSION_MINUTES = Math.max(5, Number(process.env.STALE_SESSION_MINUTES || 30));

const HAS_DB_CONFIG = Boolean(HANA_HOST && HANA_USER && HANA_PASSWORD && HANA_SCHEMA);
const INDEX_PATH = path.join(__dirname, 'index.html');
const CLIENT_APP_PATH = path.join(__dirname, 'client-app.js');

const EXAM_TTL_MS = 90 * 60 * 1000;
const ADMIN_TTL_MS = 8 * 60 * 60 * 1000;
const _examSessions = new Map();
const _validateAttempts = new Map();
const VALIDATE_MAX = 10;
const VALIDATE_WINDOW = 10 * 60 * 1000;
let _questionBankCache = null;

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: false }));

function dbConnect() {
  if (!HAS_DB_CONFIG) throw new Error('HANA env vars are missing.');
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

async function withDb(fn) {
  const conn = dbConnect();
  try {
    await execQuery(conn, `SET SCHEMA "${HANA_SCHEMA}"`);
    return await fn(conn);
  } finally {
    await closeConn(conn);
  }
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
  console.log(JSON.stringify({ ts: new Date().toISOString(), level, event, ...meta }));
}

function toCsvCell(v) {
  if (v === null || v === undefined) return '';
  return `"${String(v).replace(/"/g, '""')}"`;
}

function parseAnthropicText(content) {
  if (!Array.isArray(content)) return '';
  return content
    .filter((c) => c && c.type === 'text' && typeof c.text === 'string')
    .map((c) => c.text)
    .join('\n')
    .trim();
}

let _hasNotesColumn = null;
let _hasAuditLogTable = null;
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
  _hasNotesColumn = Number(rows?.[0]?.CNT || 0) > 0;
  return _hasNotesColumn;
}

async function hasAuditLogTable(conn) {
  if (_hasAuditLogTable !== null) return _hasAuditLogTable;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLES
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ADMIN_AUDIT_LOG'`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasAuditLogTable = Number(rows?.[0]?.CNT || 0) > 0;
  return _hasAuditLogTable;
}

function sha256(str) {
  return crypto.createHash('sha256').update(String(str)).digest('hex');
}

function getClientIp(req) {
  const xfwd = req.headers['x-forwarded-for'];
  if (typeof xfwd === 'string' && xfwd.trim()) return xfwd.split(',')[0].trim();
  return req.socket?.remoteAddress || 'unknown';
}

function makePRNG(seed) {
  let s = 0;
  for (let i = 0; i < seed.length; i++) s = (Math.imul(s, 31) + seed.charCodeAt(i)) >>> 0;
  return function prng() {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 4294967296;
  };
}

function seededShuffle(arr, rng) {
  const out = [...arr];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

function buildOrdering(questions, code) {
  const rng = makePRNG(code);
  const qOrder = seededShuffle(questions.map((_, idx) => idx), rng);
  const optOrders = qOrder.map((qIdx) => seededShuffle(questions[qIdx].opts.map((_, idx) => idx), rng));
  return { qOrder, optOrders };
}

function sanitizeProgress(progress) {
  if (!progress || typeof progress !== 'object') return null;
  return {
    answers: Array.isArray(progress.answers) ? progress.answers : [],
    visited: Array.isArray(progress.visited) ? progress.visited : [],
    currentQ: Number.isInteger(progress.currentQ) ? progress.currentQ : (Number(progress.currentQ) || 0),
    incidents: Array.isArray(progress.incidents) ? progress.incidents : [],
    tabSwitches: Number(progress.tabSwitches) || 0,
    elapsedMs: Number(progress.elapsedMs) || 0
  };
}

function createAdminToken() {
  if (!ADMIN_HASH) throw new Error('ADMIN_HASH is not configured.');
  const expiry = Date.now() + ADMIN_TTL_MS;
  const nonce = crypto.randomBytes(16).toString('hex');
  const payload = `${expiry}:${nonce}`;
  const sig = crypto.createHmac('sha256', ADMIN_HASH).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}

function isValidAdminToken(token) {
  try {
    if (!token) return false;
    const decoded = Buffer.from(token, 'base64url').toString();
    const [expiry, nonce, sig] = decoded.split(':');
    if (!expiry || !nonce || !sig) return false;
    if (Date.now() > Number(expiry)) return false;
    const expected = crypto.createHmac('sha256', ADMIN_HASH).update(`${expiry}:${nonce}`).digest('hex');
    if (sig.length !== expected.length) return false;
    return crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected));
  } catch (_e) {
    return false;
  }
}

function requireAdmin(req, res, next) {
  const token = String(req.headers['x-admin-token'] || '').trim();
  if (!isValidAdminToken(token)) return res.status(401).json({ error: 'unauthorized' });
  next();
}

async function writeAdminAudit(conn, entry) {
  if (!(await hasAuditLogTable(conn))) return false;
  await execQuery(
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
  return true;
}

async function tryWriteAdminAudit(entry) {
  if (!HAS_DB_CONFIG) return false;
  try {
    return await withDb(async (conn) => writeAdminAudit(conn, entry));
  } catch (err) {
    appLog('warn', 'admin_audit_write_failed', { message: err.message, action: entry.action });
    return false;
  }
}

function createExamSessionFromBank(code, questionBank) {
  const { qOrder, optOrders } = buildOrdering(questionBank.questions, code);
  const token = crypto.randomBytes(32).toString('hex');
  _examSessions.set(token, {
    code,
    qOrder,
    optOrders,
    createdAt: Date.now(),
    expires: Date.now() + EXAM_TTL_MS
  });
  return { token, qOrder, optOrders };
}

function getExamSession(token) {
  const session = token ? _examSessions.get(token) : null;
  if (!session) return null;
  if (session.expires < Date.now()) {
    _examSessions.delete(token);
    return null;
  }
  session.expires = Date.now() + EXAM_TTL_MS;
  return session;
}

function requireExamSession(req, res, next) {
  const token = String(req.headers['x-exam-token'] || '').trim();
  const session = getExamSession(token);
  if (!session) return res.status(401).json({ error: 'invalid_exam_session' });
  req.examSession = session;
  next();
}

function checkRateLimit(ip) {
  const now = Date.now();
  let entry = _validateAttempts.get(ip);
  if (!entry || entry.resetAt < now) {
    entry = { count: 0, resetAt: now + VALIDATE_WINDOW };
    _validateAttempts.set(ip, entry);
  }
  entry.count += 1;
  if (entry.count > VALIDATE_MAX) return false;
  if (_validateAttempts.size > 1000) {
    for (const [key, value] of _validateAttempts.entries()) {
      if (value.resetAt < now) _validateAttempts.delete(key);
    }
  }
  return true;
}

async function loadQuestionBank(conn) {
  if (_questionBankCache) return _questionBankCache;

  const rows = await execQuery(
    conn,
    `SELECT QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI
       FROM EXAM_QUESTIONS
      ORDER BY QUESTION_INDEX ASC`
  );

  const questions = rows.map((row) => {
    const opts = parseJsonOrNull(row.OPTS_JSON);
    const answer = parseJsonOrNull(row.ANSWER_JSON);
    if (!Array.isArray(opts) || !Array.isArray(answer)) {
      throw new Error(`Invalid question payload for QUESTION_INDEX=${row.QUESTION_INDEX}`);
    }
    return {
      stem: String(row.STEM || ''),
      note: row.NOTE || null,
      opts: opts.map((opt) => String(opt)),
      multi: Boolean(row.MULTI)
    };
  });

  const answerKey = rows.map((row) => {
    const answer = parseJsonOrNull(row.ANSWER_JSON);
    if (!Array.isArray(answer)) throw new Error(`Invalid answer payload for QUESTION_INDEX=${row.QUESTION_INDEX}`);
    return answer.map((value) => Number(value));
  });

  if (!questions.length) {
    throw new Error('Question bank is empty. Load EXAM_QUESTIONS before starting the app.');
  }

  _questionBankCache = {
    questions,
    answerKey,
    total: questions.length
  };
  return _questionBankCache;
}

async function getQuestionBank() {
  return withDb(async (conn) => loadQuestionBank(conn));
}

function getExamConfig(questionBank) {
  const total = Number(questionBank?.total || 0);
  return {
    examName: EXAM_NAME,
    examActive: EXAM_ACTIVE,
    durationSecs: EXAM_DURATION_SECS,
    passPct: EXAM_PASS_PCT,
    passScore: Math.ceil(total * EXAM_PASS_PCT / 100),
    total,
    proctorEnabled: PROCTOR_ENABLED
  };
}

async function getCodeRow(conn, code) {
  const hasNotes = await hasNotesColumn(conn);
  const rows = await execQuery(
    conn,
    `SELECT ACCESS_CODE, LABEL, ${hasNotes ? 'NOTES,' : ''} STATUS, SCORE, PCT, PASS, CREATED_AT
       FROM ACCESS_CODES
      WHERE ACCESS_CODE = ?`,
    [code]
  );
  if (!rows.length) return null;
  const r = rows[0];
  return {
    accessCode: r.ACCESS_CODE,
    label: r.LABEL || null,
    notes: hasNotes ? (r.NOTES || '') : '',
    status: r.STATUS || 'unused',
    score: r.SCORE,
    pct: r.PCT,
    pass: r.PASS,
    createdAt: r.CREATED_AT || null
  };
}

async function getSavedSession(conn, code) {
  const rows = await execQuery(
    conn,
    `SELECT SESSION_JSON, ELAPSED_MS, TAB_SWITCHES
       FROM EXAM_SESSIONS
      WHERE ACCESS_CODE = ?`,
    [code]
  );
  if (!rows.length) return null;
  const parsed = parseJsonOrNull(rows[0].SESSION_JSON);
  const progress = sanitizeProgress(parsed);
  if (!progress) return null;
  progress.elapsedMs = Number(rows[0].ELAPSED_MS || progress.elapsedMs || 0);
  progress.tabSwitches = Number(rows[0].TAB_SWITCHES || progress.tabSwitches || 0);
  return progress;
}

async function saveSession(conn, code, progress) {
  const payload = {
    answers: progress.answers || [],
    visited: progress.visited || [],
    currentQ: progress.currentQ || 0,
    incidents: progress.incidents || [],
    tabSwitches: progress.tabSwitches || 0,
    elapsedMs: progress.elapsedMs || 0
  };
  await execQuery(
    conn,
    `MERGE INTO EXAM_SESSIONS T
      USING (SELECT ? AS ACCESS_CODE, ? AS SESSION_JSON, ? AS ELAPSED_MS, ? AS TAB_SWITCHES FROM DUMMY) S
         ON (T.ACCESS_CODE = S.ACCESS_CODE)
      WHEN MATCHED THEN UPDATE SET
        T.SESSION_JSON = S.SESSION_JSON,
        T.ELAPSED_MS = S.ELAPSED_MS,
        T.TAB_SWITCHES = S.TAB_SWITCHES,
        T.UPDATED_AT = CURRENT_UTCTIMESTAMP
      WHEN NOT MATCHED THEN INSERT
        (ACCESS_CODE, SESSION_JSON, ELAPSED_MS, TAB_SWITCHES, UPDATED_AT)
        VALUES (S.ACCESS_CODE, S.SESSION_JSON, S.ELAPSED_MS, S.TAB_SWITCHES, CURRENT_UTCTIMESTAMP)`,
    [code, JSON.stringify(payload), payload.elapsedMs, payload.tabSwitches]
  );
}

async function deleteSession(conn, code) {
  await execQuery(conn, 'DELETE FROM EXAM_SESSIONS WHERE ACCESS_CODE = ?', [code]);
}

async function getResultRecord(conn, code) {
  const rows = await execQuery(
    conn,
    `SELECT RESULT_JSON
       FROM EXAM_RESULTS
      WHERE ACCESS_CODE = ?`,
    [code]
  );
  if (!rows.length) return null;
  return parseJsonOrNull(rows[0].RESULT_JSON);
}

async function saveResult(conn, code, result) {
  await execQuery(
    conn,
    `MERGE INTO EXAM_RESULTS T
      USING (
        SELECT ? AS ACCESS_CODE, ? AS SCORE, ? AS TOTAL, ? AS PCT, ? AS PASS, ? AS AUTO_SUBMIT,
               ? AS DURATION_SECS, ? AS TAB_SWITCHES, ? AS INCIDENT_COUNT, ? AS RESULT_JSON
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
        VALUES (S.ACCESS_CODE, S.SCORE, S.TOTAL, S.PCT, S.PASS, S.AUTO_SUBMIT, S.DURATION_SECS, S.TAB_SWITCHES, S.INCIDENT_COUNT, S.RESULT_JSON, CURRENT_UTCTIMESTAMP)`,
    [
      code,
      result.score ?? 0,
      result.total ?? 0,
      result.pct ?? 0,
      result.pass ? 1 : 0,
      result.autoSubmit ? 1 : 0,
      result.durationSecs ?? 0,
      result.tabSwitches ?? 0,
      result.incidentCount ?? 0,
      JSON.stringify(result)
    ]
  );
}

async function updateCodeStatus(conn, code, status, result = null) {
  await execQuery(
    conn,
    `UPDATE ACCESS_CODES
        SET STATUS = ?,
            SCORE = ?,
            PCT = ?,
            PASS = ?
      WHERE ACCESS_CODE = ?`,
    [status, result?.score ?? null, result?.pct ?? null, result?.pass == null ? null : (result.pass ? 1 : 0), code]
  );
}

function gradeExamFromSession(session, answers, questionBank) {
  const answerKey = questionBank.answerKey;
  let score = 0;
  const questionResults = [];

  session.qOrder.forEach((questionIdx, displayIdx) => {
    const displaySelection = Array.isArray(answers[displayIdx]) ? answers[displayIdx].map(Number) : [];
    const optionOrder = session.optOrders[displayIdx];
    const originalSelection = displaySelection
      .filter((idx) => Number.isInteger(idx) && idx >= 0 && idx < optionOrder.length)
      .map((idx) => optionOrder[idx])
      .sort((a, b) => a - b);
    const expected = (answerKey[questionIdx] || []).slice().sort((a, b) => a - b);
    const correct = originalSelection.join(',') === expected.join(',');
    if (correct) score += 1;
    questionResults.push({ displayIdx, questionIdx, correct, given: originalSelection, expected });
  });

  const total = questionBank.total;
  const pct = Math.round((score / total) * 100);
  const pass = pct >= EXAM_PASS_PCT;
  return { score, total, pct, pass, questionResults };
}

app.post('/api/proctor/check', async (req, res) => {
  const imageB64 = req.body && typeof req.body.imageB64 === 'string' ? req.body.imageB64 : '';
  if (!imageB64) return res.status(400).json({ error: 'missing_image' });
  if (!ANTHROPIC_API_KEY) return res.json({ enabled: false, flag: false, reason: null });

  try {
    const prompt =
      'Exam proctor AI. Respond ONLY with JSON, no other text: {"flag":false,"reason":null} or {"flag":true,"reason":"brief reason"}. ' +
      'Flag ONLY if: no face visible, second person visible, phone/notes visible, candidate clearly looking away for extended period. ' +
      'Do NOT flag for minor head movements, blinking, or adjusting posture.';

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': ANTHROPIC_VERSION
      },
      body: JSON.stringify({
        model: ANTHROPIC_MODEL,
        max_tokens: 100,
        messages: [
          {
            role: 'user',
            content: [
              { type: 'image', source: { type: 'base64', media_type: 'image/jpeg', data: imageB64 } },
              { type: 'text', text: prompt }
            ]
          }
        ]
      }),
      signal: AbortSignal.timeout(12000)
    });

    if (!response.ok) {
      const detail = await response.text();
      appLog('warn', 'proctor_provider_non_ok', { status: response.status, detail: detail.slice(0, 300) });
      return res.status(502).json({ error: 'provider_non_ok' });
    }

    const data = await response.json();
    const text = parseAnthropicText(data.content);
    let parsed = { flag: false, reason: null };
    try {
      parsed = JSON.parse(text);
    } catch (_e) {
      appLog('warn', 'proctor_provider_parse_failed', { text: text.slice(0, 300) });
      return res.status(502).json({ error: 'provider_parse_failed' });
    }

    return res.json({
      enabled: true,
      flag: Boolean(parsed && parsed.flag),
      reason: parsed && parsed.reason ? String(parsed.reason) : null
    });
  } catch (err) {
    appLog('error', 'proctor_check_failed', { message: err.message });
    return res.status(500).json({ error: 'proctor_check_failed' });
  }
});

app.get('/api/health', async (_req, res) => {
  if (!HAS_DB_CONFIG) return res.status(500).json({ ok: false, message: 'Missing HANA env vars.' });
  try {
    const questionBank = await withDb(async (conn) => {
      await execQuery(conn, 'SELECT 1 AS OK FROM DUMMY');
      return loadQuestionBank(conn);
    });
    res.json({ ok: true, db: 'connected', schema: HANA_SCHEMA, totalQuestions: questionBank.total });
  } catch (err) {
    appLog('error', 'health_failed', { message: err.message });
    res.status(500).json({ ok: false, message: err.message });
  }
});

app.get('/api/status', async (_req, res) => {
  try {
    const questionBank = await getQuestionBank();
    res.json(getExamConfig(questionBank));
  } catch (err) {
    appLog('error', 'status_failed', { message: err.message });
    res.status(500).json({ error: 'status_failed', message: err.message });
  }
});

app.get('/api/bootstrap', (_req, res) => {
  res.status(410).json({ error: 'bootstrap_removed', message: 'Client bootstrap is disabled for security reasons.' });
});

app.post('/api/validate', async (req, res) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
  if (!checkRateLimit(String(ip))) return res.status(429).json({ valid: false, reason: 'too_many_attempts' });

  const code = String(req.body?.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.json({ valid: false, reason: 'invalid_format' });

  try {
    const questionBank = await getQuestionBank();
    const cfg = getExamConfig(questionBank);
    if (!cfg.examActive) return res.json({ valid: false, reason: 'exam_not_active' });

    const result = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) return { valid: false, reason: 'not_found' };

      const savedResult = await getResultRecord(conn, code);
      if (savedResult || codeRow.status === 'completed') {
        return { valid: true, status: 'completed', result: savedResult || null };
      }

      const progress = await getSavedSession(conn, code);
      if (progress || codeRow.status === 'active') {
        return { valid: true, status: 'active', progress: progress || null };
      }

      return { valid: true, status: 'unused' };
    });

    res.json({ ...cfg, ...result });
  } catch (err) {
    appLog('error', 'validate_failed', { code, message: err.message });
    res.status(500).json({ error: 'validate_failed', message: err.message });
  }
});

app.post('/api/session/start', async (req, res) => {
  const code = String(req.body?.code || '').trim().toUpperCase();
  const fresh = Boolean(req.body?.fresh);
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    const questionBank = await getQuestionBank();
    const payload = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) return { status: 404, body: { error: 'code_not_found' } };

      const savedResult = await getResultRecord(conn, code);
      if (savedResult || codeRow.status === 'completed') {
        return { status: 409, body: { error: 'exam_completed' } };
      }

      if (fresh) await deleteSession(conn, code);
      const progress = fresh ? null : await getSavedSession(conn, code);
      const { token } = createExamSessionFromBank(code, questionBank);
      await updateCodeStatus(conn, code, 'active');

      return {
        status: 200,
        body: {
          ok: true,
          examToken: token,
          progress,
          ...getExamConfig(questionBank)
        }
      };
    });

    res.status(payload.status).json(payload.body);
  } catch (err) {
    appLog('error', 'session_start_failed', { code, message: err.message });
    res.status(500).json({ error: 'session_start_failed', message: err.message });
  }
});

app.get('/api/question/:displayIdx', requireExamSession, async (req, res) => {
  const displayIdx = Number(req.params.displayIdx);
  const session = req.examSession;
  if (!Number.isInteger(displayIdx) || displayIdx < 0 || displayIdx >= session.qOrder.length) {
    return res.status(400).json({ error: 'invalid_question_index' });
  }

  try {
    const questionBank = await getQuestionBank();
    const questionIdx = session.qOrder[displayIdx];
    const question = questionBank.questions[questionIdx];
    const optionOrder = session.optOrders[displayIdx];
    if (!question) return res.status(404).json({ error: 'question_not_found' });
    res.json({
      displayIdx,
      total: questionBank.total,
      stem: question.stem,
      note: question.note || null,
      multi: Boolean(question.multi),
      opts: optionOrder.map((idx) => question.opts[idx])
    });
  } catch (err) {
    appLog('error', 'question_fetch_failed', { message: err.message, displayIdx });
    res.status(500).json({ error: 'question_fetch_failed', message: err.message });
  }
});

app.post('/api/progress', requireExamSession, async (req, res) => {
  const session = req.examSession;
  const code = String(req.body?.code || '').trim().toUpperCase();
  if (code !== session.code) return res.status(403).json({ error: 'code_mismatch' });

  const progress = sanitizeProgress({
    answers: req.body?.answers,
    visited: req.body?.visited,
    currentQ: req.body?.currentQ,
    incidents: req.body?.incidents,
    tabSwitches: req.body?.tabSwitches,
    elapsedMs: req.body?.elapsedMs
  });

  try {
    await withDb(async (conn) => {
      await saveSession(conn, code, progress);
      await updateCodeStatus(conn, code, 'active');
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'progress_save_failed', { code, message: err.message });
    res.status(500).json({ error: 'progress_save_failed', message: err.message });
  }
});

app.post('/api/submit', requireExamSession, async (req, res) => {
  const session = req.examSession;
  const code = String(req.body?.code || '').trim().toUpperCase();
  if (code !== session.code) return res.status(403).json({ error: 'code_mismatch' });

  const answers = Array.isArray(req.body?.answers) ? req.body.answers : [];
  const durationSecs = Math.max(10, Math.min(Number(req.body?.durationSecs) || 0, EXAM_DURATION_SECS + 300));
  const tabSwitches = Number(req.body?.tabSwitches) || 0;
  const incidents = Array.isArray(req.body?.incidents) ? req.body.incidents : [];
  const autoSubmit = Boolean(req.body?.autoSubmit);

  try {
    const questionBank = await getQuestionBank();
    const result = gradeExamFromSession(session, answers, questionBank);
    const record = {
      code,
      score: result.score,
      total: result.total,
      pct: result.pct,
      pass: result.pass,
      autoSubmit,
      durationSecs,
      tabSwitches,
      incidents,
      incidentCount: incidents.length,
      submittedAt: new Date().toISOString()
    };

    await withDb(async (conn) => {
      await saveResult(conn, code, record);
      await updateCodeStatus(conn, code, 'completed', record);
      await deleteSession(conn, code);
    });

    for (const [token, value] of _examSessions.entries()) {
      if (value.code === code) _examSessions.delete(token);
    }

    appLog('info', 'exam_submitted', { code, score: record.score, pct: record.pct, pass: record.pass });
    res.json({ ok: true, result: record });
  } catch (err) {
    appLog('error', 'submit_failed', { code, message: err.message });
    res.status(500).json({ error: 'submit_failed', message: err.message });
  }
});

app.get('/api/result/:code', async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  try {
    const result = await withDb(async (conn) => getResultRecord(conn, code));
    if (!result) return res.status(404).json({ error: 'not_found' });
    res.json({ result });
  } catch (err) {
    appLog('error', 'result_fetch_failed', { code, message: err.message });
    res.status(500).json({ error: 'result_fetch_failed', message: err.message });
  }
});

app.post('/api/admin/login', (req, res) => {
  if (!ADMIN_HASH) return res.status(503).json({ ok: false, error: 'admin_not_configured' });
  const ip = getClientIp(req);
  if (!checkRateLimit(String(ip))) return res.status(429).json({ ok: false, error: 'too_many_attempts' });

  const hash = String(req.body?.hash || '').trim().toLowerCase();
  if (!hash || hash !== ADMIN_HASH) {
    void tryWriteAdminAudit({
      action: 'admin_login_failed',
      actor: 'admin',
      clientIp: ip,
      details: { reason: 'invalid_credentials' }
    });
    return setTimeout(() => res.status(401).json({ ok: false, error: 'invalid_credentials' }), 350);
  }

  void tryWriteAdminAudit({
    action: 'admin_login_success',
    actor: 'admin',
    clientIp: ip,
    details: { ok: true }
  });
  return res.json({ ok: true, token: createAdminToken() });
});

app.get('/api/admin/codes', requireAdmin, async (_req, res) => {
  try {
    const rows = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      return execQuery(
        conn,
        `SELECT c.ACCESS_CODE, c.LABEL, ${hasNotes ? 'c.NOTES,' : `'' AS NOTES,`} c.STATUS, c.SCORE, c.PCT, c.PASS,
                r.DURATION_SECS, r.TAB_SWITCHES, r.INCIDENT_COUNT, r.SUBMITTED_AT, r.RESULT_JSON
           FROM ACCESS_CODES c
           LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
          ORDER BY c.ACCESS_CODE ASC`
      );
    });
    const codes = rows.map((r) => {
      const parsedResult = parseJsonOrNull(r.RESULT_JSON);
      return {
        code: r.ACCESS_CODE,
        label: r.LABEL || '',
        notes: r.NOTES || '',
        status: r.STATUS || 'unused',
        score: r.SCORE,
        pct: r.PCT,
        pass: r.PASS === null || r.PASS === undefined ? null : Boolean(r.PASS),
        durationSecs: r.DURATION_SECS,
        tabSwitches: r.TAB_SWITCHES || 0,
        incidentCount: r.INCIDENT_COUNT || 0,
        submittedAt: r.SUBMITTED_AT ? new Date(r.SUBMITTED_AT).toISOString() : null,
        incidents: parsedResult?.incidents || []
      };
    });
    res.json({ codes, examName: EXAM_NAME, examActive: EXAM_ACTIVE, proctorEnabled: PROCTOR_ENABLED });
  } catch (err) {
    appLog('error', 'admin_codes_failed', { message: err.message });
    res.status(500).json({ error: 'admin_codes_failed', message: err.message });
  }
});

app.get('/api/admin/system-status', requireAdmin, async (_req, res) => {
  try {
    const status = await withDb(async (conn) => {
      const questionBank = await loadQuestionBank(conn);
      const hasNotes = await hasNotesColumn(conn);
      const auditEnabled = await hasAuditLogTable(conn);
      const accessCodeRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM ACCESS_CODES');
      const resultRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM EXAM_RESULTS');
      const sessionRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM EXAM_SESSIONS');
      const staleSessionRows = await execQuery(
        conn,
        `SELECT ACCESS_CODE, UPDATED_AT
           FROM EXAM_SESSIONS
          WHERE UPDATED_AT < ADD_SECONDS(CURRENT_UTCTIMESTAMP, ?)
          ORDER BY UPDATED_AT ASC
          LIMIT 5`,
        [-1 * STALE_SESSION_MINUTES * 60]
      );
      const auditRows = auditEnabled ? await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM ADMIN_AUDIT_LOG') : [{ CNT: 0 }];
      return {
        ok: questionBank.total > 0,
        schema: HANA_SCHEMA,
        questionCount: questionBank.total,
        accessCodeCount: Number(accessCodeRows?.[0]?.CNT || 0),
        resultCount: Number(resultRows?.[0]?.CNT || 0),
        activeSessionCount: Number(sessionRows?.[0]?.CNT || 0),
        staleSessionCount: staleSessionRows.length,
        staleSessionMinutes: STALE_SESSION_MINUTES,
        staleSessions: staleSessionRows.map((row) => ({
          code: row.ACCESS_CODE,
          updatedAt: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
        })),
        auditCount: Number(auditRows?.[0]?.CNT || 0),
        appVersion: APP_VERSION,
        appRevision: APP_REVISION,
        deployedAt: APP_DEPLOYED_AT,
        notesEnabled: Boolean(hasNotes),
        auditEnabled,
        adminConfigured: Boolean(ADMIN_HASH),
        warnings: [
          ...(questionBank.total > 0 ? [] : ['Question bank is empty.']),
          ...(hasNotes ? [] : ['ACCESS_CODES.NOTES column is missing.']),
          ...(staleSessionRows.length ? [`${staleSessionRows.length} active session(s) look stale (${STALE_SESSION_MINUTES}+ min without a save).`] : []),
          ...(auditEnabled ? [] : ['ADMIN_AUDIT_LOG table is missing.']),
          ...(ADMIN_HASH ? [] : ['ADMIN_HASH is not configured on the server.'])
        ]
      };
    });
    res.json(status);
  } catch (err) {
    appLog('error', 'admin_system_status_failed', { message: err.message });
    res.status(500).json({
      ok: false,
      schema: HANA_SCHEMA,
      questionCount: 0,
      accessCodeCount: 0,
      resultCount: 0,
      activeSessionCount: 0,
      staleSessionCount: 0,
      staleSessionMinutes: STALE_SESSION_MINUTES,
      staleSessions: [],
      auditCount: 0,
      appVersion: APP_VERSION,
      appRevision: APP_REVISION,
      deployedAt: APP_DEPLOYED_AT,
      notesEnabled: false,
      auditEnabled: false,
      adminConfigured: Boolean(ADMIN_HASH),
      warnings: ['Could not load system status from HANA.'],
      error: 'admin_system_status_failed'
    });
  }
});

app.get('/api/admin/question-probe', requireAdmin, async (_req, res) => {
  try {
    const questionBank = await getQuestionBank();
    const idx = Math.floor(Math.random() * questionBank.total);
    const question = questionBank.questions[idx];
    res.json({
      ok: true,
      questionIndex: idx,
      total: questionBank.total,
      multi: Boolean(question.multi),
      optionCount: Array.isArray(question.opts) ? question.opts.length : 0,
      notePresent: Boolean(question.note),
      stemPreview: String(question.stem || '').slice(0, 220)
    });
  } catch (err) {
    appLog('error', 'admin_question_probe_failed', { message: err.message });
    res.status(500).json({ ok: false, error: 'admin_question_probe_failed' });
  }
});

app.get('/api/admin/audit', requireAdmin, async (req, res) => {
  const limit = Math.min(Math.max(Number(req.query?.limit) || 20, 1), 100);
  try {
    const entries = await withDb(async (conn) => {
      if (!(await hasAuditLogTable(conn))) return [];
      const rows = await execQuery(
        conn,
        `SELECT AUDIT_ID, ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT
           FROM ADMIN_AUDIT_LOG
          ORDER BY CREATED_AT DESC
          LIMIT ${limit}`
      );
      return rows.map((row) => ({
        id: row.AUDIT_ID,
        action: row.ACTION,
        targetCode: row.TARGET_CODE || '',
        actor: row.ACTOR || 'admin',
        clientIp: row.CLIENT_IP || '',
        createdAt: row.CREATED_AT ? new Date(row.CREATED_AT).toISOString() : null,
        details: parseJsonOrNull(row.DETAILS_JSON) || null
      }));
    });
    res.json({ entries });
  } catch (err) {
    appLog('error', 'admin_audit_fetch_failed', { message: err.message });
    res.status(500).json({ error: 'admin_audit_fetch_failed' });
  }
});

app.post('/api/admin/note', requireAdmin, async (req, res) => {
  const code = String(req.body?.code || '').trim().toUpperCase();
  const notes = String(req.body?.notes || '');
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    await withDb(async (conn) => {
      if (await hasNotesColumn(conn)) {
        await execQuery(conn, 'UPDATE ACCESS_CODES SET NOTES = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE ACCESS_CODE = ?', [notes, code]);
      }
      await writeAdminAudit(conn, {
        action: 'admin_note_saved',
        targetCode: code,
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { noteLength: notes.length }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'admin_note_failed', { code, message: err.message });
    res.status(500).json({ error: 'admin_note_failed', message: err.message });
  }
});

app.post('/api/admin/reset', requireAdmin, async (req, res) => {
  const code = String(req.body?.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    await withDb(async (conn) => {
      await deleteSession(conn, code);
      await execQuery(conn, 'DELETE FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
      await execQuery(conn, 'UPDATE ACCESS_CODES SET STATUS = ?, SCORE = NULL, PCT = NULL, PASS = NULL, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE ACCESS_CODE = ?', ['unused', code]);
      await writeAdminAudit(conn, {
        action: 'admin_code_reset',
        targetCode: code,
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { status: 'unused' }
      });
    });
    for (const [token, value] of _examSessions.entries()) {
      if (value.code === code) _examSessions.delete(token);
    }
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'admin_reset_failed', { code, message: err.message });
    res.status(500).json({ error: 'admin_reset_failed', message: err.message });
  }
});

app.post('/api/admin/generate', requireAdmin, async (req, res) => {
  const count = Math.min(Math.max(Number(req.body?.count) || 10, 1), 200);
  const chars = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

  try {
    const added = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const existingRows = await execQuery(conn, 'SELECT ACCESS_CODE FROM ACCESS_CODES');
      const used = new Set(existingRows.map((r) => r.ACCESS_CODE));
      const seatBase = existingRows.length + 1;
      const created = [];

      while (created.length < count) {
        let code = '';
        for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
        if (!used.has(code)) {
          used.add(code);
          created.push(code);
        }
      }

      const sql = hasNotes
        ? `INSERT INTO ACCESS_CODES (ACCESS_CODE, LABEL, NOTES, STATUS, CREATED_AT, UPDATED_AT)
             VALUES (?, ?, ?, 'unused', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`
        : `INSERT INTO ACCESS_CODES (ACCESS_CODE, LABEL, STATUS, CREATED_AT, UPDATED_AT)
             VALUES (?, ?, 'unused', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`;

      for (let i = 0; i < created.length; i++) {
        const label = `Seat ${String(seatBase + i).padStart(3, '0')}`;
        const params = hasNotes ? [created[i], label, ''] : [created[i], label];
        await execQuery(conn, sql, params);
      }
      await writeAdminAudit(conn, {
        action: 'admin_codes_generated',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { count: created.length, firstCode: created[0] || null, lastCode: created[created.length - 1] || null }
      });
      return created.length;
    });

    res.json({ ok: true, added });
  } catch (err) {
    appLog('error', 'admin_generate_failed', { message: err.message });
    res.status(500).json({ error: 'admin_generate_failed', message: err.message });
  }
});

app.get('/api/admin/export.csv', requireAdmin, async (_req, res) => {
  try {
    const rows = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      return execQuery(
        conn,
        `SELECT c.ACCESS_CODE, c.LABEL, ${hasNotes ? 'c.NOTES,' : `'' AS NOTES,`} c.STATUS,
                r.SCORE, r.PCT, r.PASS, r.DURATION_SECS, r.TAB_SWITCHES, r.INCIDENT_COUNT, r.SUBMITTED_AT
           FROM ACCESS_CODES c
           LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
          ORDER BY c.ACCESS_CODE ASC`
      );
    });

    const lines = ['Code,Seat,Notes,Status,Score,Pct,Result,Duration,TabSwitches,Incidents,SubmittedAt'];
    for (const r of rows) {
      const resultLabel = r.PASS === null || r.PASS === undefined ? '' : (r.PASS ? 'PASS' : 'FAIL');
      const duration = r.DURATION_SECS == null ? '' : `${Math.floor(r.DURATION_SECS / 60)}m ${String(r.DURATION_SECS % 60).padStart(2, '0')}s`;
      lines.push([
        toCsvCell(r.ACCESS_CODE),
        toCsvCell(r.LABEL || ''),
        toCsvCell(r.NOTES || ''),
        toCsvCell(r.STATUS || ''),
        toCsvCell(r.SCORE ?? ''),
        toCsvCell(r.PCT == null ? '' : `${r.PCT}%`),
        toCsvCell(resultLabel),
        toCsvCell(duration),
        toCsvCell(r.TAB_SWITCHES ?? ''),
        toCsvCell(r.INCIDENT_COUNT ?? ''),
        toCsvCell(r.SUBMITTED_AT ? new Date(r.SUBMITTED_AT).toISOString() : '')
      ].join(','));
    }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="ITIL4_Exam_Results.csv"');
    res.send(lines.join('\n'));
  } catch (err) {
    appLog('error', 'admin_export_failed', { message: err.message });
    res.status(500).json({ error: 'admin_export_failed', message: err.message });
  }
});

app.get('/client-app.js', (_req, res) => {
  res.type('application/javascript').sendFile(CLIENT_APP_PATH);
});

app.get('/', (_req, res) => {
  res.sendFile(INDEX_PATH);
});

app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api/')) return next();
  if (req.path === '/client-app.js' && fs.existsSync(CLIENT_APP_PATH)) return res.type('application/javascript').sendFile(CLIENT_APP_PATH);
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
