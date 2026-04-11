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
const MANAGER_HASH = (process.env.MANAGER_HASH || '').trim().toLowerCase();
const EXAM_NAME = process.env.EXAM_NAME || 'ITIL 4 Foundation';
const EXAM_DURATION_SECS = Number(process.env.EXAM_DURATION_SECS || 45 * 60);
const EXAM_PASS_PCT = Number(process.env.EXAM_PASS_PCT || 80);
const EXAM_ACTIVE = String(process.env.EXAM_ACTIVE || 'true').toLowerCase() !== 'false';
const PROCTOR_ENABLED = String(process.env.PROCTOR_ENABLED || 'true').toLowerCase() !== 'false';
const APP_REVISION = process.env.APP_REVISION || 'dev';
const APP_DEPLOYED_AT = process.env.APP_DEPLOYED_AT || new Date().toISOString();
const STALE_SESSION_MINUTES = Math.max(5, Number(process.env.STALE_SESSION_MINUTES || 30));
const APP_SETTING_EXAMS_ENABLED = 'EXAMS_ENABLED';

const HAS_DB_CONFIG = Boolean(HANA_HOST && HANA_USER && HANA_PASSWORD && HANA_SCHEMA);
const INDEX_PATH = path.join(__dirname, 'index.html');
const CLIENT_APP_PATH = path.join(__dirname, 'client-app.js');

const EXAM_TTL_MS = 90 * 60 * 1000;
const ADMIN_TTL_MS = 8 * 60 * 60 * 1000;
const _examSessions = new Map();
const _validateAttempts = new Map();
const VALIDATE_MAX = 10;
const VALIDATE_WINDOW = 10 * 60 * 1000;
const _questionSetCache = new Map();

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
let _hasDeletedAtColumn = null;
let _hasQuestionSetModeColumns = null;
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

async function hasDeletedAtColumn(conn) {
  if (_hasDeletedAtColumn !== null) return _hasDeletedAtColumn;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'ACCESS_CODES'
        AND COLUMN_NAME = 'DELETED_AT'`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasDeletedAtColumn = Number(rows?.[0]?.CNT || 0) > 0;
  return _hasDeletedAtColumn;
}

async function hasQuestionSetModeColumns(conn) {
  if (_hasQuestionSetModeColumns !== null) return _hasQuestionSetModeColumns;
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM SYS.TABLE_COLUMNS
      WHERE SCHEMA_NAME = ?
        AND TABLE_NAME = 'QUESTION_SETS'
        AND COLUMN_NAME IN ('EXAM_MODE', 'SHOW_CORRECT_ANSWERS', 'COUNTS_TOWARD_RESULTS')`,
    [String(HANA_SCHEMA || '').toUpperCase()]
  );
  _hasQuestionSetModeColumns = Number(rows?.[0]?.CNT || 0) === 3;
  return _hasQuestionSetModeColumns;
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

function tokenSecretForRole(role) {
  return role === 'manager' ? MANAGER_HASH : ADMIN_HASH;
}

function createAdminToken(role = 'admin') {
  const expiry = Date.now() + ADMIN_TTL_MS;
  const nonce = crypto.randomBytes(16).toString('hex');
  const safeRole = role === 'manager' ? 'manager' : 'admin';
  const secret = tokenSecretForRole(safeRole);
  if (!secret) throw new Error(`${safeRole.toUpperCase()}_HASH is not configured.`);
  const payload = `${expiry}:${nonce}:${safeRole}`;
  const sig = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  return Buffer.from(`${payload}:${sig}`).toString('base64url');
}

function getAdminTokenRole(token) {
  try {
    if (!token) return null;
    const decoded = Buffer.from(token, 'base64url').toString();
    const parts = decoded.split(':');
    const expiry = parts[0];
    const nonce = parts[1];
    const role = parts.length === 4 ? parts[2] : 'admin';
    const sig = parts.length === 4 ? parts[3] : parts[2];
    if (!expiry || !nonce || !sig) return null;
    if (Date.now() > Number(expiry)) return null;
    const payload = parts.length === 4 ? `${expiry}:${nonce}:${role}` : `${expiry}:${nonce}`;
    const secret = tokenSecretForRole(role === 'manager' ? 'manager' : 'admin');
    if (!secret) return null;
    const expected = crypto.createHmac('sha256', secret).update(payload).digest('hex');
    if (sig.length !== expected.length) return null;
    if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) return null;
    return role === 'manager' ? 'manager' : 'admin';
  } catch (_e) {
    return null;
  }
}

function requireAdmin(req, res, next) {
  const token = String(req.headers['x-admin-token'] || '').trim();
  const role = getAdminTokenRole(token);
  if (!role) return res.status(401).json({ error: 'unauthorized' });
  req.adminRole = role;
  next();
}

function requireAdminRole(role) {
  return (req, res, next) => {
    if (role === 'admin' && req.adminRole !== 'admin') {
      return res.status(403).json({ error: 'admin_role_required' });
    }
    next();
  };
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

function createExamSessionFromSet(code, questionSet) {
  const selectedQuestions = pickQuestionsForSession(questionSet, code);
  const { qOrder, optOrders } = buildOrdering(selectedQuestions, code);
  const answerKey = selectedQuestions.map((question) => question.answer.slice());
  const token = crypto.randomBytes(32).toString('hex');
  _examSessions.set(token, {
    code,
    questionSetId: questionSet.id,
    questionSetName: questionSet.name,
    examMode: questionSet.examMode,
    showCorrectAnswers: questionSet.showCorrectAnswers === true,
    countsTowardResults: questionSet.countsTowardResults !== false,
    passPct: Number(questionSet.passPct || 80) || 80,
    durationSecs: (Number(questionSet.durationMinutes || 45) || 45) * 60,
    proctorEnabled: questionSet.proctorEnabled !== false,
    questions: selectedQuestions,
    answerKey,
    total: selectedQuestions.length,
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

function clearQuestionSetCache(questionSetId = null) {
  if (questionSetId == null) {
    _questionSetCache.clear();
    return;
  }
  _questionSetCache.delete(String(questionSetId));
}

async function hasAppSettingsTable(conn) {
  const rows = await execQuery(
    conn,
    `SELECT COUNT(*) AS CNT
       FROM TABLES
      WHERE SCHEMA_NAME = CURRENT_SCHEMA
        AND TABLE_NAME = 'APP_SETTINGS'`
  );
  return Number(rows?.[0]?.CNT || 0) > 0;
}

async function getAppSetting(conn, key, fallbackValue = null) {
  if (!(await hasAppSettingsTable(conn))) return fallbackValue;
  const rows = await execQuery(conn, 'SELECT SETTING_VALUE FROM APP_SETTINGS WHERE SETTING_KEY = ?', [key]);
  if (!rows.length) return fallbackValue;
  return rows[0].SETTING_VALUE == null ? fallbackValue : String(rows[0].SETTING_VALUE);
}

async function setAppSetting(conn, key, value) {
  if (!(await hasAppSettingsTable(conn))) throw new Error('app_settings_missing');
  await execQuery(
    conn,
    `MERGE INTO APP_SETTINGS T
      USING (SELECT ? AS SETTING_KEY, ? AS SETTING_VALUE FROM DUMMY) S
         ON (T.SETTING_KEY = S.SETTING_KEY)
      WHEN MATCHED THEN UPDATE SET
        T.SETTING_VALUE = S.SETTING_VALUE,
        T.UPDATED_AT = CURRENT_UTCTIMESTAMP
      WHEN NOT MATCHED THEN INSERT
        (SETTING_KEY, SETTING_VALUE, UPDATED_AT)
        VALUES (S.SETTING_KEY, S.SETTING_VALUE, CURRENT_UTCTIMESTAMP)`,
    [key, String(value)]
  );
}

async function getExamEnabled(conn) {
  const value = await getAppSetting(conn, APP_SETTING_EXAMS_ENABLED, EXAM_ACTIVE ? 'true' : 'false');
  return String(value).toLowerCase() !== 'false';
}

function normalizeQuestionSetRow(row) {
  const examMode = String(row.EXAM_MODE || 'GRADED').toUpperCase() === 'PRACTICE' ? 'PRACTICE' : 'GRADED';
  return {
    id: Number(row.QUESTION_SET_ID),
    name: String(row.NAME || 'Exam'),
    description: row.DESCRIPTION || '',
    isActive: Boolean(row.IS_ACTIVE),
    durationMinutes: Number(row.DURATION_MINUTES || 45),
    passPct: Number(row.PASS_PCT || 80),
    proctorEnabled: row.PROCTOR_ENABLED == null ? true : Boolean(row.PROCTOR_ENABLED),
    examMode,
    showCorrectAnswers: row.SHOW_CORRECT_ANSWERS == null ? examMode === 'PRACTICE' : Boolean(row.SHOW_CORRECT_ANSWERS),
    countsTowardResults: row.COUNTS_TOWARD_RESULTS == null ? examMode !== 'PRACTICE' : Boolean(row.COUNTS_TOWARD_RESULTS),
    numQuestions: row.NUM_QUESTIONS == null ? null : Number(row.NUM_QUESTIONS),
    createdAt: row.CREATED_AT ? new Date(row.CREATED_AT).toISOString() : null,
    updatedAt: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
  };
}

async function getQuestionSetRows(conn, options = {}) {
  const includeCounts = Boolean(options.includeCounts);
  const hasModeColumns = await hasQuestionSetModeColumns(conn);
  const modeSelect = hasModeColumns
    ? 'qs.EXAM_MODE, qs.SHOW_CORRECT_ANSWERS, qs.COUNTS_TOWARD_RESULTS,'
    : `'GRADED' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const modeGroup = hasModeColumns
    ? 'qs.EXAM_MODE, qs.SHOW_CORRECT_ANSWERS, qs.COUNTS_TOWARD_RESULTS,'
    : '';
  const modeSelectPlain = hasModeColumns
    ? 'EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS,'
    : `'GRADED' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const rows = includeCounts
    ? await execQuery(
        conn,
        `SELECT qs.QUESTION_SET_ID, qs.NAME, qs.DESCRIPTION, qs.IS_ACTIVE,
                qs.DURATION_MINUTES, qs.PASS_PCT, qs.PROCTOR_ENABLED, ${modeSelect} qs.NUM_QUESTIONS,
                qs.CREATED_AT, qs.UPDATED_AT,
                COUNT(q.QUESTION_ID) AS QUESTION_COUNT
           FROM QUESTION_SETS qs
           LEFT JOIN QUESTION_SET_QUESTIONS q ON q.QUESTION_SET_ID = qs.QUESTION_SET_ID
          GROUP BY qs.QUESTION_SET_ID, qs.NAME, qs.DESCRIPTION, qs.IS_ACTIVE,
                   qs.DURATION_MINUTES, qs.PASS_PCT, qs.PROCTOR_ENABLED, ${modeGroup} qs.NUM_QUESTIONS,
                   qs.CREATED_AT, qs.UPDATED_AT
          ORDER BY qs.IS_ACTIVE DESC, qs.NAME ASC, qs.QUESTION_SET_ID ASC`
      )
    : await execQuery(
        conn,
        `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
                DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelectPlain} NUM_QUESTIONS,
                CREATED_AT, UPDATED_AT
           FROM QUESTION_SETS
          ORDER BY IS_ACTIVE DESC, NAME ASC, QUESTION_SET_ID ASC`
      );
  return rows.map((row) => ({
    ...normalizeQuestionSetRow(row),
    ...(includeCounts ? { questionCount: Number(row.QUESTION_COUNT || 0) } : {})
  }));
}

async function getActiveQuestionSetRow(conn) {
  const hasModeColumns = await hasQuestionSetModeColumns(conn);
  const modeSelect = hasModeColumns
    ? 'EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS,'
    : `'GRADED' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const rows = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
            DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelect} NUM_QUESTIONS,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SETS
      WHERE IS_ACTIVE = TRUE
      ORDER BY QUESTION_SET_ID ASC
      LIMIT 1`
  );
  if (rows.length) return normalizeQuestionSetRow(rows[0]);
  const fallback = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
            DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelect} NUM_QUESTIONS,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SETS
      ORDER BY QUESTION_SET_ID ASC
      LIMIT 1`
  );
  return fallback.length ? normalizeQuestionSetRow(fallback[0]) : null;
}

async function resolveQuestionSetIdForCode(conn, code) {
  const hasDeletedAt = await hasDeletedAtColumn(conn);
  const assignedRows = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID
       FROM ACCESS_CODES
      WHERE ACCESS_CODE = ?
        ${hasDeletedAt ? 'AND DELETED_AT IS NULL' : ''}`,
    [code]
  );
  const assigned = assignedRows?.[0]?.QUESTION_SET_ID;
  if (assigned != null) return Number(assigned);
  const active = await getActiveQuestionSetRow(conn);
  return active ? Number(active.id) : null;
}

async function loadQuestionSet(conn, questionSetId, options = {}) {
  const allowEmpty = Boolean(options.allowEmpty);
  const cacheKey = String(questionSetId);
  if (!allowEmpty && _questionSetCache.has(cacheKey)) return _questionSetCache.get(cacheKey);

  const hasModeColumns = await hasQuestionSetModeColumns(conn);
  const modeSelect = hasModeColumns
    ? 'EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS,'
    : `'GRADED' AS EXAM_MODE, FALSE AS SHOW_CORRECT_ANSWERS, TRUE AS COUNTS_TOWARD_RESULTS,`;
  const metaRows = await execQuery(
    conn,
    `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE,
            DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, ${modeSelect} NUM_QUESTIONS,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SETS
      WHERE QUESTION_SET_ID = ?`,
    [questionSetId]
  );
  if (!metaRows.length) throw new Error(`Question set ${questionSetId} not found.`);

  const sectionRows = await execQuery(
    conn,
    `SELECT SECTION_ID, QUESTION_SET_ID, NAME, DESCRIPTION, DISPLAY_ORDER, DRAW_COUNT,
            CREATED_AT, UPDATED_AT
       FROM QUESTION_SECTIONS
      WHERE QUESTION_SET_ID = ?
      ORDER BY DISPLAY_ORDER ASC, SECTION_ID ASC`,
    [questionSetId]
  );

  const questionRows = await execQuery(
    conn,
    `SELECT q.QUESTION_ID, q.QUESTION_SET_ID, q.SECTION_ID, q.QUESTION_INDEX,
            q.STEM, q.NOTE, q.OPTS_JSON, q.ANSWER_JSON, q.MULTI,
            s.NAME AS SECTION_NAME, s.DISPLAY_ORDER AS SECTION_ORDER, s.DRAW_COUNT AS SECTION_DRAW_COUNT
       FROM QUESTION_SET_QUESTIONS q
       LEFT JOIN QUESTION_SECTIONS s ON s.SECTION_ID = q.SECTION_ID
      WHERE q.QUESTION_SET_ID = ?
      ORDER BY q.QUESTION_INDEX ASC, q.QUESTION_ID ASC`,
    [questionSetId]
  );

  const questions = questionRows.map((row) => {
    const opts = parseJsonOrNull(row.OPTS_JSON);
    const answer = parseJsonOrNull(row.ANSWER_JSON);
    if (!Array.isArray(opts) || !Array.isArray(answer)) {
      throw new Error(`Invalid question payload for QUESTION_ID=${row.QUESTION_ID}`);
    }
    return {
      questionId: Number(row.QUESTION_ID),
      questionSetId: Number(row.QUESTION_SET_ID),
      sectionId: row.SECTION_ID == null ? null : Number(row.SECTION_ID),
      questionIndex: Number(row.QUESTION_INDEX),
      stem: String(row.STEM || ''),
      note: row.NOTE || null,
      opts: opts.map((opt) => String(opt)),
      answer: answer.map((value) => Number(value)),
      multi: Boolean(row.MULTI),
      sectionName: row.SECTION_NAME || '',
      sectionOrder: row.SECTION_ORDER == null ? 0 : Number(row.SECTION_ORDER),
      sectionDrawCount: row.SECTION_DRAW_COUNT == null ? null : Number(row.SECTION_DRAW_COUNT)
    };
  });

  if (!allowEmpty && !questions.length) {
    throw new Error(`Question set ${questionSetId} has no questions loaded.`);
  }

  const questionSet = {
    ...normalizeQuestionSetRow(metaRows[0]),
    sections: sectionRows.map((row) => ({
      id: Number(row.SECTION_ID),
      questionSetId: Number(row.QUESTION_SET_ID),
      name: String(row.NAME || ''),
      description: row.DESCRIPTION || '',
      displayOrder: Number(row.DISPLAY_ORDER || 0),
      drawCount: row.DRAW_COUNT == null ? null : Number(row.DRAW_COUNT),
      createdAt: row.CREATED_AT ? new Date(row.CREATED_AT).toISOString() : null,
      updatedAt: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
    })),
    questions,
    totalQuestions: questions.length
  };

  if (!allowEmpty) _questionSetCache.set(cacheKey, questionSet);
  return questionSet;
}

async function loadResolvedQuestionSet(conn, code) {
  const questionSetId = await resolveQuestionSetIdForCode(conn, code);
  if (!questionSetId) throw new Error('No active question set is configured.');
  return loadQuestionSet(conn, questionSetId);
}

function pickQuestionsForSession(questionSet, code) {
  const allQuestions = Array.isArray(questionSet?.questions) ? [...questionSet.questions] : [];
  if (!allQuestions.length) throw new Error('Question set is empty.');

  const requested = questionSet.numQuestions == null ? allQuestions.length : Number(questionSet.numQuestions);
  const targetCount = Math.max(1, Math.min(Number.isFinite(requested) ? requested : allQuestions.length, allQuestions.length));
  if (targetCount >= allQuestions.length) {
    return allQuestions.sort((a, b) => a.questionIndex - b.questionIndex);
  }

  const rng = makePRNG(`${code}:${questionSet.id}`);
  const hasSectionQuotas = allQuestions.some((q) => q.sectionId != null && q.sectionDrawCount != null);
  if (!hasSectionQuotas) {
    return seededShuffle(allQuestions, rng)
      .slice(0, targetCount)
      .sort((a, b) => a.questionIndex - b.questionIndex);
  }

  const chosen = [];
  const used = new Set();
  const bySection = new Map();

  for (const question of allQuestions) {
    const key = question.sectionId == null ? '__unsectioned__' : String(question.sectionId);
    if (!bySection.has(key)) bySection.set(key, []);
    bySection.get(key).push(question);
  }

  for (const [key, items] of bySection.entries()) {
    const quota = key === '__unsectioned__' ? null : items[0].sectionDrawCount;
    if (quota == null) continue;
    const pickCount = Math.max(0, Math.min(Number(quota) || 0, items.length, targetCount - chosen.length));
    const sample = seededShuffle(items, rng).slice(0, pickCount);
    for (const q of sample) {
      chosen.push(q);
      used.add(q.questionId);
    }
    if (chosen.length >= targetCount) break;
  }

  if (chosen.length < targetCount) {
    const remaining = allQuestions.filter((q) => !used.has(q.questionId));
    const fill = seededShuffle(remaining, rng).slice(0, targetCount - chosen.length);
    chosen.push(...fill);
  }

  return chosen
    .slice(0, targetCount)
    .sort((a, b) => a.questionIndex - b.questionIndex);
}

function buildExamConfigForSet(questionSet, totalOverride = null, examEnabled = EXAM_ACTIVE) {
  const total = totalOverride == null
    ? (questionSet.numQuestions == null ? Number(questionSet.totalQuestions || 0) : Math.min(Number(questionSet.numQuestions || 0), Number(questionSet.totalQuestions || 0)))
    : Number(totalOverride || 0);
  const durationSecs = (Number(questionSet.durationMinutes || 45) || 45) * 60;
  const passPct = Number(questionSet.passPct || 80) || 80;
  return {
    examName: questionSet.name || EXAM_NAME,
    examDescription: questionSet.description || '',
    examActive: examEnabled,
    durationSecs,
    passPct,
    passScore: Math.ceil(total * passPct / 100),
    total,
    proctorEnabled: questionSet.proctorEnabled !== false,
    examMode: questionSet.examMode || 'GRADED',
    isPractice: questionSet.examMode === 'PRACTICE',
    showCorrectAnswers: questionSet.showCorrectAnswers === true,
    countsTowardResults: questionSet.countsTowardResults !== false
  };
}

async function getCodeRow(conn, code) {
  const hasNotes = await hasNotesColumn(conn);
  const hasDeletedAt = await hasDeletedAtColumn(conn);
  const rows = await execQuery(
    conn,
    `SELECT ACCESS_CODE, LABEL, ${hasNotes ? 'NOTES,' : ''} STATUS, SCORE, PCT, PASS, CREATED_AT
       FROM ACCESS_CODES
      WHERE ACCESS_CODE = ?
        ${hasDeletedAt ? 'AND DELETED_AT IS NULL' : ''}`,
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

function sanitizeCandidateResult(result) {
  if (!result || typeof result !== 'object') return result;
  const isPractice = result.examMode === 'PRACTICE' || result.isPractice === true;
  const showCorrectAnswers = isPractice && result.showCorrectAnswers === true;
  const safe = {
    ...result,
    isPractice,
    showCorrectAnswers
  };
  if (!showCorrectAnswers) {
    delete safe.questionResults;
  }
  return safe;
}

async function syncAccessCodeSummaryFromResult(conn, code, result) {
  const summary = officialSummaryFields(result);
  await execQuery(
    conn,
    `UPDATE ACCESS_CODES
        SET STATUS = 'completed',
            SCORE = ?,
            PCT = ?,
            PASS = ?,
            UPDATED_AT = CURRENT_UTCTIMESTAMP
      WHERE ACCESS_CODE = ?`,
    [summary.score, summary.pct, summary.pass, code]
  );
}

async function saveResult(conn, code, result) {
  const summary = officialSummaryFields(result);
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
      summary.score,
      result.total ?? 0,
      summary.pct,
      summary.pass,
      result.autoSubmit ? 1 : 0,
      result.durationSecs ?? 0,
      result.tabSwitches ?? 0,
      result.incidentCount ?? 0,
      JSON.stringify(result)
    ]
  );
}

function officialSummaryFields(result) {
  if (result?.countsTowardResults === false) {
    return { score: null, pct: null, pass: null };
  }
  return {
    score: result?.score ?? null,
    pct: result?.pct ?? null,
    pass: result?.pass == null ? null : (result.pass ? 1 : 0)
  };
}

async function updateCodeStatus(conn, code, status, result = null) {
  const summary = officialSummaryFields(result);
  await execQuery(
    conn,
    `UPDATE ACCESS_CODES
        SET STATUS = ?,
            SCORE = ?,
            PCT = ?,
            PASS = ?
      WHERE ACCESS_CODE = ?`,
    [status, summary.score, summary.pct, summary.pass, code]
  );
}

function gradeExamFromSession(session, answers) {
  const answerKey = Array.isArray(session.answerKey) ? session.answerKey : [];
  let score = 0;
  const questionResults = [];
  const sectionMap = new Map();

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
    const question = session.questions?.[questionIdx];
    const displayOptions = Array.isArray(optionOrder)
      ? optionOrder.map((idx) => String(question?.opts?.[idx] || ''))
      : (question?.opts || []).map((opt) => String(opt));
    const toDisplayIndexes = (originalIndexes) => originalIndexes
      .map((originalIdx) => optionOrder.findIndex((idx) => idx === originalIdx))
      .filter((idx) => idx >= 0)
      .sort((a, b) => a - b);
    questionResults.push({
      displayIdx,
      questionIndex: question?.questionIndex ?? questionIdx,
      questionId: question?.questionId ?? null,
      correct,
      given: originalSelection,
      expected,
      givenDisplay: toDisplayIndexes(originalSelection),
      expectedDisplay: toDisplayIndexes(expected),
      stem: question?.stem || '',
      note: question?.note || null,
      opts: Array.isArray(question?.opts) ? question.opts.map((opt) => String(opt)) : [],
      displayOptions,
      multi: Boolean(question?.multi),
      sectionId: question?.sectionId ?? null,
      sectionName: question?.sectionName || ''
    });
    if (question?.sectionId != null) {
      const key = String(question.sectionId);
      if (!sectionMap.has(key)) {
        sectionMap.set(key, {
          sectionId: question.sectionId,
          name: question.sectionName || 'Section',
          displayOrder: Number(question.sectionOrder || 0),
          correct: 0,
          total: 0
        });
      }
      const section = sectionMap.get(key);
      section.total += 1;
      if (correct) section.correct += 1;
    }
  });

  const total = Number(session.total || session.questions?.length || 0);
  const pct = Math.round((score / total) * 100);
  const pass = pct >= Number(session.passPct || 80);
  const sectionResults = [...sectionMap.values()]
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .map((section) => ({
      sectionId: section.sectionId,
      name: section.name,
      correct: section.correct,
      total: section.total,
      pct: section.total ? Math.round((section.correct / section.total) * 100) : 0
    }));
  return { score, total, pct, pass, questionResults, sectionResults };
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
    const status = await withDb(async (conn) => {
      await execQuery(conn, 'SELECT 1 AS OK FROM DUMMY');
      const activeSet = await getActiveQuestionSetRow(conn);
      if (!activeSet) throw new Error('No active question set found.');
      const questionSet = await loadQuestionSet(conn, activeSet.id);
      const setRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM QUESTION_SETS');
      const examEnabled = await getExamEnabled(conn);
      return {
        activeSet,
        questionSet,
        setCount: Number(setRows?.[0]?.CNT || 0),
        examEnabled
      };
    });
    res.json({
      ok: true,
      db: 'connected',
      schema: HANA_SCHEMA,
      totalQuestions: status.questionSet.totalQuestions,
      totalQuestionSets: status.setCount,
      examActive: status.examEnabled,
      activeQuestionSet: {
        id: status.activeSet.id,
        name: status.activeSet.name
      }
    });
  } catch (err) {
    appLog('error', 'health_failed', { message: err.message });
    res.status(500).json({ ok: false, message: err.message });
  }
});

app.get('/api/status', async (_req, res) => {
  try {
    const status = await withDb(async (conn) => {
      const activeSet = await getActiveQuestionSetRow(conn);
      if (!activeSet) throw new Error('No active question set configured.');
      const questionSet = await loadQuestionSet(conn, activeSet.id);
      const examEnabled = await getExamEnabled(conn);
      return buildExamConfigForSet(questionSet, null, examEnabled);
    });
    res.json(status);
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
    const result = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) return { valid: false, reason: 'not_found' };
      const questionSet = await loadResolvedQuestionSet(conn, code);
      const examEnabled = await getExamEnabled(conn);
      const cfg = buildExamConfigForSet(questionSet, null, examEnabled);
      if (!cfg.examActive) return { valid: false, reason: 'exam_not_active', ...cfg };

      const savedResult = await getResultRecord(conn, code);
      if (savedResult || codeRow.status === 'completed') {
        return { valid: true, status: 'completed', result: sanitizeCandidateResult(savedResult) || null, questionSet: { id: questionSet.id, name: questionSet.name }, ...cfg };
      }

      const progress = await getSavedSession(conn, code);
      if (progress || codeRow.status === 'active') {
        return { valid: true, status: 'active', progress: progress || null, questionSet: { id: questionSet.id, name: questionSet.name }, ...cfg };
      }

      return { valid: true, status: 'unused', questionSet: { id: questionSet.id, name: questionSet.name }, ...cfg };
    });

    res.json(result);
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
    const payload = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) return { status: 404, body: { error: 'code_not_found' } };
      const questionSet = await loadResolvedQuestionSet(conn, code);
      const examEnabled = await getExamEnabled(conn);
      const sessionConfig = buildExamConfigForSet(questionSet, null, examEnabled);
      if (!sessionConfig.examActive) {
        return { status: 409, body: { error: 'exam_not_active', ...sessionConfig } };
      }

      const savedResult = await getResultRecord(conn, code);
      if (savedResult || codeRow.status === 'completed') {
        return { status: 409, body: { error: 'exam_completed' } };
      }

      if (fresh) await deleteSession(conn, code);
      const progress = fresh ? null : await getSavedSession(conn, code);
      const { token } = createExamSessionFromSet(code, questionSet);
      await updateCodeStatus(conn, code, 'active');

      return {
        status: 200,
        body: {
          ok: true,
          examToken: token,
          progress,
          questionSet: { id: questionSet.id, name: questionSet.name },
          ...sessionConfig
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
    const questionIdx = session.qOrder[displayIdx];
    const question = session.questions[questionIdx];
    const optionOrder = session.optOrders[displayIdx];
    if (!question) return res.status(404).json({ error: 'question_not_found' });
    res.json({
      displayIdx,
      total: session.total,
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
  const durationSecs = Math.max(10, Math.min(Number(req.body?.durationSecs) || 0, Number(session.durationSecs || EXAM_DURATION_SECS) + 300));
  const tabSwitches = Number(req.body?.tabSwitches) || 0;
  const incidents = Array.isArray(req.body?.incidents) ? req.body.incidents : [];
  const autoSubmit = Boolean(req.body?.autoSubmit);

  try {
    const result = gradeExamFromSession(session, answers);
    const record = {
      code,
      questionSetId: session.questionSetId,
      questionSetName: session.questionSetName,
      examMode: session.examMode || 'GRADED',
      isPractice: session.examMode === 'PRACTICE',
      showCorrectAnswers: session.showCorrectAnswers === true,
      countsTowardResults: session.countsTowardResults !== false,
      score: result.score,
      total: result.total,
      pct: result.pct,
      pass: result.pass,
      autoSubmit,
      durationSecs,
      tabSwitches,
      incidents,
      incidentCount: incidents.length,
      questionResults: result.questionResults,
      sectionResults: result.sectionResults,
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
    res.json({ ok: true, result: sanitizeCandidateResult(record) });
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
    res.json({ result: sanitizeCandidateResult(result) });
  } catch (err) {
    appLog('error', 'result_fetch_failed', { code, message: err.message });
    res.status(500).json({ error: 'result_fetch_failed', message: err.message });
  }
});

app.post('/api/admin/login', (req, res) => {
  if (!ADMIN_HASH && !MANAGER_HASH) return res.status(503).json({ ok: false, error: 'admin_not_configured' });
  const ip = getClientIp(req);
  if (!checkRateLimit(String(ip))) return res.status(429).json({ ok: false, error: 'too_many_attempts' });

  const hash = String(req.body?.hash || '').trim().toLowerCase();
  const role = hash && hash === ADMIN_HASH
    ? 'admin'
    : (MANAGER_HASH && hash === MANAGER_HASH ? 'manager' : null);
  if (!role) {
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
    actor: role,
    clientIp: ip,
    details: { ok: true, role }
  });
  return res.json({ ok: true, token: createAdminToken(role), role });
});

app.get('/api/admin/codes', requireAdmin, async (_req, res) => {
  try {
    const payload = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const [rows, questionSets, examEnabled] = await Promise.all([
        execQuery(
          conn,
          `SELECT c.ACCESS_CODE, c.LABEL, ${hasNotes ? 'c.NOTES,' : `'' AS NOTES,`} c.STATUS, c.SCORE, c.PCT, c.PASS,
                  c.QUESTION_SET_ID, qs.NAME AS QUESTION_SET_NAME, qs.IS_ACTIVE AS QUESTION_SET_ACTIVE,
                  r.SCORE AS RESULT_SCORE, r.PCT AS RESULT_PCT, r.PASS AS RESULT_PASS,
                  r.DURATION_SECS, r.TAB_SWITCHES, r.INCIDENT_COUNT, r.SUBMITTED_AT, r.RESULT_JSON
             FROM ACCESS_CODES c
             LEFT JOIN QUESTION_SETS qs ON qs.QUESTION_SET_ID = c.QUESTION_SET_ID
             LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
            ${hasDeletedAt ? 'WHERE c.DELETED_AT IS NULL' : ''}
            ORDER BY c.ACCESS_CODE ASC`
        ),
        getQuestionSetRows(conn, { includeCounts: true }),
        getExamEnabled(conn)
      ]);
      return { rows, questionSets, examEnabled };
    });
    const codes = payload.rows.map((r) => {
      const parsedResult = parseJsonOrNull(r.RESULT_JSON);
      const countsTowardResults = parsedResult?.countsTowardResults !== false;
      return {
        code: r.ACCESS_CODE,
        label: r.LABEL || '',
        notes: r.NOTES || '',
        status: r.STATUS || 'unused',
        score: countsTowardResults ? (r.SCORE ?? r.RESULT_SCORE ?? parsedResult?.score ?? null) : null,
        pct: countsTowardResults ? (r.PCT ?? r.RESULT_PCT ?? parsedResult?.pct ?? null) : null,
        pass: countsTowardResults
          ? (r.PASS === null || r.PASS === undefined
              ? (r.RESULT_PASS === null || r.RESULT_PASS === undefined ? (parsedResult?.pass ?? null) : Boolean(r.RESULT_PASS))
              : Boolean(r.PASS))
          : null,
        durationSecs: r.DURATION_SECS,
        tabSwitches: r.TAB_SWITCHES || 0,
        incidentCount: r.INCIDENT_COUNT || 0,
        submittedAt: r.SUBMITTED_AT ? new Date(r.SUBMITTED_AT).toISOString() : null,
        incidents: parsedResult?.incidents || [],
        questionResults: Array.isArray(parsedResult?.questionResults) ? parsedResult.questionResults : [],
        questionSetId: r.QUESTION_SET_ID == null ? null : Number(r.QUESTION_SET_ID),
        questionSetName: r.QUESTION_SET_NAME || '',
        questionSetActive: r.QUESTION_SET_ACTIVE == null ? false : Boolean(r.QUESTION_SET_ACTIVE),
        examMode: parsedResult?.examMode || '',
        isPractice: parsedResult?.examMode === 'PRACTICE' || parsedResult?.isPractice === true,
        countsTowardResults
      };
    });
    res.json({
      codes,
      questionSets: payload.questionSets,
      examActive: payload.examEnabled,
      role: _req.adminRole || 'admin'
    });
  } catch (err) {
    appLog('error', 'admin_codes_failed', { message: err.message });
    res.status(500).json({ error: 'admin_codes_failed', message: err.message });
  }
});

app.get('/api/admin/system-status', requireAdmin, async (_req, res) => {
  try {
    const status = await withDb(async (conn) => {
      const questionSets = await getQuestionSetRows(conn, { includeCounts: true });
      const activeSet = questionSets.find((set) => set.isActive) || questionSets[0] || null;
      const activeQuestionSet = activeSet ? await loadQuestionSet(conn, activeSet.id) : null;
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const auditEnabled = await hasAuditLogTable(conn);
      const examEnabled = await getExamEnabled(conn);
      const accessCodeRows = await execQuery(conn, `SELECT COUNT(*) AS CNT FROM ACCESS_CODES ${hasDeletedAt ? 'WHERE DELETED_AT IS NULL' : ''}`);
      const resultRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM EXAM_RESULTS');
      const sessionRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM EXAM_SESSIONS');
      const questionRows = await execQuery(conn, 'SELECT COUNT(*) AS CNT FROM QUESTION_SET_QUESTIONS');
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
        ok: Boolean(activeQuestionSet && activeQuestionSet.totalQuestions > 0),
        schema: HANA_SCHEMA,
        questionCount: Number(questionRows?.[0]?.CNT || 0),
        questionSetCount: questionSets.length,
        activeQuestionSet: activeSet ? { id: activeSet.id, name: activeSet.name } : null,
        activeQuestionCount: activeQuestionSet ? activeQuestionSet.totalQuestions : 0,
        accessCodeCount: Number(accessCodeRows?.[0]?.CNT || 0),
        resultCount: Number(resultRows?.[0]?.CNT || 0),
        activeSessionCount: Number(sessionRows?.[0]?.CNT || 0),
        examEnabled,
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
        managerConfigured: Boolean(MANAGER_HASH),
        warnings: [
          ...(questionSets.length ? [] : ['No question sets found.']),
          ...(activeQuestionSet && activeQuestionSet.totalQuestions > 0 ? [] : ['Active question set has no questions.']),
          ...(examEnabled ? [] : ['Exam access is currently disabled. Candidates cannot enter codes.']),
          ...(hasNotes ? [] : ['ACCESS_CODES.NOTES column is missing.']),
          ...(staleSessionRows.length ? [`${staleSessionRows.length} active session(s) look stale (${STALE_SESSION_MINUTES}+ min without a save).`] : []),
          ...(auditEnabled ? [] : ['ADMIN_AUDIT_LOG table is missing.']),
          ...(ADMIN_HASH ? [] : ['ADMIN_HASH is not configured on the server.']),
          ...(MANAGER_HASH ? [] : ['MANAGER_HASH is not configured. Manager role login is disabled until it is added.'])
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
      questionSetCount: 0,
      activeQuestionSet: null,
      activeQuestionCount: 0,
      accessCodeCount: 0,
      resultCount: 0,
      activeSessionCount: 0,
      examEnabled: EXAM_ACTIVE,
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
      managerConfigured: Boolean(MANAGER_HASH),
      warnings: ['Could not load system status from HANA.'],
      error: 'admin_system_status_failed'
    });
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

app.post('/api/admin/exam-availability', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const enabled = req.body?.enabled !== false;
  try {
    await withDb(async (conn) => {
      await setAppSetting(conn, APP_SETTING_EXAMS_ENABLED, enabled ? 'true' : 'false');
      await writeAdminAudit(conn, {
        action: 'admin_exam_availability_updated',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { enabled }
      });
    });
    res.json({ ok: true, enabled });
  } catch (err) {
    appLog('error', 'admin_exam_availability_failed', { message: err.message });
    res.status(500).json({ error: 'admin_exam_availability_failed' });
  }
});

app.get('/api/admin/results/:code/review', requireAdmin, async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  try {
    const payload = await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) throw new Error('code_not_found');
      const result = await getResultRecord(conn, code);
      if (!result) throw new Error('result_not_found');
      return {
        code,
        label: codeRow.label || '',
        status: codeRow.status || 'completed',
        result
      };
    });
    res.json({
      ok: true,
      code: payload.code,
      label: payload.label,
      status: payload.status,
      result: payload.result,
      reviewAvailable: Array.isArray(payload.result?.questionResults) && payload.result.questionResults.length > 0
    });
  } catch (err) {
    const status = err.message === 'code_not_found' || err.message === 'result_not_found' ? 404 : 500;
    res.status(status).json({ error: err.message });
  }
});

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((p / 100) * sorted.length)));
  return sorted[idx];
}

app.get('/api/admin/question-sets/:id/analytics', requireAdmin, async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    const payload = await withDb(async (conn) => {
      const questionSet = await loadQuestionSet(conn, id, { allowEmpty: true });
      const rows = await execQuery(
        conn,
        `SELECT r.ACCESS_CODE, r.SCORE, r.TOTAL, r.PCT, r.PASS, r.DURATION_SECS, r.RESULT_JSON, r.SUBMITTED_AT,
                c.LABEL, c.QUESTION_SET_ID
           FROM EXAM_RESULTS r
           LEFT JOIN ACCESS_CODES c ON c.ACCESS_CODE = r.ACCESS_CODE
          ORDER BY r.SUBMITTED_AT DESC`,
        []
      );

      const attempts = rows
        .map((row) => {
          const result = parseJsonOrNull(row.RESULT_JSON) || {};
          const score = Number(row.SCORE ?? result.score);
          const total = Number(row.TOTAL ?? result.total);
          const pct = Number(row.PCT ?? result.pct);
          const durationSecs = Number(row.DURATION_SECS ?? result.durationSecs);
          return {
            code: row.ACCESS_CODE,
            label: row.LABEL || '',
            questionSetId: Number(row.QUESTION_SET_ID ?? result.questionSetId ?? 0),
            score: Number.isFinite(score) ? score : null,
            total: Number.isFinite(total) ? total : null,
            pct: Number.isFinite(pct) ? pct : null,
            pass: row.PASS == null ? Boolean(result.pass) : Boolean(row.PASS),
            durationSecs: Number.isFinite(durationSecs) ? durationSecs : null,
            examMode: result.examMode || questionSet.examMode || 'GRADED',
            questionResults: Array.isArray(result.questionResults) ? result.questionResults : [],
            sectionResults: Array.isArray(result.sectionResults) ? result.sectionResults : [],
            submittedAt: row.SUBMITTED_AT ? new Date(row.SUBMITTED_AT).toISOString() : result.submittedAt || null
          };
        })
        .filter((attempt) => Number(attempt.questionSetId || 0) === id);

      const completed = attempts.filter((item) => item.score != null && item.total != null && item.pct != null);
      const pctValues = completed.map((item) => item.pct).filter(Number.isFinite);
      const durationValues = completed.map((item) => item.durationSecs).filter(Number.isFinite);
      const avg = (values) => values.length ? Math.round(values.reduce((sum, value) => sum + value, 0) / values.length) : null;

      const questionMap = new Map();
      const sectionMap = new Map();
      for (const attempt of attempts) {
        for (const qr of attempt.questionResults) {
          const key = qr.questionId != null ? `id:${qr.questionId}` : `idx:${qr.questionIndex}`;
          if (!questionMap.has(key)) {
            questionMap.set(key, {
              questionId: qr.questionId ?? null,
              questionIndex: qr.questionIndex ?? null,
              stem: qr.stem || 'Question',
              sectionName: qr.sectionName || '',
              answered: 0,
              correct: 0,
              wrong: 0
            });
          }
          const item = questionMap.get(key);
          item.answered += 1;
          if (qr.correct) item.correct += 1;
          else item.wrong += 1;
        }
        for (const sr of attempt.sectionResults) {
          const key = sr.sectionId != null ? String(sr.sectionId) : sr.name || 'Section';
          if (!sectionMap.has(key)) {
            sectionMap.set(key, {
              sectionId: sr.sectionId ?? null,
              name: sr.name || 'Section',
              correct: 0,
              total: 0
            });
          }
          const item = sectionMap.get(key);
          item.correct += Number(sr.correct || 0);
          item.total += Number(sr.total || 0);
        }
      }

      const questionStats = [...questionMap.values()]
        .map((item) => ({
          ...item,
          pctCorrect: item.answered ? Math.round((item.correct / item.answered) * 100) : null
        }))
        .filter((item) => item.answered > 0);

      const sectionStats = [...sectionMap.values()]
        .map((item) => ({
          ...item,
          wrong: Math.max(0, item.total - item.correct),
          pctCorrect: item.total ? Math.round((item.correct / item.total) * 100) : null
        }))
        .sort((a, b) => String(a.name).localeCompare(String(b.name)));

      return {
        questionSet: {
          id: questionSet.id,
          name: questionSet.name,
          examMode: questionSet.examMode,
          isPractice: questionSet.examMode === 'PRACTICE',
          questionCount: questionSet.totalQuestions
        },
        summary: {
          attempts: attempts.length,
          completed: completed.length,
          gradedAttempts: attempts.filter((item) => item.examMode !== 'PRACTICE').length,
          practiceAttempts: attempts.filter((item) => item.examMode === 'PRACTICE').length,
          averageScore: completed.length ? Number((completed.reduce((sum, item) => sum + item.score, 0) / completed.length).toFixed(1)) : null,
          averagePct: avg(pctValues),
          passRate: completed.length ? Math.round((completed.filter((item) => item.pass).length / completed.length) * 100) : null,
          averageDurationSecs: avg(durationValues),
          medianDurationSecs: percentile(durationValues, 50)
        },
        hardestQuestions: [...questionStats].sort((a, b) => a.pctCorrect - b.pctCorrect).slice(0, 10),
        easiestQuestions: [...questionStats].sort((a, b) => b.pctCorrect - a.pctCorrect).slice(0, 10),
        sectionStats,
        recentAttempts: attempts.slice(0, 20).map((item) => ({
          code: item.code,
          label: item.label,
          score: item.score,
          total: item.total,
          pct: item.pct,
          pass: item.pass,
          durationSecs: item.durationSecs,
          examMode: item.examMode,
          submittedAt: item.submittedAt
        }))
      };
    });
    res.json({ ok: true, ...payload });
  } catch (err) {
    appLog('error', 'admin_question_set_analytics_failed', { id, message: err.message });
    res.status(500).json({ error: 'admin_question_set_analytics_failed', message: err.message });
  }
});

app.post('/api/admin/clear-stale-sessions', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  try {
    const payload = await withDb(async (conn) => {
      const staleRows = await execQuery(
        conn,
        `SELECT ACCESS_CODE
           FROM EXAM_SESSIONS
          WHERE UPDATED_AT < ADD_SECONDS(CURRENT_UTCTIMESTAMP, ?)
          ORDER BY UPDATED_AT ASC`,
        [-1 * STALE_SESSION_MINUTES * 60]
      );

      const cleared = [];
      for (const row of staleRows) {
        const code = String(row.ACCESS_CODE || '').trim().toUpperCase();
        if (!code) continue;
        await deleteSession(conn, code);
        await execQuery(
          conn,
          `UPDATE ACCESS_CODES
              SET STATUS = 'unused',
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE ACCESS_CODE = ?
              AND STATUS = 'active'
              AND NOT EXISTS (
                SELECT 1 FROM EXAM_RESULTS r WHERE r.ACCESS_CODE = ACCESS_CODES.ACCESS_CODE
              )`,
          [code]
        );
        cleared.push(code);
      }

      await writeAdminAudit(conn, {
        action: 'admin_stale_sessions_cleared',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { count: cleared.length, codes: cleared.slice(0, 20) }
      });

      return { ok: true, clearedCount: cleared.length, clearedCodes: cleared };
    });

    for (const [token, value] of _examSessions.entries()) {
      if (payload.clearedCodes.includes(value.code)) _examSessions.delete(token);
    }

    res.json(payload);
  } catch (err) {
    appLog('error', 'admin_clear_stale_sessions_failed', { message: err.message });
    res.status(500).json({ error: 'admin_clear_stale_sessions_failed' });
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
        actor: req.adminRole || 'admin',
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

app.post('/api/admin/reset', requireAdmin, requireAdminRole('admin'), async (req, res) => {
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

app.delete('/api/admin/codes/:code', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });

  try {
    await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) throw new Error('code_not_found');
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      await deleteSession(conn, code);
      if (hasDeletedAt) {
        await execQuery(
          conn,
          `UPDATE ACCESS_CODES
              SET STATUS = 'deleted',
                  DELETED_AT = CURRENT_UTCTIMESTAMP,
                  DELETED_BY = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE ACCESS_CODE = ?`,
          [req.adminRole || 'admin', code]
        );
      } else {
        await execQuery(conn, 'DELETE FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
        await execQuery(conn, 'DELETE FROM ACCESS_CODES WHERE ACCESS_CODE = ?', [code]);
      }
      await writeAdminAudit(conn, {
        action: 'admin_code_deleted',
        targetCode: code,
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { previousStatus: codeRow.status || 'unknown' }
      });
    });
    for (const [token, value] of _examSessions.entries()) {
      if (value.code === code) _examSessions.delete(token);
    }
    res.json({ ok: true });
  } catch (err) {
    const status = err.message === 'code_not_found' ? 404 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.post('/api/admin/codes/bulk-delete', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const codes = Array.isArray(req.body?.codes)
    ? [...new Set(req.body.codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => /^[A-Z2-9]{6}$/.test(code)))]
    : [];
  if (!codes.length) return res.status(400).json({ error: 'codes_required' });
  if (codes.length > 500) return res.status(400).json({ error: 'too_many_codes' });

  try {
    const payload = await withDb(async (conn) => {
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const deleted = [];
      const notFound = [];
      const summary = { unused: 0, active: 0, completed: 0, other: 0 };
      for (const code of codes) {
        const codeRow = await getCodeRow(conn, code);
        if (!codeRow) {
          notFound.push(code);
          continue;
        }
        summary[summary[codeRow.status] == null ? 'other' : codeRow.status] += 1;
        await deleteSession(conn, code);
        if (hasDeletedAt) {
          await execQuery(
            conn,
            `UPDATE ACCESS_CODES
                SET STATUS = 'deleted',
                    DELETED_AT = CURRENT_UTCTIMESTAMP,
                    DELETED_BY = ?,
                    UPDATED_AT = CURRENT_UTCTIMESTAMP
              WHERE ACCESS_CODE = ?`,
            [req.adminRole || 'admin', code]
          );
        } else {
          await execQuery(conn, 'DELETE FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
          await execQuery(conn, 'DELETE FROM ACCESS_CODES WHERE ACCESS_CODE = ?', [code]);
        }
        deleted.push(code);
      }
      await writeAdminAudit(conn, {
        action: 'admin_codes_bulk_deleted',
        actor: req.adminRole || 'admin',
        clientIp: getClientIp(req),
        details: { count: deleted.length, summary, codes: deleted.slice(0, 50), notFound: notFound.slice(0, 50) }
      });
      return { ok: true, deletedCount: deleted.length, deleted, notFound, summary };
    });
    for (const [token, value] of _examSessions.entries()) {
      if (payload.deleted.includes(value.code)) _examSessions.delete(token);
    }
    res.json(payload);
  } catch (err) {
    appLog('error', 'admin_bulk_delete_codes_failed', { message: err.message });
    res.status(500).json({ error: 'admin_bulk_delete_codes_failed' });
  }
});

app.post('/api/admin/generate', requireAdmin, async (req, res) => {
  const count = Math.min(Math.max(Number(req.body?.count) || 10, 1), 200);
  const chars = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

  try {
    const added = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      const existingRows = await execQuery(conn, 'SELECT ACCESS_CODE FROM ACCESS_CODES');
      const activeRows = hasDeletedAt ? await execQuery(conn, 'SELECT ACCESS_CODE FROM ACCESS_CODES WHERE DELETED_AT IS NULL') : existingRows;
      const used = new Set(existingRows.map((r) => r.ACCESS_CODE));
      const seatBase = activeRows.length + 1;
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
        actor: req.adminRole || 'admin',
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

app.post('/api/admin/results/repair-summaries', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  try {
    const payload = await withDb(async (conn) => {
      const rows = await execQuery(conn, 'SELECT ACCESS_CODE, RESULT_JSON FROM EXAM_RESULTS');
      let repaired = 0;
      let skipped = 0;
      for (const row of rows) {
        const result = parseJsonOrNull(row.RESULT_JSON);
        if (!result || typeof result.score !== 'number' || typeof result.pct !== 'number') {
          skipped += 1;
          continue;
        }
        if (result.countsTowardResults === false) {
          skipped += 1;
          continue;
        }
        await syncAccessCodeSummaryFromResult(conn, row.ACCESS_CODE, result);
        repaired += 1;
      }
      await writeAdminAudit(conn, {
        action: 'admin_result_summaries_repaired',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { repaired, skipped }
      });
      return { repaired, skipped };
    });
    res.json({ ok: true, ...payload });
  } catch (err) {
    appLog('error', 'admin_repair_result_summaries_failed', { message: err.message });
    res.status(500).json({ error: 'admin_repair_result_summaries_failed' });
  }
});

app.post('/api/admin/results/clear-summaries', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'UPDATE ACCESS_CODES SET SCORE = NULL, PCT = NULL, PASS = NULL, UPDATED_AT = CURRENT_UTCTIMESTAMP');
      await execQuery(conn, 'UPDATE EXAM_RESULTS SET SCORE = NULL, PCT = NULL, PASS = NULL');
      await writeAdminAudit(conn, {
        action: 'admin_result_summaries_cleared',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { scope: 'all' }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    appLog('error', 'admin_clear_result_summaries_failed', { message: err.message });
    res.status(500).json({ error: 'admin_clear_result_summaries_failed' });
  }
});

app.post('/api/admin/codes/:code/question-set', requireAdmin, async (req, res) => {
  const code = String(req.params.code || '').trim().toUpperCase();
  const questionSetIdRaw = req.body?.questionSetId;
  const questionSetId = questionSetIdRaw == null || questionSetIdRaw === '' ? null : Number(questionSetIdRaw);
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  if (questionSetIdRaw != null && questionSetIdRaw !== '' && !Number.isInteger(questionSetId)) {
    return res.status(400).json({ error: 'invalid_question_set_id' });
  }

  try {
    await withDb(async (conn) => {
      const codeRow = await getCodeRow(conn, code);
      if (!codeRow) throw new Error('code_not_found');
      if (codeRow.status !== 'unused') throw new Error('code_assignment_requires_unused_status');
      if (questionSetId != null) {
        const qs = await execQuery(conn, 'SELECT QUESTION_SET_ID FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [questionSetId]);
        if (!qs.length) throw new Error('question_set_not_found');
      }
      await execQuery(
        conn,
        'UPDATE ACCESS_CODES SET QUESTION_SET_ID = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE ACCESS_CODE = ?',
        [questionSetId, code]
      );
      await writeAdminAudit(conn, {
        action: 'admin_code_question_set_assigned',
        targetCode: code,
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId }
      });
    });
    res.json({ ok: true, questionSetId });
  } catch (err) {
    const status =
      err.message === 'code_not_found' ? 404 :
      err.message === 'question_set_not_found' ? 404 :
      err.message === 'code_assignment_requires_unused_status' ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.get('/api/admin/question-sets', requireAdmin, async (_req, res) => {
  try {
    const sets = await withDb(async (conn) => getQuestionSetRows(conn, { includeCounts: true }));
    res.json({ sets });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_sets_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  if (!name) return res.status(400).json({ error: 'name_required' });

  try {
    const created = await withDb(async (conn) => {
      await execQuery(
        conn,
        `INSERT INTO QUESTION_SETS
          (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, NUM_QUESTIONS, CREATED_AT, UPDATED_AT)
         VALUES (?, ?, FALSE, 45, 80, TRUE, NULL, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
        [name, description || null]
      );
      const rows = await execQuery(
        conn,
        `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, NUM_QUESTIONS, CREATED_AT, UPDATED_AT
           FROM QUESTION_SETS
          WHERE NAME = ?
          ORDER BY QUESTION_SET_ID DESC
          LIMIT 1`,
        [name]
      );
      await writeAdminAudit(conn, {
        action: 'admin_question_set_created',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { name }
      });
      return normalizeQuestionSetRow(rows[0]);
    });
    res.json({ ok: true, questionSet: created });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_create_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/config', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  const durationMinutes = Math.max(1, Math.min(Number(req.body?.durationMinutes) || 45, 240));
  const passPct = Math.max(1, Math.min(Number(req.body?.passPct) || 80, 100));
  const proctorEnabled = req.body?.proctorEnabled !== false;
  const examMode = String(req.body?.examMode || 'GRADED').toUpperCase() === 'PRACTICE' ? 'PRACTICE' : 'GRADED';
  const showCorrectAnswers = examMode === 'PRACTICE' && req.body?.showCorrectAnswers !== false;
  const countsTowardResults = examMode === 'PRACTICE' ? false : req.body?.countsTowardResults !== false;
  const numQuestionsRaw = req.body?.numQuestions;
  const numQuestions = numQuestionsRaw == null || numQuestionsRaw === '' ? null : Math.max(1, Number(numQuestionsRaw));
  if (!name) return res.status(400).json({ error: 'name_required' });

  try {
    await withDb(async (conn) => {
      if (await hasQuestionSetModeColumns(conn)) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SETS
              SET NAME = ?,
                  DESCRIPTION = ?,
                  DURATION_MINUTES = ?,
                  PASS_PCT = ?,
                  PROCTOR_ENABLED = ?,
                  EXAM_MODE = ?,
                  SHOW_CORRECT_ANSWERS = ?,
                  COUNTS_TOWARD_RESULTS = ?,
                  NUM_QUESTIONS = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_SET_ID = ?`,
          [name, description || null, durationMinutes, passPct, proctorEnabled ? 1 : 0, examMode, showCorrectAnswers ? 1 : 0, countsTowardResults ? 1 : 0, numQuestions, id]
        );
      } else {
        await execQuery(
          conn,
          `UPDATE QUESTION_SETS
              SET NAME = ?,
                  DESCRIPTION = ?,
                  DURATION_MINUTES = ?,
                  PASS_PCT = ?,
                  PROCTOR_ENABLED = ?,
                  NUM_QUESTIONS = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_SET_ID = ?`,
          [name, description || null, durationMinutes, passPct, proctorEnabled ? 1 : 0, numQuestions, id]
        );
      }
      clearQuestionSetCache(id);
      await writeAdminAudit(conn, {
        action: 'admin_question_set_config_updated',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: id, name, durationMinutes, passPct, proctorEnabled, examMode, showCorrectAnswers, countsTowardResults, numQuestions }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_config_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:id/activate', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'UPDATE QUESTION_SETS SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_UTCTIMESTAMP');
      await execQuery(
        conn,
        'UPDATE QUESTION_SETS SET IS_ACTIVE = TRUE, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE QUESTION_SET_ID = ?',
        [id]
      );
      clearQuestionSetCache();
      await writeAdminAudit(conn, {
        action: 'admin_question_set_activated',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: id }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_activate_failed', message: err.message });
  }
});

app.delete('/api/admin/question-sets/:id', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    await withDb(async (conn) => {
      const rows = await execQuery(conn, 'SELECT IS_ACTIVE FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      if (!rows.length) throw new Error('question_set_not_found');
      if (rows[0].IS_ACTIVE) throw new Error('cannot_delete_active_question_set');
      await execQuery(conn, 'DELETE FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      clearQuestionSetCache(id);
      await writeAdminAudit(conn, {
        action: 'admin_question_set_deleted',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: id }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    const status =
      err.message === 'question_set_not_found' ? 404 :
      err.message === 'cannot_delete_active_question_set' ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

app.get('/api/admin/question-sets/:id/questions', requireAdmin, async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    const payload = await withDb(async (conn) => {
      const questionSet = await loadQuestionSet(conn, id, { allowEmpty: true });
      return {
        questionSet: {
          id: questionSet.id,
          name: questionSet.name,
          description: questionSet.description,
          isActive: questionSet.isActive,
          examMode: questionSet.examMode,
          showCorrectAnswers: questionSet.showCorrectAnswers,
          countsTowardResults: questionSet.countsTowardResults,
          proctorEnabled: questionSet.proctorEnabled,
          durationMinutes: questionSet.durationMinutes,
          passPct: questionSet.passPct,
          numQuestions: questionSet.numQuestions
        },
        questions: questionSet.questions.map((question) => ({
          id: question.questionId,
          qNum: question.questionIndex,
          stem: question.stem,
          note: question.note || '',
          opts: question.opts,
          correctIndices: question.answer,
          multi: Boolean(question.multi),
          sectionId: question.sectionId
        }))
      };
    });
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_questions_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:setId/questions', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const setId = Number(req.params.setId);
  if (!Number.isInteger(setId)) return res.status(400).json({ error: 'invalid_question_set_id' });

  const questionId = req.body?.id == null || req.body?.id === '' ? null : Number(req.body.id);
  const qNum = Number(req.body?.qNum);
  const stem = String(req.body?.stem || '').trim();
  const note = String(req.body?.note || '').trim();
  const opts = Array.isArray(req.body?.opts) ? req.body.opts.map((item) => String(item || '').trim()).filter(Boolean) : [];
  const correctIndices = Array.isArray(req.body?.correctIndices) ? req.body.correctIndices.map((item) => Number(item)).filter((n) => Number.isInteger(n) && n >= 0) : [];
  const multi = Boolean(req.body?.multi);
  const sectionId = req.body?.sectionId == null || req.body?.sectionId === '' ? null : Number(req.body.sectionId);

  if (!Number.isInteger(qNum) || qNum < 1) return res.status(400).json({ error: 'invalid_question_number' });
  if (!stem) return res.status(400).json({ error: 'stem_required' });
  if (opts.length < 2) return res.status(400).json({ error: 'at_least_two_options_required' });
  if (!correctIndices.length) return res.status(400).json({ error: 'correct_indices_required' });
  if (!multi && correctIndices.length !== 1) return res.status(400).json({ error: 'single_select_requires_exactly_one_correct_option' });
  if (correctIndices.some((idx) => idx >= opts.length)) return res.status(400).json({ error: 'correct_index_out_of_range' });

  try {
    await withDb(async (conn) => {
      if (sectionId != null) {
        const sections = await execQuery(conn, 'SELECT SECTION_ID FROM QUESTION_SECTIONS WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?', [sectionId, setId]);
        if (!sections.length) throw new Error('section_not_found');
      }
      if (questionId != null) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SET_QUESTIONS
              SET QUESTION_INDEX = ?,
                  STEM = ?,
                  NOTE = ?,
                  OPTS_JSON = ?,
                  ANSWER_JSON = ?,
                  MULTI = ?,
                  SECTION_ID = ?,
                  UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE QUESTION_ID = ?
              AND QUESTION_SET_ID = ?`,
          [qNum, stem, note || null, JSON.stringify(opts), JSON.stringify(correctIndices), multi ? 1 : 0, sectionId, questionId, setId]
        );
      } else {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SET_QUESTIONS
            (QUESTION_SET_ID, SECTION_ID, QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [setId, sectionId, qNum, stem, note || null, JSON.stringify(opts), JSON.stringify(correctIndices), multi ? 1 : 0]
        );
      }
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: questionId != null ? 'admin_question_updated' : 'admin_question_created',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: setId, questionId, qNum }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_save_failed', message: err.message });
  }
});

app.delete('/api/admin/question-sets/:setId/questions/:questionId', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const setId = Number(req.params.setId);
  const questionId = Number(req.params.questionId);
  if (!Number.isInteger(setId) || !Number.isInteger(questionId)) return res.status(400).json({ error: 'invalid_identifier' });

  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'DELETE FROM QUESTION_SET_QUESTIONS WHERE QUESTION_ID = ? AND QUESTION_SET_ID = ?', [questionId, setId]);
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: 'admin_question_deleted',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: setId, questionId }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_delete_failed', message: err.message });
  }
});

app.get('/api/admin/question-sets/:setId/sections', requireAdmin, async (req, res) => {
  const setId = Number(req.params.setId);
  if (!Number.isInteger(setId)) return res.status(400).json({ error: 'invalid_question_set_id' });

  try {
    const sections = await withDb(async (conn) => {
      const rows = await execQuery(
        conn,
        `SELECT s.SECTION_ID, s.QUESTION_SET_ID, s.NAME, s.DESCRIPTION, s.DISPLAY_ORDER, s.DRAW_COUNT,
                s.CREATED_AT, s.UPDATED_AT, COUNT(q.QUESTION_ID) AS QUESTION_COUNT
           FROM QUESTION_SECTIONS s
           LEFT JOIN QUESTION_SET_QUESTIONS q ON q.SECTION_ID = s.SECTION_ID
          WHERE s.QUESTION_SET_ID = ?
          GROUP BY s.SECTION_ID, s.QUESTION_SET_ID, s.NAME, s.DESCRIPTION, s.DISPLAY_ORDER, s.DRAW_COUNT, s.CREATED_AT, s.UPDATED_AT
          ORDER BY s.DISPLAY_ORDER ASC, s.SECTION_ID ASC`,
        [setId]
      );
      return rows.map((row) => ({
        id: Number(row.SECTION_ID),
        questionSetId: Number(row.QUESTION_SET_ID),
        name: String(row.NAME || ''),
        description: row.DESCRIPTION || '',
        displayOrder: Number(row.DISPLAY_ORDER || 0),
        drawCount: row.DRAW_COUNT == null ? null : Number(row.DRAW_COUNT),
        questionCount: Number(row.QUESTION_COUNT || 0)
      }));
    });
    res.json({ sections });
  } catch (err) {
    res.status(500).json({ error: 'admin_sections_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/:setId/sections', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const setId = Number(req.params.setId);
  if (!Number.isInteger(setId)) return res.status(400).json({ error: 'invalid_question_set_id' });

  const sectionId = req.body?.id == null || req.body?.id === '' ? null : Number(req.body.id);
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  const displayOrder = Number(req.body?.displayOrder) || 0;
  const drawCountRaw = req.body?.drawCount;
  const drawCount = drawCountRaw == null || drawCountRaw === '' ? null : Math.max(1, Number(drawCountRaw));
  if (!name) return res.status(400).json({ error: 'name_required' });

  try {
    await withDb(async (conn) => {
      if (sectionId != null) {
        await execQuery(
          conn,
          `UPDATE QUESTION_SECTIONS
              SET NAME = ?, DESCRIPTION = ?, DISPLAY_ORDER = ?, DRAW_COUNT = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP
            WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?`,
          [name, description || null, displayOrder, drawCount, sectionId, setId]
        );
      } else {
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SECTIONS
            (QUESTION_SET_ID, NAME, DESCRIPTION, DISPLAY_ORDER, DRAW_COUNT, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [setId, name, description || null, displayOrder, drawCount]
        );
      }
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: sectionId != null ? 'admin_section_updated' : 'admin_section_created',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: setId, sectionId, name }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_section_save_failed', message: err.message });
  }
});

app.delete('/api/admin/question-sets/:setId/sections/:sectionId', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const setId = Number(req.params.setId);
  const sectionId = Number(req.params.sectionId);
  if (!Number.isInteger(setId) || !Number.isInteger(sectionId)) return res.status(400).json({ error: 'invalid_identifier' });

  try {
    await withDb(async (conn) => {
      await execQuery(conn, 'UPDATE QUESTION_SET_QUESTIONS SET SECTION_ID = NULL, UPDATED_AT = CURRENT_UTCTIMESTAMP WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?', [sectionId, setId]);
      await execQuery(conn, 'DELETE FROM QUESTION_SECTIONS WHERE SECTION_ID = ? AND QUESTION_SET_ID = ?', [sectionId, setId]);
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: 'admin_section_deleted',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: setId, sectionId }
      });
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'admin_section_delete_failed', message: err.message });
  }
});

app.post('/api/admin/question-sets/upload', requireAdmin, requireAdminRole('admin'), async (req, res) => {
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  const questions = Array.isArray(req.body?.questions) ? req.body.questions : [];
  if (!name) return res.status(400).json({ error: 'name_required' });
  if (!questions.length) return res.status(400).json({ error: 'questions_required' });

  try {
    const result = await withDb(async (conn) => {
      await execQuery(
        conn,
        `INSERT INTO QUESTION_SETS
          (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED, NUM_QUESTIONS, CREATED_AT, UPDATED_AT)
         VALUES (?, ?, FALSE, 45, 80, TRUE, NULL, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
        [name, description || null]
      );
      const createdRows = await execQuery(
        conn,
        `SELECT QUESTION_SET_ID
           FROM QUESTION_SETS
          WHERE NAME = ?
          ORDER BY QUESTION_SET_ID DESC
          LIMIT 1`,
        [name]
      );
      const setId = Number(createdRows[0].QUESTION_SET_ID);

      for (const entry of questions) {
        const qNum = Number(entry.qNum);
        const stem = String(entry.stem || '').trim();
        const note = String(entry.note || '').trim();
        const opts = Array.isArray(entry.opts) ? entry.opts.map((item) => String(item || '').trim()).filter(Boolean) : [];
        const correctIndices = Array.isArray(entry.correctIndices) ? entry.correctIndices.map((item) => Number(item)).filter((n) => Number.isInteger(n) && n >= 0) : [];
        const multi = Boolean(entry.multi);
        if (!Number.isInteger(qNum) || !stem || opts.length < 2 || !correctIndices.length) {
          throw new Error(`invalid_question_payload_${qNum || 'unknown'}`);
        }
        await execQuery(
          conn,
          `INSERT INTO QUESTION_SET_QUESTIONS
            (QUESTION_SET_ID, QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [setId, qNum, stem, note || null, JSON.stringify(opts), JSON.stringify(correctIndices), multi ? 1 : 0]
        );
      }
      clearQuestionSetCache(setId);
      await writeAdminAudit(conn, {
        action: 'admin_question_set_uploaded',
        actor: 'admin',
        clientIp: getClientIp(req),
        details: { questionSetId: setId, name, count: questions.length }
      });
      return { setId, count: questions.length };
    });
    res.json({ ok: true, questionSetId: result.setId, count: result.count });
  } catch (err) {
    res.status(500).json({ error: 'admin_question_set_upload_failed', message: err.message });
  }
});

app.get('/api/admin/export.csv', requireAdmin, async (_req, res) => {
  try {
    const rows = await withDb(async (conn) => {
      const hasNotes = await hasNotesColumn(conn);
      const hasDeletedAt = await hasDeletedAtColumn(conn);
      return execQuery(
        conn,
        `SELECT c.ACCESS_CODE, c.LABEL, ${hasNotes ? 'c.NOTES,' : `'' AS NOTES,`} c.STATUS,
                qs.NAME AS QUESTION_SET_NAME,
                r.SCORE, r.PCT, r.PASS, r.DURATION_SECS, r.TAB_SWITCHES, r.INCIDENT_COUNT, r.SUBMITTED_AT, r.RESULT_JSON
           FROM ACCESS_CODES c
           LEFT JOIN QUESTION_SETS qs ON qs.QUESTION_SET_ID = c.QUESTION_SET_ID
           LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
          ${hasDeletedAt ? 'WHERE c.DELETED_AT IS NULL' : ''}
          ORDER BY c.ACCESS_CODE ASC`
      );
    });

    const lines = ['Code,Seat,Notes,QuestionSet,Mode,Status,Score,Pct,Result,Duration,TabSwitches,Incidents,SubmittedAt'];
    for (const r of rows) {
      const parsedResult = parseJsonOrNull(r.RESULT_JSON);
      const countsTowardResults = parsedResult?.countsTowardResults !== false;
      const mode = parsedResult?.examMode === 'PRACTICE' ? 'Practice' : 'Graded';
      const resultLabel = !countsTowardResults || r.PASS === null || r.PASS === undefined ? '' : (r.PASS ? 'PASS' : 'FAIL');
      const duration = r.DURATION_SECS == null ? '' : `${Math.floor(r.DURATION_SECS / 60)}m ${String(r.DURATION_SECS % 60).padStart(2, '0')}s`;
      lines.push([
        toCsvCell(r.ACCESS_CODE),
        toCsvCell(r.LABEL || ''),
        toCsvCell(r.NOTES || ''),
        toCsvCell(r.QUESTION_SET_NAME || ''),
        toCsvCell(mode),
        toCsvCell(r.STATUS || ''),
        toCsvCell(countsTowardResults ? (r.SCORE ?? '') : ''),
        toCsvCell(countsTowardResults ? (r.PCT == null ? '' : `${r.PCT}%`) : ''),
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
