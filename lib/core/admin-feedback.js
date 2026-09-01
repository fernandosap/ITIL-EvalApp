'use strict';

const { withConnection, exec, columnExists } = require('./db.js');
const { normalizeExamTitle, EXAM_MODE } = require('../../shared/constants.js');
const { toCsvCell } = require('../responses.js');
const audit = require('../audit.js');

function parseJson(value) {
  if (value == null || value === '') return null;
  try { return typeof value === 'string' ? JSON.parse(value) : JSON.parse(String(value)); }
  catch (_e) { return null; }
}

function seatNumber(label) {
  const match = String(label || '').match(/(\d+)/);
  return match ? Number(match[1]) : null;
}

function normalizeCodes(value) {
  const list = Array.isArray(value) ? value : [value];
  return [...new Set(list.map((v) => String(v || '').trim().toUpperCase()).filter((v) => /^[A-Z0-9]{6}$/.test(v)))].slice(0, 500);
}

function normalizedName(value) {
  const raw = String(value || '').trim();
  return raw ? normalizeExamTitle(raw) : '';
}

function resolveAttemptSnapshot(row) {
  const result = parseJson(row.RESULT_JSON) || {};
  const saved = parseJson(row.SESSION_JSON) || {};
  const session = saved.session || saved || {};
  const status = String(row.STATUS || '').toLowerCase();

  if (status === 'unused') {
    const assignedId = Number(row.ASSIGNED_QUESTION_SET_ID);
    const assignedVersion = Number(row.ASSIGNED_QUESTION_SET_VERSION);
    return {
      questionSetId: Number.isInteger(assignedId) && assignedId > 0 ? assignedId : null,
      questionSetName: normalizedName(row.ASSIGNED_QUESTION_SET_NAME),
      questionSetVersion: Number.isInteger(assignedVersion) && assignedVersion > 0 ? assignedVersion : null,
      examMode: String(row.ASSIGNED_EXAM_MODE || '').toUpperCase() || ''
    };
  }

  const source = status === 'completed' ? result : session;
  const snapshotId = Number(row.ATTEMPT_QUESTION_SET_ID);
  const sourceId = Number(source?.questionSetId ?? result?.questionSetId ?? session?.questionSetId);
  const id = Number.isInteger(snapshotId) && snapshotId > 0
    ? snapshotId
    : (Number.isInteger(sourceId) && sourceId > 0 ? sourceId : null);
  const snapshotVersion = Number(row.ATTEMPT_QUESTION_SET_VERSION);
  const sourceVersion = Number(source?.questionSetVersion ?? result?.questionSetVersion ?? session?.questionSetVersion);
  const version = Number.isInteger(snapshotVersion) && snapshotVersion > 0
    ? snapshotVersion
    : (Number.isInteger(sourceVersion) && sourceVersion > 0 ? sourceVersion : null);
  const rawName = row.ATTEMPT_QUESTION_SET_NAME || source?.questionSetName || result?.questionSetName || session?.questionSetName || '';
  const name = normalizedName(rawName);
  const mode = String(result?.examMode || session?.examMode || row.ASSIGNED_EXAM_MODE || '').toUpperCase();
  return {
    questionSetId: id,
    questionSetName: name || 'Legacy / unknown exam',
    questionSetVersion: version,
    examMode: mode
  };
}

async function loadAdminRows(conn) {
  const hasNotes = await columnExists(conn, 'ACCESS_CODES', 'NOTES');
  const hasDeleted = await columnExists(conn, 'ACCESS_CODES', 'DELETED_AT');
  const rows = await exec(conn,
    `SELECT c.ACCESS_CODE, c.LABEL, ${hasNotes ? 'c.NOTES' : `'' AS NOTES`}, c.STATUS,
            c.SCORE AS CODE_SCORE, c.PCT AS CODE_PCT, c.PASS AS CODE_PASS,
            c.QUESTION_SET_ID AS ASSIGNED_QUESTION_SET_ID,
            c.ATTEMPT_QUESTION_SET_ID, c.ATTEMPT_QUESTION_SET_NAME,
            c.ATTEMPT_QUESTION_SET_VERSION, c.ARCHIVED_AT,
            qs.NAME AS ASSIGNED_QUESTION_SET_NAME,
            qs.VERSION_NUMBER AS ASSIGNED_QUESTION_SET_VERSION,
            qs.EXAM_MODE AS ASSIGNED_EXAM_MODE,
            r.SCORE AS RESULT_SCORE, r.PCT AS RESULT_PCT, r.PASS AS RESULT_PASS,
            r.DURATION_SECS, r.TAB_SWITCHES, r.INCIDENT_COUNT, r.SUBMITTED_AT, r.RESULT_JSON,
            s.SESSION_JSON
       FROM ACCESS_CODES c
       LEFT JOIN QUESTION_SETS qs ON qs.QUESTION_SET_ID = c.QUESTION_SET_ID
       LEFT JOIN EXAM_RESULTS r ON r.ACCESS_CODE = c.ACCESS_CODE
       LEFT JOIN EXAM_SESSIONS s ON s.ACCESS_CODE = c.ACCESS_CODE
      ${hasDeleted ? 'WHERE c.DELETED_AT IS NULL' : ''}
      ORDER BY c.ACCESS_CODE ASC`);

  return rows.map((row) => {
    const historical = resolveAttemptSnapshot(row);
    const parsedResult = parseJson(row.RESULT_JSON) || {};
    return {
      code: row.ACCESS_CODE,
      label: row.LABEL || '',
      notes: row.NOTES || '',
      status: String(row.STATUS || '').toLowerCase(),
      score: row.RESULT_SCORE ?? row.CODE_SCORE ?? null,
      pct: row.RESULT_PCT ?? row.CODE_PCT ?? null,
      pass: row.RESULT_PASS ?? row.CODE_PASS ?? null,
      durationSecs: row.DURATION_SECS ?? null,
      tabSwitches: Number(row.TAB_SWITCHES || 0),
      incidentCount: Number(row.INCIDENT_COUNT || 0),
      submittedAt: row.SUBMITTED_AT ? new Date(row.SUBMITTED_AT).toISOString() : null,
      resultJson: row.RESULT_JSON || null,
      questionSetId: historical.questionSetId,
      questionSetName: historical.questionSetName,
      questionSetVersion: historical.questionSetVersion,
      examMode: historical.examMode || String(parsedResult.examMode || '').toUpperCase(),
      isPractice: String(historical.examMode || parsedResult.examMode || '').toUpperCase() === EXAM_MODE.PRACTICE,
      countsTowardResults: parsedResult.countsTowardResults !== false,
      archivedAt: row.ARCHIVED_AT ? new Date(row.ARCHIVED_AT).toISOString() : null,
      archived: Boolean(row.ARCHIVED_AT)
    };
  });
}

function mergeHistoricalRows(body, sourceRows) {
  if (!body || !Array.isArray(body.codes)) return body;
  const byCode = new Map(sourceRows.map((row) => [row.code, row]));
  body.codes = body.codes.map((row) => {
    const source = byCode.get(String(row.code || '').toUpperCase());
    if (!source) return row;
    return {
      ...row,
      questionSetId: source.questionSetId,
      questionSetName: source.questionSetName,
      questionSetVersion: source.questionSetVersion,
      examMode: source.examMode || row.examMode || '',
      isPractice: source.isPractice,
      countsTowardResults: source.countsTowardResults,
      archivedAt: source.archivedAt,
      archived: source.archived
    };
  });
  return body;
}

function enrichCodesResponse(_req, res, next) {
  const originalJson = res.json.bind(res);
  res.json = function historicalCodesJson(body) {
    if (!body || !Array.isArray(body.codes)) return originalJson(body);
    return withConnection((conn) => loadAdminRows(conn))
      .then((rows) => originalJson(mergeHistoricalRows(body, rows)))
      .catch(() => originalJson(body));
  };
  return next();
}

async function freezeAttemptSnapshot(code, questionSetId, questionSetName) {
  const normalizedCode = String(code || '').trim().toUpperCase();
  const setId = Number(questionSetId);
  if (!/^[A-Z0-9]{6}$/.test(normalizedCode) || !Number.isInteger(setId) || setId <= 0) return false;
  return withConnection(async (conn) => {
    const setRows = await exec(conn,
      'SELECT NAME, VERSION_NUMBER FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [setId]);
    const set = setRows[0] || {};
    const name = normalizedName(questionSetName || set.NAME) || null;
    const version = Number(set.VERSION_NUMBER);
    // A successful session/start resolves the authoritative set for this
    // attempt. Assign directly (rather than COALESCE) so a previously reset
    // code can never retain a stale snapshot if an earlier cleanup failed.
    await exec(conn,
      `UPDATE ACCESS_CODES
          SET ATTEMPT_QUESTION_SET_ID = ?,
              ATTEMPT_QUESTION_SET_NAME = ?,
              ATTEMPT_QUESTION_SET_VERSION = ?,
              ARCHIVED_AT = NULL,
              UPDATED_AT = CURRENT_UTCTIMESTAMP
        WHERE ACCESS_CODE = ?`,
      [setId, name, Number.isInteger(version) && version > 0 ? version : null, normalizedCode]);
    return true;
  });
}

function freezeAttemptOnStart(req, res, next) {
  const originalJson = res.json.bind(res);
  res.json = function snapshotStartJson(body) {
    if (!body?.ok || !body?.questionSet?.id) return originalJson(body);
    return freezeAttemptSnapshot(req.body?.code, body.questionSet.id, body.questionSet.name)
      .then(() => originalJson(body))
      .catch((err) => {
        res.status(500);
        return originalJson({ error: 'attempt_snapshot_failed', message: err.message });
      });
  };
  return next();
}

async function clearAttemptSnapshot(code) {
  const normalizedCode = String(code || '').trim().toUpperCase();
  if (!/^[A-Z0-9]{6}$/.test(normalizedCode)) return false;
  return withConnection(async (conn) => {
    await exec(conn,
      `UPDATE ACCESS_CODES
          SET ATTEMPT_QUESTION_SET_ID = NULL,
              ATTEMPT_QUESTION_SET_NAME = NULL,
              ATTEMPT_QUESTION_SET_VERSION = NULL,
              ARCHIVED_AT = NULL,
              UPDATED_AT = CURRENT_UTCTIMESTAMP
        WHERE ACCESS_CODE = ?`, [normalizedCode]);
    return true;
  });
}

function clearSnapshotOnReset(req, res, next) {
  const originalJson = res.json.bind(res);
  res.json = function resetSnapshotJson(body) {
    if (!body?.ok) return originalJson(body);
    return clearAttemptSnapshot(req.body?.code)
      .then(() => originalJson(body))
      .catch((err) => {
        res.status(500);
        return originalJson({ error: 'attempt_snapshot_reset_failed', message: err.message });
      });
  };
  return next();
}

function clientIp(req) {
  return String(req.headers?.['x-forwarded-for'] || '').split(',')[0].trim() || req.ip || req.socket?.remoteAddress || null;
}

async function setArchived(req, res, archived) {
  const codes = normalizeCodes(req.body?.codes);
  if (!codes.length) return res.status(400).json({ error: 'codes_required' });
  try {
    const result = await withConnection(async (conn) => {
      const placeholders = codes.map(() => '?').join(',');
      const rows = await exec(conn,
        `SELECT ACCESS_CODE, STATUS, ARCHIVED_AT FROM ACCESS_CODES WHERE ACCESS_CODE IN (${placeholders})`, codes);
      const byCode = new Map(rows.map((row) => [row.ACCESS_CODE, row]));
      const missing = codes.filter((code) => !byCode.has(code));
      if (missing.length) return { error: 'code_not_found', codes: missing };
      if (archived) {
        const invalid = rows.filter((row) => String(row.STATUS || '').toLowerCase() !== 'completed').map((row) => row.ACCESS_CODE);
        if (invalid.length) return { error: 'archive_requires_completed', codes: invalid };
      }
      await exec(conn,
        `UPDATE ACCESS_CODES SET ARCHIVED_AT = ${archived ? 'CURRENT_UTCTIMESTAMP' : 'NULL'}, UPDATED_AT = CURRENT_UTCTIMESTAMP
          WHERE ACCESS_CODE IN (${placeholders})`, codes);
      return { ok: true, updatedCount: rows.length };
    }, { transaction: true });
    if (result.error) return res.status(result.error === 'code_not_found' ? 404 : 409).json(result);
    for (const code of codes) {
      void audit.tryWriteAdminAudit({
        action: archived ? 'admin_result_archived' : 'admin_result_unarchived',
        targetCode: code,
        details: { archived },
        actor: req.adminRole || req.auth?.role || 'admin',
        clientIp: clientIp(req)
      });
    }
    return res.json({ ok: true, archived, updatedCount: result.updatedCount });
  } catch (err) {
    return res.status(500).json({ error: archived ? 'archive_failed' : 'unarchive_failed', message: err.message });
  }
}

function archiveHandler(req, res) { return setArchived(req, res, true); }
function unarchiveHandler(req, res) { return setArchived(req, res, false); }

function optionalNonNegative(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function normalizeFilters(query = {}) {
  const questionSetId = Number(query.questionSetId);
  return {
    questionSetId: Number.isInteger(questionSetId) && questionSetId > 0 ? questionSetId : null,
    status: ['unused', 'active', 'completed'].includes(String(query.status || '').toLowerCase()) ? String(query.status).toLowerCase() : '',
    mode: [EXAM_MODE.GRADED, EXAM_MODE.PRACTICE].includes(String(query.mode || '').toUpperCase()) ? String(query.mode).toUpperCase() : '',
    dateFrom: /^\d{4}-\d{2}-\d{2}$/.test(String(query.dateFrom || '')) ? String(query.dateFrom) : '',
    dateTo: /^\d{4}-\d{2}-\d{2}$/.test(String(query.dateTo || '')) ? String(query.dateTo) : '',
    seatFrom: optionalNonNegative(query.seatFrom),
    seatTo: optionalNonNegative(query.seatTo),
    archive: ['current', 'archived', 'all'].includes(String(query.archive || '').toLowerCase()) ? String(query.archive).toLowerCase() : 'current'
  };
}

function rowMatchesFilters(row, filters, activeSet = null) {
  if (filters.archive === 'current' && row.archived) return false;
  if (filters.archive === 'archived' && !row.archived) return false;
  const effectiveSetId = row.questionSetId != null
    ? Number(row.questionSetId)
    : (row.status === 'unused' ? Number(activeSet?.id) : null);
  if (filters.questionSetId && effectiveSetId !== filters.questionSetId) return false;
  if (filters.status && row.status !== filters.status) return false;
  const effectiveMode = String(row.examMode || (row.status === 'unused' ? activeSet?.examMode : '') || '').toUpperCase();
  if (filters.mode && effectiveMode !== filters.mode) return false;
  const seat = seatNumber(row.label);
  if (filters.seatFrom != null && (seat == null || seat < filters.seatFrom)) return false;
  if (filters.seatTo != null && (seat == null || seat > filters.seatTo)) return false;
  if (filters.dateFrom) {
    if (!row.submittedAt || row.submittedAt.slice(0, 10) < filters.dateFrom) return false;
  }
  if (filters.dateTo) {
    if (!row.submittedAt || row.submittedAt.slice(0, 10) > filters.dateTo) return false;
  }
  return true;
}

async function exportHandler(req, res) {
  try {
    const filters = normalizeFilters(req.query || {});
    const payload = await withConnection(async (conn) => {
      const rows = await loadAdminRows(conn);
      const activeRows = await exec(conn,
        "SELECT QUESTION_SET_ID, NAME, EXAM_MODE FROM QUESTION_SETS WHERE IS_ACTIVE = TRUE ORDER BY QUESTION_SET_ID DESC LIMIT 1");
      const active = activeRows[0] ? {
        id: Number(activeRows[0].QUESTION_SET_ID),
        name: normalizedName(activeRows[0].NAME),
        examMode: String(activeRows[0].EXAM_MODE || '').toUpperCase()
      } : null;
      return { rows, active };
    });
    const rows = payload.rows.filter((row) => rowMatchesFilters(row, filters, payload.active));
    const lines = ['Code,Seat,Notes,QuestionSet,Mode,Status,Score,Pct,Result,Duration,TabSwitches,Incidents,SubmittedAt'];
    for (const row of rows) {
      const effectiveMode = String(row.examMode || (row.status === 'unused' ? payload.active?.examMode : '') || '').toUpperCase();
      const mode = effectiveMode === EXAM_MODE.PRACTICE ? 'Practice' : 'Graded';
      const resultLabel = !row.countsTowardResults || row.pass == null ? '' : (Boolean(row.pass) ? 'PASS' : 'FAIL');
      const baseSetName = row.questionSetName || (row.status === 'unused' ? payload.active?.name : '') || '';
      const setLabel = row.questionSetVersion
        ? `${baseSetName || 'Legacy / unknown exam'} (v${row.questionSetVersion})`
        : baseSetName;
      lines.push([
        toCsvCell(row.code), toCsvCell(row.label), toCsvCell(row.notes), toCsvCell(setLabel), toCsvCell(mode),
        toCsvCell(row.status), toCsvCell(row.score ?? ''), toCsvCell(row.pct ?? ''), toCsvCell(resultLabel),
        toCsvCell(row.durationSecs ?? ''), toCsvCell(row.tabSwitches ?? 0), toCsvCell(row.incidentCount ?? 0),
        toCsvCell(row.submittedAt || '')
      ].join(','));
    }
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="Academy_Exam_App_Results.csv"');
    return res.send(lines.join('\n'));
  } catch (err) {
    return res.status(500).json({ error: 'admin_export_failed', message: err.message });
  }
}

module.exports = {
  parseJson,
  seatNumber,
  normalizeCodes,
  normalizedName,
  resolveAttemptSnapshot,
  loadAdminRows,
  mergeHistoricalRows,
  enrichCodesResponse,
  freezeAttemptSnapshot,
  freezeAttemptOnStart,
  clearAttemptSnapshot,
  clearSnapshotOnReset,
  archiveHandler,
  unarchiveHandler,
  optionalNonNegative,
  normalizeFilters,
  rowMatchesFilters,
  exportHandler
};
