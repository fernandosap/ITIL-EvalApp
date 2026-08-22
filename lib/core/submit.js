'use strict';

const { gradeExamFromSession } = require('../../shared/scoring.js');
const { withConnection, exec, tableExists } = require('./db.js');

function parseJson(value) {
  try { return value ? JSON.parse(value) : null; } catch (_e) { return null; }
}

function sanitizeCandidateResult(result) {
  if (!result || typeof result !== 'object') return result;
  const isPractice = result.examMode === 'PRACTICE' || result.isPractice === true;
  const showCorrectAnswers = isPractice && result.showCorrectAnswers === true;
  const safe = { ...result, isPractice, showCorrectAnswers };
  if (!showCorrectAnswers) delete safe.questionResults;
  return safe;
}

async function readExisting(conn, code) {
  const rows = await exec(conn, 'SELECT RESULT_JSON FROM EXAM_RESULTS WHERE ACCESS_CODE = ?', [code]);
  return rows.length ? parseJson(rows[0].RESULT_JSON) : null;
}

async function serverIncidentRows(conn, code) {
  if (!(await tableExists(conn, 'EXAM_PROCTOR_INCIDENTS'))) return [];
  const rows = await exec(conn,
    `SELECT EVENT_TYPE, DETAIL, CLIENT_TIME, SERVER_TIME
       FROM EXAM_PROCTOR_INCIDENTS
      WHERE ACCESS_CODE = ?
      ORDER BY SERVER_TIME ASC, INCIDENT_ID ASC`, [code]);
  return rows.map((row) => ({
    type: String(row.EVENT_TYPE || ''),
    detail: row.DETAIL == null ? '' : String(row.DETAIL),
    time: row.CLIENT_TIME || (row.SERVER_TIME ? new Date(row.SERVER_TIME).toISOString() : null),
    serverTime: row.SERVER_TIME ? new Date(row.SERVER_TIME).toISOString() : null
  }));
}

function resultRecord(req) {
  const session = req.examSession;
  const answers = Array.isArray(req.body?.answers) ? req.body.answers : [];
  const graded = gradeExamFromSession(session, answers);
  const durationSecs = Math.max(10, Math.min(
    Number(req.body?.durationSecs) || 0,
    Number(session.durationSecs || 2700) + 300
  ));
  return {
    code: session.code,
    questionSetId: session.questionSetId,
    questionSetName: session.questionSetName,
    examMode: session.examMode || 'GRADED',
    isPractice: session.examMode === 'PRACTICE',
    showCorrectAnswers: session.showCorrectAnswers === true,
    countsTowardResults: session.countsTowardResults !== false,
    score: graded.score,
    total: graded.total,
    pct: graded.pct,
    pass: graded.pass,
    autoSubmit: Boolean(req.body?.autoSubmit),
    durationSecs,
    tabSwitches: Number(req.body?.tabSwitches) || 0,
    incidents: Array.isArray(req.body?.incidents) ? req.body.incidents : [],
    incidentCount: Array.isArray(req.body?.incidents) ? req.body.incidents.length : 0,
    questionResults: graded.questionResults,
    sectionResults: graded.sectionResults,
    submittedAt: new Date().toISOString()
  };
}

async function writeImmutableResult(conn, record) {
  const counts = record.countsTowardResults !== false;
  const score = counts ? record.score : null;
  const pct = counts ? record.pct : null;
  const pass = counts ? (record.pass ? 1 : 0) : null;
  await exec(conn,
    `INSERT INTO EXAM_RESULTS
      (ACCESS_CODE, SCORE, TOTAL, PCT, PASS, AUTO_SUBMIT, DURATION_SECS, TAB_SWITCHES, INCIDENT_COUNT, RESULT_JSON, SUBMITTED_AT)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP)`,
    [record.code, score, record.total ?? 0, pct, pass, record.autoSubmit ? 1 : 0,
      record.durationSecs ?? 0, record.tabSwitches ?? 0, record.incidentCount ?? 0, JSON.stringify(record)]);
  await exec(conn,
    `UPDATE ACCESS_CODES
        SET STATUS = 'completed', SCORE = ?, PCT = ?, PASS = ?, UPDATED_AT = CURRENT_UTCTIMESTAMP
      WHERE ACCESS_CODE = ?`, [score, pct, pass, record.code]);
  await exec(conn, 'DELETE FROM EXAM_SESSIONS WHERE ACCESS_CODE = ?', [record.code]);
}

async function submitHandler(req, res) {
  const session = req.examSession;
  const code = String(req.body?.code || '').trim().toUpperCase();
  if (!session || code !== String(session.code || '').toUpperCase()) {
    return res.status(403).json({ error: 'code_mismatch' });
  }

  try {
    const outcome = await withConnection(async (conn) => {
      const existing = await readExisting(conn, code);
      if (existing) return { existing: true, result: existing };

      const record = resultRecord(req);
      const serverIncidents = await serverIncidentRows(conn, code);
      if (serverIncidents.length) {
        record.incidents = serverIncidents;
        record.incidentCount = serverIncidents.length;
        record.tabSwitches = Math.max(
          record.tabSwitches,
          serverIncidents.filter((item) => item.type === 'tab_switch').length
        );
      }
      await writeImmutableResult(conn, record);
      return { existing: false, result: record };
    }, { transaction: true });
    return res.json({ ok: true, idempotentReplay: outcome.existing, result: sanitizeCandidateResult(outcome.result) });
  } catch (err) {
    // Cross-instance race: if another instance inserted first, the PK wins.
    // Re-read the durable row and return it instead of recalculating/overwriting.
    try {
      const existing = await withConnection((conn) => readExisting(conn, code));
      if (existing) return res.json({ ok: true, idempotentReplay: true, result: sanitizeCandidateResult(existing) });
    } catch (_e) { /* fall through to original error */ }
    return res.status(500).json({ error: 'submit_failed', message: err.message });
  }
}

module.exports = { parseJson, sanitizeCandidateResult, resultRecord, submitHandler };
