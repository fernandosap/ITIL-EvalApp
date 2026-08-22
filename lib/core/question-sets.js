'use strict';

const { withConnection, exec, currentIdentity } = require('./db.js');

function asBool(v, fallback = false) { return v == null ? fallback : Boolean(v); }
function json(value, fallback = []) { try { return JSON.parse(value); } catch (_e) { return fallback; } }

async function audit(conn, action, req, details) {
  try {
    await exec(conn,
      `INSERT INTO ADMIN_AUDIT_LOG
        (ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT)
       VALUES (?, NULL, ?, ?, ?, CURRENT_UTCTIMESTAMP)`,
      [action, JSON.stringify(details || {}), String(req.adminRole || 'admin'), String(req.ip || req.socket?.remoteAddress || 'unknown')]);
  } catch (_e) { /* audit remains best-effort */ }
}

async function insertSet(conn, values) {
  await exec(conn,
    `INSERT INTO QUESTION_SETS
      (NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT, PROCTOR_ENABLED,
       EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS, NUM_QUESTIONS,
       VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID,
       IMPORT_SOURCE, CREATED_AT, UPDATED_AT)
     VALUES (?, ?, FALSE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
    [values.name, values.description || null, values.durationMinutes || 45, values.passPct || 80,
      values.proctorEnabled === false ? 0 : 1, values.examMode || 'GRADED', values.showCorrectAnswers ? 1 : 0,
      values.countsTowardResults === false ? 0 : 1, values.numQuestions ?? null, values.versionGroupId ?? null,
      values.versionNumber || 1, values.lifecycleStatus || 'DRAFT', values.parentQuestionSetId ?? null,
      values.importSource || 'manual']);
  const id = await currentIdentity(conn);
  if (values.versionGroupId == null) {
    await exec(conn, 'UPDATE QUESTION_SETS SET VERSION_GROUP_ID = QUESTION_SET_ID WHERE QUESTION_SET_ID = ?', [id]);
  }
  return id;
}

async function setSummary(conn, id) {
  const rows = await exec(conn,
    `SELECT QUESTION_SET_ID, NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES, PASS_PCT,
            PROCTOR_ENABLED, EXAM_MODE, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS,
            NUM_QUESTIONS, VERSION_GROUP_ID, VERSION_NUMBER, LIFECYCLE_STATUS,
            PARENT_QUESTION_SET_ID, IMPORT_SOURCE, CREATED_AT, UPDATED_AT
       FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?`, [id]);
  if (!rows.length) return null;
  const r = rows[0];
  return {
    id: Number(r.QUESTION_SET_ID), name: r.NAME, description: r.DESCRIPTION || '', isActive: Boolean(r.IS_ACTIVE),
    durationMinutes: Number(r.DURATION_MINUTES || 45), passPct: Number(r.PASS_PCT || 80),
    proctorEnabled: asBool(r.PROCTOR_ENABLED, true), examMode: r.EXAM_MODE || 'GRADED',
    showCorrectAnswers: Boolean(r.SHOW_CORRECT_ANSWERS), countsTowardResults: asBool(r.COUNTS_TOWARD_RESULTS, true),
    numQuestions: r.NUM_QUESTIONS == null ? null : Number(r.NUM_QUESTIONS),
    versionGroupId: Number(r.VERSION_GROUP_ID || r.QUESTION_SET_ID), versionNumber: Number(r.VERSION_NUMBER || 1),
    lifecycleStatus: r.LIFECYCLE_STATUS || 'DRAFT', parentQuestionSetId: r.PARENT_QUESTION_SET_ID == null ? null : Number(r.PARENT_QUESTION_SET_ID),
    importSource: r.IMPORT_SOURCE || '', createdAt: r.CREATED_AT ? new Date(r.CREATED_AT).toISOString() : null,
    updatedAt: r.UPDATED_AT ? new Date(r.UPDATED_AT).toISOString() : null
  };
}

async function createHandler(req, res) {
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  if (!name) return res.status(400).json({ error: 'name_required' });
  try {
    const questionSet = await withConnection(async (conn) => {
      const id = await insertSet(conn, { name, description, importSource: 'manual' });
      await audit(conn, 'admin_question_set_created', req, { questionSetId: id, name });
      return setSummary(conn, id);
    }, { transaction: true });
    return res.json({ ok: true, questionSet });
  } catch (err) {
    return res.status(500).json({ error: 'admin_question_set_create_failed', message: err.message });
  }
}

function normalizeUpload(questions) {
  const normalized = [];
  const errors = [];
  const seen = new Set();
  if (!Array.isArray(questions) || !questions.length) errors.push('At least one question is required.');
  if (Array.isArray(questions) && questions.length > 500) errors.push('Question set upload limit is 500 questions per file.');
  for (const [idx, raw] of (Array.isArray(questions) ? questions : []).entries()) {
    const qNum = Number(raw?.qNum);
    const stem = String(raw?.stem || '').trim();
    const note = String(raw?.note || '').trim();
    const opts = Array.isArray(raw?.opts) ? raw.opts.map((x) => String(x || '').trim()).filter(Boolean) : [];
    const correctIndices = Array.isArray(raw?.correctIndices) ? raw.correctIndices.map(Number).filter(Number.isInteger) : [];
    const multi = Boolean(raw?.multi);
    const label = `Row ${idx + 2}`;
    if (!Number.isInteger(qNum) || qNum < 1) errors.push(`${label}: q_num must be a positive integer.`);
    if (seen.has(qNum)) errors.push(`${label}: duplicate q_num ${qNum}.`); else seen.add(qNum);
    if (!stem || stem.length > 2000) errors.push(`${label}: invalid stem.`);
    if (opts.length < 2 || opts.length > 6) errors.push(`${label}: options must contain 2-6 values.`);
    if (!correctIndices.length || correctIndices.some((i) => i < 0 || i >= opts.length)) errors.push(`${label}: invalid correct_indices.`);
    if (!multi && correctIndices.length !== 1) errors.push(`${label}: single-select questions need exactly one answer.`);
    normalized.push({ qNum, stem, note, opts, correctIndices, multi });
  }
  return { ok: errors.length === 0, errors, normalized };
}

async function uploadHandler(req, res) {
  const name = String(req.body?.name || '').trim();
  const description = String(req.body?.description || '').trim();
  if (!name) return res.status(400).json({ error: 'name_required' });
  const validation = normalizeUpload(req.body?.questions);
  if (!validation.ok) return res.status(400).json({ error: 'invalid_question_upload', errors: validation.errors, warnings: [] });
  try {
    const result = await withConnection(async (conn) => {
      const id = await insertSet(conn, { name, description, importSource: 'csv_upload' });
      for (const q of validation.normalized) {
        await exec(conn,
          `INSERT INTO QUESTION_SET_QUESTIONS
            (QUESTION_SET_ID, QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [id, q.qNum, q.stem, q.note || null, JSON.stringify(q.opts), JSON.stringify(q.correctIndices), q.multi ? 1 : 0]);
      }
      await audit(conn, 'admin_question_set_uploaded', req, { questionSetId: id, name, count: validation.normalized.length });
      return { setId: id, count: validation.normalized.length };
    }, { transaction: true });
    return res.json({ ok: true, questionSetId: result.setId, count: result.count });
  } catch (err) {
    return res.status(500).json({ error: 'admin_question_set_upload_failed', message: err.message });
  }
}

async function cloneHandler(req, res) {
  const sourceId = Number(req.params?.id);
  if (!Number.isInteger(sourceId)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    const questionSet = await withConnection(async (conn) => {
      const meta = await exec(conn, 'SELECT * FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [sourceId]);
      if (!meta.length) throw new Error('question_set_not_found');
      const src = meta[0];
      const versionRows = await exec(conn, 'SELECT COALESCE(MAX(VERSION_NUMBER), 0) AS V FROM QUESTION_SETS WHERE VERSION_GROUP_ID = ?', [src.VERSION_GROUP_ID || sourceId]);
      const nextVersion = Number(versionRows?.[0]?.V || 0) + 1;
      const newId = await insertSet(conn, {
        name: String(req.body?.name || `${src.NAME} v${nextVersion}`).trim(), description: src.DESCRIPTION || '',
        durationMinutes: Number(src.DURATION_MINUTES || 45), passPct: Number(src.PASS_PCT || 80), proctorEnabled: Boolean(src.PROCTOR_ENABLED),
        examMode: src.EXAM_MODE || 'GRADED', showCorrectAnswers: Boolean(src.SHOW_CORRECT_ANSWERS), countsTowardResults: Boolean(src.COUNTS_TOWARD_RESULTS),
        numQuestions: src.NUM_QUESTIONS == null ? null : Number(src.NUM_QUESTIONS), versionGroupId: Number(src.VERSION_GROUP_ID || sourceId),
        versionNumber: nextVersion, lifecycleStatus: 'DRAFT', parentQuestionSetId: sourceId, importSource: 'clone'
      });
      const sections = await exec(conn, 'SELECT * FROM QUESTION_SECTIONS WHERE QUESTION_SET_ID = ? ORDER BY DISPLAY_ORDER, SECTION_ID', [sourceId]);
      const sectionMap = new Map();
      for (const section of sections) {
        await exec(conn,
          `INSERT INTO QUESTION_SECTIONS (QUESTION_SET_ID, NAME, DESCRIPTION, DISPLAY_ORDER, DRAW_COUNT, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [newId, section.NAME, section.DESCRIPTION || null, Number(section.DISPLAY_ORDER || 0), section.DRAW_COUNT]);
        sectionMap.set(Number(section.SECTION_ID), await currentIdentity(conn));
      }
      const questions = await exec(conn, 'SELECT * FROM QUESTION_SET_QUESTIONS WHERE QUESTION_SET_ID = ? ORDER BY QUESTION_INDEX', [sourceId]);
      for (const q of questions) {
        await exec(conn,
          `INSERT INTO QUESTION_SET_QUESTIONS
            (QUESTION_SET_ID, SECTION_ID, QUESTION_INDEX, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, CREATED_AT, UPDATED_AT)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`,
          [newId, q.SECTION_ID == null ? null : sectionMap.get(Number(q.SECTION_ID)) || null, q.QUESTION_INDEX, q.STEM, q.NOTE || null,
            q.OPTS_JSON, q.ANSWER_JSON, q.MULTI ? 1 : 0]);
      }
      await audit(conn, 'admin_question_set_cloned', req, { sourceId, clonedId: newId });
      return setSummary(conn, newId);
    }, { transaction: true });
    return res.json({ ok: true, questionSet });
  } catch (err) {
    const status = err.message === 'question_set_not_found' ? 404 : 500;
    return res.status(status).json({ error: status === 404 ? err.message : 'admin_question_set_clone_failed', message: status === 500 ? err.message : undefined });
  }
}

async function readiness(conn, id) {
  const meta = await exec(conn, 'SELECT * FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
  if (!meta.length) return null;
  const questions = await exec(conn, 'SELECT QUESTION_INDEX, STEM, OPTS_JSON, ANSWER_JSON, MULTI, SECTION_ID FROM QUESTION_SET_QUESTIONS WHERE QUESTION_SET_ID = ? ORDER BY QUESTION_INDEX', [id]);
  const sections = await exec(conn, 'SELECT SECTION_ID, NAME, DRAW_COUNT FROM QUESTION_SECTIONS WHERE QUESTION_SET_ID = ? ORDER BY DISPLAY_ORDER, SECTION_ID', [id]);
  const blockers = [];
  const warnings = [];
  if (!questions.length) blockers.push('Question set has no questions.');
  const bySection = new Map();
  for (const q of questions) {
    const opts = json(q.OPTS_JSON, []);
    const answer = json(q.ANSWER_JSON, []);
    if (!String(q.STEM || '').trim()) blockers.push(`Question ${q.QUESTION_INDEX}: missing stem.`);
    if (!Array.isArray(opts) || opts.length < 2 || opts.length > 6) blockers.push(`Question ${q.QUESTION_INDEX}: invalid option count.`);
    if (!Array.isArray(answer) || !answer.length || answer.some((i) => !Number.isInteger(Number(i)) || Number(i) < 0 || Number(i) >= opts.length)) blockers.push(`Question ${q.QUESTION_INDEX}: invalid answer key.`);
    if (!Boolean(q.MULTI) && answer.length !== 1) blockers.push(`Question ${q.QUESTION_INDEX}: single-select must have one answer.`);
    const sid = q.SECTION_ID == null ? 'none' : String(q.SECTION_ID);
    bySection.set(sid, Number(bySection.get(sid) || 0) + 1);
  }
  for (const section of sections) {
    const count = Number(bySection.get(String(section.SECTION_ID)) || 0);
    const draw = section.DRAW_COUNT == null ? null : Number(section.DRAW_COUNT);
    if (draw != null && draw > count) blockers.push(`Section ${section.NAME}: draw count ${draw} exceeds ${count} available questions.`);
    if (!count) warnings.push(`Section ${section.NAME} has no questions.`);
  }
  const configured = meta[0].NUM_QUESTIONS == null ? null : Number(meta[0].NUM_QUESTIONS);
  if (configured != null && configured > questions.length) blockers.push(`Configured exam size ${configured} exceeds ${questions.length} available questions.`);
  return { id, ready: blockers.length === 0, blockers, warnings, questionCount: questions.length, sectionCount: sections.length };
}

async function readinessHandler(req, res) {
  const id = Number(req.params?.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    const result = await withConnection((conn) => readiness(conn, id));
    if (!result) return res.status(404).json({ error: 'question_set_not_found' });
    return res.json({ ok: true, ...result });
  } catch (err) {
    return res.status(500).json({ error: 'question_set_readiness_failed', message: err.message });
  }
}

async function activateHandler(req, res) {
  const id = Number(req.params?.id);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_question_set_id' });
  try {
    const result = await withConnection(async (conn) => {
      const check = await readiness(conn, id);
      if (!check) return { status: 404, body: { error: 'question_set_not_found' } };
      if (!check.ready) return { status: 409, body: { error: 'question_set_not_ready', readiness: check } };
      const meta = await exec(conn, 'SELECT VERSION_GROUP_ID FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [id]);
      const groupId = Number(meta[0].VERSION_GROUP_ID || id);
      await exec(conn,
        `UPDATE QUESTION_SETS
            SET IS_ACTIVE = CASE WHEN QUESTION_SET_ID = ? THEN TRUE ELSE FALSE END,
                LIFECYCLE_STATUS = CASE
                  WHEN QUESTION_SET_ID = ? THEN 'PUBLISHED'
                  WHEN VERSION_GROUP_ID = ? THEN 'ARCHIVED'
                  ELSE LIFECYCLE_STATUS END,
                UPDATED_AT = CURRENT_UTCTIMESTAMP
          WHERE IS_ACTIVE = TRUE OR QUESTION_SET_ID = ? OR VERSION_GROUP_ID = ?`, [id, id, groupId, id, groupId]);
      await audit(conn, 'admin_question_set_activated', req, { questionSetId: id, readiness: { questionCount: check.questionCount, sectionCount: check.sectionCount } });
      return { status: 200, body: { ok: true, readiness: check } };
    }, { transaction: true });
    return res.status(result.status).json(result.body);
  } catch (err) {
    return res.status(500).json({ error: 'admin_question_set_activate_failed', message: err.message });
  }
}

module.exports = { normalizeUpload, insertSet, readiness, createHandler, uploadHandler, cloneHandler, readinessHandler, activateHandler };
