'use strict';

const { withConnection, exec } = require('./db.js');

function normalizeTag(value) {
  return String(value || '').trim().replace(/^W\//, '').replace(/^"|"$/g, '');
}

function timestampVersion(value) {
  if (!value) return null;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? String(ms) : null;
}

async function currentVersion(id) {
  return withConnection(async (conn) => {
    const rows = await exec(conn, 'SELECT UPDATED_AT FROM QUESTION_SETS WHERE QUESTION_SET_ID = ?', [Number(id)]);
    if (!rows.length) return null;
    return timestampVersion(rows[0].UPDATED_AT);
  });
}

function quoted(version) { return `"${String(version)}"`; }

async function exposeVersion(req, res, next) {
  const id = Number(req.params?.id);
  if (!Number.isInteger(id)) return next();
  try {
    const version = await currentVersion(id);
    if (version) {
      res.setHeader('ETag', quoted(version));
      res.setHeader('X-Resource-Version', version);
    }
  } catch (_e) { /* metadata should not break reads */ }
  return next();
}

async function requireCurrentVersion(req, res, next) {
  const id = Number(req.params?.id);
  if (!Number.isInteger(id)) return next();
  const supplied = normalizeTag(req.headers?.['if-match'] || req.body?.expectedVersion || req.body?.expectedUpdatedAt);
  if (!supplied) {
    return res.status(428).json({
      error: 'precondition_required',
      message: 'Reload this question set before saving so the latest version can be verified.'
    });
  }
  try {
    const current = await currentVersion(id);
    if (!current) return res.status(404).json({ error: 'question_set_not_found' });
    if (supplied !== current) {
      res.setHeader('ETag', quoted(current));
      res.setHeader('X-Resource-Version', current);
      return res.status(412).json({
        error: 'stale_question_set_version',
        message: 'This question set was changed by another administrator. Reload before saving.',
        currentVersion: current
      });
    }
    req.questionSetVersion = current;
    return next();
  } catch (err) {
    return res.status(503).json({ error: 'question_set_version_check_failed', message: err.message });
  }
}

module.exports = { normalizeTag, timestampVersion, currentVersion, exposeVersion, requireCurrentVersion };
