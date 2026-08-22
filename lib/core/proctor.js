'use strict';

const crypto = require('node:crypto');
const { withConnection, exec, ensureRuntimeTables } = require('./db.js');

function normalizeIncident(item) {
  if (!item || typeof item !== 'object') return null;
  const type = String(item.type || '').trim().slice(0, 80);
  if (!type) return null;
  return {
    type,
    detail: String(item.detail || '').trim().slice(0, 1000),
    clientTime: String(item.time || '').trim().slice(0, 64)
  };
}

function incidentHash(code, incident) {
  return crypto.createHash('sha256')
    .update(JSON.stringify([String(code || '').toUpperCase(), incident.type, incident.detail, incident.clientTime]))
    .digest('hex');
}

async function appendIncidents(code, incidents) {
  const safeCode = String(code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(safeCode)) return 0;
  const normalized = (Array.isArray(incidents) ? incidents : []).map(normalizeIncident).filter(Boolean).slice(0, 250);
  if (!normalized.length) return 0;
  return withConnection(async (conn) => {
    await ensureRuntimeTables(conn);
    let added = 0;
    for (const incident of normalized) {
      try {
        await exec(conn,
          `INSERT INTO EXAM_PROCTOR_INCIDENTS
            (ACCESS_CODE, EVENT_HASH, EVENT_TYPE, DETAIL, CLIENT_TIME, SERVER_TIME)
           VALUES (?, ?, ?, ?, ?, CURRENT_UTCTIMESTAMP)`,
          [safeCode, incidentHash(safeCode, incident), incident.type, incident.detail || null, incident.clientTime || null]);
        added += 1;
      } catch (err) {
        const message = String(err?.message || err);
        if (!/unique|duplicate/i.test(message)) throw err;
      }
    }
    return added;
  });
}

async function getIncidentTimeline(code) {
  const safeCode = String(code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(safeCode)) return [];
  return withConnection(async (conn) => {
    await ensureRuntimeTables(conn);
    const rows = await exec(conn,
      `SELECT INCIDENT_ID, EVENT_TYPE, DETAIL, CLIENT_TIME, SERVER_TIME
         FROM EXAM_PROCTOR_INCIDENTS
        WHERE ACCESS_CODE = ?
        ORDER BY SERVER_TIME ASC, INCIDENT_ID ASC`, [safeCode]);
    return rows.map((row) => ({
      id: Number(row.INCIDENT_ID),
      type: String(row.EVENT_TYPE || ''),
      detail: row.DETAIL == null ? '' : String(row.DETAIL),
      clientTime: row.CLIENT_TIME == null ? null : String(row.CLIENT_TIME),
      serverTime: row.SERVER_TIME ? new Date(row.SERVER_TIME).toISOString() : null
    }));
  });
}

function progressIncidentMiddleware(req, _res, next) {
  const code = String(req.examSession?.code || req.body?.code || '').trim().toUpperCase();
  appendIncidents(code, req.body?.incidents).catch(() => {});
  next();
}

async function timelineHandler(req, res) {
  const code = String(req.params?.code || '').trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(code)) return res.status(400).json({ error: 'invalid_code' });
  try {
    const incidents = await getIncidentTimeline(code);
    return res.json({ ok: true, code, incidents });
  } catch (err) {
    return res.status(500).json({ error: 'proctor_timeline_failed', message: err.message });
  }
}

async function liveSessionsHandler(_req, res) {
  try {
    const sessions = await withConnection(async (conn) => {
      await ensureRuntimeTables(conn);
      const rows = await exec(conn,
        `SELECT s.ACCESS_CODE, c.LABEL, c.QUESTION_SET_ID,
                s.ELAPSED_MS, s.TAB_SWITCHES, s.UPDATED_AT,
                (SELECT COUNT(*) FROM EXAM_PROCTOR_INCIDENTS i WHERE i.ACCESS_CODE = s.ACCESS_CODE) AS INCIDENT_COUNT
           FROM EXAM_SESSIONS s
           LEFT JOIN ACCESS_CODES c ON c.ACCESS_CODE = s.ACCESS_CODE
          ORDER BY s.UPDATED_AT DESC`);
      return rows.map((row) => ({
        code: row.ACCESS_CODE,
        label: row.LABEL || '',
        questionSetId: row.QUESTION_SET_ID == null ? null : Number(row.QUESTION_SET_ID),
        elapsedMs: Number(row.ELAPSED_MS || 0),
        tabSwitches: Number(row.TAB_SWITCHES || 0),
        incidentCount: Number(row.INCIDENT_COUNT || 0),
        lastSaveAt: row.UPDATED_AT ? new Date(row.UPDATED_AT).toISOString() : null
      }));
    });
    return res.json({ ok: true, sessions, count: sessions.length });
  } catch (err) {
    return res.status(500).json({ error: 'live_sessions_failed', message: err.message });
  }
}

module.exports = {
  normalizeIncident,
  incidentHash,
  appendIncidents,
  getIncidentTimeline,
  progressIncidentMiddleware,
  timelineHandler,
  liveSessionsHandler
};
