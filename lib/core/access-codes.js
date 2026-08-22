'use strict';

const crypto = require('node:crypto');
const { withConnection, exec } = require('./db.js');

const CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

function generateCode() {
  let code = '';
  for (let i = 0; i < 6; i += 1) code += CHARS[crypto.randomInt(0, CHARS.length)];
  return code;
}

async function generateHandler(req, res) {
  const count = Math.min(Math.max(Number(req.body?.count) || 10, 1), 200);
  try {
    const added = await withConnection(async (conn) => {
      const columnRows = await exec(conn,
        `SELECT COUNT(*) AS CNT FROM SYS.TABLE_COLUMNS
          WHERE SCHEMA_NAME = CURRENT_SCHEMA AND TABLE_NAME = 'ACCESS_CODES' AND COLUMN_NAME = 'NOTES'`);
      const hasNotes = Number(columnRows?.[0]?.CNT || 0) > 0;
      const existing = await exec(conn, 'SELECT ACCESS_CODE FROM ACCESS_CODES');
      const used = new Set(existing.map((row) => String(row.ACCESS_CODE)));
      const activeRows = await exec(conn,
        `SELECT COUNT(*) AS CNT FROM ACCESS_CODES WHERE DELETED_AT IS NULL`)
        .catch(() => [{ CNT: existing.length }]);
      const seatBase = Number(activeRows?.[0]?.CNT || 0) + 1;
      const created = [];
      while (created.length < count) {
        const code = generateCode();
        if (!used.has(code)) { used.add(code); created.push(code); }
      }
      for (let i = 0; i < created.length; i += 1) {
        const label = `Seat ${String(seatBase + i).padStart(3, '0')}`;
        if (hasNotes) {
          await exec(conn,
            `INSERT INTO ACCESS_CODES (ACCESS_CODE, LABEL, NOTES, STATUS, CREATED_AT, UPDATED_AT)
             VALUES (?, ?, '', 'unused', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`, [created[i], label]);
        } else {
          await exec(conn,
            `INSERT INTO ACCESS_CODES (ACCESS_CODE, LABEL, STATUS, CREATED_AT, UPDATED_AT)
             VALUES (?, ?, 'unused', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP)`, [created[i], label]);
        }
      }
      try {
        await exec(conn,
          `INSERT INTO ADMIN_AUDIT_LOG (ACTION, TARGET_CODE, DETAILS_JSON, ACTOR, CLIENT_IP, CREATED_AT)
           VALUES ('admin_codes_generated', NULL, ?, ?, ?, CURRENT_UTCTIMESTAMP)`,
          [JSON.stringify({ count: created.length }), String(req.adminRole || 'admin'), String(req.ip || req.socket?.remoteAddress || 'unknown')]);
      } catch (_e) { /* best effort */ }
      return created.length;
    }, { transaction: true });
    return res.json({ ok: true, added });
  } catch (err) {
    return res.status(500).json({ error: 'admin_generate_failed', message: err.message });
  }
}

module.exports = { CHARS, generateCode, generateHandler };
