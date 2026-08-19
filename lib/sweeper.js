'use strict';

// Stale-session sweeper. Extracted from server.js so the SQL logic is
// testable with a fake connection (no real HANA required). The sweeper
// finds EXAM_SESSIONS rows whose UPDATED_AT is older than
// `thresholdMinutes`, deletes them, and returns the matching
// ACCESS_CODEs to the 'unused' pool (only if they're still in 'active'
// and have no EXAM_RESULTS row — i.e. the candidate didn't actually
// finish the exam before the session went stale).
//
// The SQL uses CURRENT_UTCTIMESTAMP and ADD_SECONDS so the timestamp
// math is done in HANA's clock, not the Node process clock. That's
// intentional: if Node and HANA clocks drift, the sweeper still
// respects HANA's "now" for the cutoff.

const { CODE_STATUS } = require('../shared/constants.js');

// `conn` is a HANA connection that responds to conn.exec(sql, params, cb).
// `thresholdMinutes` is how old a session must be (in minutes) to qualify.
// `deleteSession` is an injected dependency (defaults to a simple
// DELETE FROM EXAM_SESSIONS) so tests can swap it.
async function clearStaleSessionsWithConn(conn, thresholdMinutes, deleteSession) {
  if (typeof conn !== 'object' || conn === null) {
    throw new Error('clearStaleSessionsWithConn: conn is required');
  }
  if (!Number.isFinite(thresholdMinutes) || thresholdMinutes < 1) {
    throw new Error('clearStaleSessionsWithConn: thresholdMinutes must be a positive number');
  }
  const doDelete = typeof deleteSession === 'function'
    ? deleteSession
    : async (c, code) => exec(c, 'DELETE FROM EXAM_SESSIONS WHERE ACCESS_CODE = ?', [code]);

  const staleRows = await exec(
    conn,
    `SELECT ACCESS_CODE
       FROM EXAM_SESSIONS
      WHERE UPDATED_AT < ADD_SECONDS(CURRENT_UTCTIMESTAMP, ?)
      ORDER BY UPDATED_AT ASC`,
    [-1 * Number(thresholdMinutes) * 60]
  );

  const cleared = [];
  for (const row of staleRows) {
    const code = String(row.ACCESS_CODE || '').trim().toUpperCase();
    if (!code) continue;
    await doDelete(conn, code);
    await exec(
      conn,
      `UPDATE ACCESS_CODES
          SET STATUS = '${CODE_STATUS.UNUSED}',
              UPDATED_AT = CURRENT_UTCTIMESTAMP
        WHERE ACCESS_CODE = ?
          AND STATUS = '${CODE_STATUS.ACTIVE}'
          AND NOT EXISTS (
            SELECT 1 FROM EXAM_RESULTS r WHERE r.ACCESS_CODE = ACCESS_CODES.ACCESS_CODE
          )`,
      [code]
    );
    cleared.push(code);
  }
  return cleared;
}

// Thin wrapper around conn.exec. Mirrors the call shape used in
// server.js: callback-style with err/rows. Resolves to rows[].
function exec(conn, sql, params) {
  return new Promise((resolve, reject) => {
    conn.exec(sql, params || [], (err, rows) => {
      if (err) return reject(err);
      resolve(rows || []);
    });
  });
}

module.exports = {
  clearStaleSessionsWithConn,
  exec
};
