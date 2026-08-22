'use strict';

const { withConnection, exec } = require('./db.js');

const DEFAULT_INTERVAL_MS = 60 * 60 * 1000;
let timer = null;

function retentionDays(env = process.env) {
  const raw = Number(env.PROCTOR_INCIDENT_RETENTION_DAYS || 90);
  return Number.isFinite(raw) ? Math.max(0, Math.floor(raw)) : 90;
}

async function cleanupRuntimeState(now = Date.now(), env = process.env) {
  return withConnection(async (conn) => {
    const expiredSessions = await exec(conn,
      'DELETE FROM ADMIN_SSO_SESSIONS_V3 WHERE EXPIRES_AT_MS <= ?', [Number(now)]);
    const expiredBuckets = await exec(conn,
      'DELETE FROM APP_RATE_LIMITS WHERE RESET_AT_MS <= ?', [Number(now)]);
    let incidentRetentionApplied = false;
    const days = retentionDays(env);
    if (days > 0) {
      await exec(conn,
        'DELETE FROM EXAM_PROCTOR_INCIDENTS WHERE SERVER_TIME < ADD_DAYS(CURRENT_UTCTIMESTAMP, ?)', [-days]);
      incidentRetentionApplied = true;
    }
    return {
      expiredSessions: Number(expiredSessions?.affectedRows || 0),
      expiredBuckets: Number(expiredBuckets?.affectedRows || 0),
      incidentRetentionDays: days,
      incidentRetentionApplied
    };
  }, { env });
}

function startHousekeeping(options = {}) {
  if (timer) return timer;
  const env = options.env || process.env;
  const intervalMs = Math.max(60_000, Number(options.intervalMs || env.RUNTIME_HOUSEKEEPING_INTERVAL_MS || DEFAULT_INTERVAL_MS));
  const log = typeof options.log === 'function' ? options.log : () => {};
  const tick = () => cleanupRuntimeState(Date.now(), env)
    .then((result) => log('info', 'runtime_housekeeping_completed', result))
    .catch((err) => log('warn', 'runtime_housekeeping_failed', { message: err.message }));
  timer = setInterval(tick, intervalMs);
  if (typeof timer.unref === 'function') timer.unref();
  void tick();
  return timer;
}

function stopHousekeeping() {
  if (timer) clearInterval(timer);
  timer = null;
}

module.exports = { retentionDays, cleanupRuntimeState, startHousekeeping, stopHousekeeping };
