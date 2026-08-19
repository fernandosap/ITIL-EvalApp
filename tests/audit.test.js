'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');

function loadAuditWithFakeHana(opts = {}) {
  const { installFake = true } = opts;
  const fakeHanaPath = require.resolve('@sap/hana-client');
  const dbPoolPath = require.resolve('../shared/db-pool.js');
  const auditPath = require.resolve('../lib/audit.js');

  delete require.cache[auditPath];
  delete require.cache[dbPoolPath];
  if (installFake) {
    delete require.cache[fakeHanaPath];
    require.cache[fakeHanaPath] = {
      id: fakeHanaPath,
      filename: fakeHanaPath,
      loaded: true,
      exports: buildFakeHana(),
      children: [],
      paths: []
    };
  }
  return require('../lib/audit.js');
}

function buildFakeHana() {
  const state = {
    lastCreateConn: null,
    fakeHana: {
      createConnection() {
        const conn = {
          _queries: [],
          _disconnected: false,
          connect(opts) { state.lastCreateConn = opts; },
          exec(sql, params, cb) {
            this._queries.push({ sql, params });
            // Default: empty result set
            if (typeof cb === 'function') setImmediate(() => cb(null, []));
          },
          disconnect() { this._disconnected = true; }
        };
        return conn;
      }
    }
  };
  state.fakeHana.__state = state;
  return state.fakeHana;
}

test('init: stores config and resets cached schema flag', () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true,
    hanaHost: 'h',
    hanaPort: '443',
    hanaUser: 'u',
    hanaPassword: 'p',
    hanaSchema: 'S',
    hanaEncrypt: true,
    hanaSslValidateCertificate: false
  });
  const m = audit.getMetrics();
  assert.equal(m.attempts, 0);
  assert.equal(m.auditTablePresent, null); // not yet checked
});

test('tryWriteAdminAudit: returns no_db when config missing', async () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  // do NOT call init()
  const result = await audit.tryWriteAdminAudit({ action: 'admin_login_failed' });
  assert.equal(result, 'no_db');
  const m = audit.getMetrics();
  assert.equal(m.skippedNoDb, 1);
  assert.equal(m.attempts, 1);
  assert.equal(m.writes, 0);
  assert.equal(m.failures, 0);
});

function fakeConn() {
  return {
    _queries: [],
    exec(sql, params, cb) {
      this._queries.push({ sql, params });
      if (typeof cb === 'function') setImmediate(() => cb(null, []));
    },
    disconnect() { /* no-op */ }
  };
}

test('tryWriteAdminAudit: writes and counts on success', async () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true,
    hanaHost: 'h', hanaPort: '443', hanaUser: 'u', hanaPassword: 'p',
    hanaSchema: 'S', hanaEncrypt: true, hanaSslValidateCertificate: false
  });
  // Inject a withDb that always reports the table is present and runs the fn
  audit._setDepsForTests({
    withDb: async (fn) => fn(fakeConn()),
    hasAuditLogTable: async () => true
  });
  const result = await audit.tryWriteAdminAudit({
    action: 'admin_login_success',
    actor: 'admin',
    clientIp: '127.0.0.1',
    targetCode: 'ABC123',
    details: { role: 'admin' }
  });
  assert.equal(result, 'ok');
  const m = audit.getMetrics();
  assert.equal(m.attempts, 1);
  assert.equal(m.writes, 1);
  assert.equal(m.failures, 0);
  assert.equal(m.auditTablePresent, true);
});

test('tryWriteAdminAudit: counts as skipped when table missing', async () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true, hanaHost: 'h', hanaPort: '443', hanaUser: 'u',
    hanaPassword: 'p', hanaSchema: 'S', hanaEncrypt: true, hanaSslValidateCertificate: false
  });
  audit._setDepsForTests({
    withDb: async (fn) => fn(fakeConn()),
    hasAuditLogTable: async () => false
  });
  const result = await audit.tryWriteAdminAudit({ action: 'admin_login_failed' });
  assert.equal(result, 'no_table');
  const m = audit.getMetrics();
  assert.equal(m.attempts, 1);
  assert.equal(m.writes, 0);
  assert.equal(m.skippedNoTable, 1);
  assert.equal(m.failures, 0);
});

test('tryWriteAdminAudit: counts as failure on exception, never throws', async () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true, hanaHost: 'h', hanaPort: '443', hanaUser: 'u',
    hanaPassword: 'p', hanaSchema: 'S', hanaEncrypt: true, hanaSslValidateCertificate: false
  });
  audit._setDepsForTests({
    withDb: async () => { throw new Error('hana unreachable'); },
    hasAuditLogTable: async () => true
  });
  const result = await audit.tryWriteAdminAudit({ action: 'admin_login_failed' });
  assert.equal(result, 'failed');
  const m = audit.getMetrics();
  assert.equal(m.attempts, 1);
  assert.equal(m.failures, 1);
  assert.equal(m.lastFailureAt > 0, true);
  assert.match(m.lastFailureMessage, /hana unreachable/);
});

test('tryWriteAdminAudit: emits a structured log on failure', async () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true, hanaHost: 'h', hanaPort: '443', hanaUser: 'u',
    hanaPassword: 'p', hanaSchema: 'S', hanaEncrypt: true, hanaSslValidateCertificate: false
  });
  const logs = [];
  audit._setDepsForTests({
    withDb: async () => { throw new Error('connection reset'); },
    hasAuditLogTable: async () => true,
    log: (level, event, meta) => logs.push({ level, event, meta })
  });
  await audit.tryWriteAdminAudit({ action: 'admin_login_failed' });
  assert.equal(logs.length, 1);
  assert.equal(logs[0].event, 'admin_audit_write_failed');
  assert.equal(logs[0].level, 'warn');
  assert.equal(logs[0].meta.action, 'admin_login_failed');
  assert.match(logs[0].meta.message, /connection reset/);
});

test('tryWriteAdminAudit: emits a log on skipped_no_table', async () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true, hanaHost: 'h', hanaPort: '443', hanaUser: 'u',
    hanaPassword: 'p', hanaSchema: 'S', hanaEncrypt: true, hanaSslValidateCertificate: false
  });
  const logs = [];
  audit._setDepsForTests({
    withDb: async (fn) => fn({}),
    hasAuditLogTable: async () => false,
    log: (level, event, meta) => logs.push({ level, event, meta })
  });
  await audit.tryWriteAdminAudit({ action: 'codes_generate' });
  assert.equal(logs.length, 1);
  assert.equal(logs[0].event, 'admin_audit_skipped_no_table');
  assert.equal(logs[0].meta.action, 'codes_generate');
});

test('getMetrics: returns zeroed counters after reset', () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true, hanaHost: 'h', hanaPort: '443', hanaUser: 'u',
    hanaPassword: 'p', hanaSchema: 'S', hanaEncrypt: true, hanaSslValidateCertificate: false
  });
  const m = audit.getMetrics();
  assert.equal(m.attempts, 0);
  assert.equal(m.writes, 0);
  assert.equal(m.skippedNoTable, 0);
  assert.equal(m.skippedNoDb, 0);
  assert.equal(m.failures, 0);
  assert.equal(m.lastFailureAt, 0);
  assert.equal(m.lastFailureMessage, null);
});

test('tryWriteAdminAudit: accumulates mixed outcomes', async () => {
  const audit = loadAuditWithFakeHana();
  audit._resetForTests();
  audit.init({
    hasDbConfig: true, hanaHost: 'h', hanaPort: '443', hanaUser: 'u',
    hanaPassword: 'p', hanaSchema: 'S', hanaEncrypt: true, hanaSslValidateCertificate: false
  });
  // Walk through: ok, no_table, failed, ok
  let step = 0;
  const plans = [
    { hasTable: true, throw: null },
    { hasTable: false, throw: null },
    { hasTable: true, throw: new Error('conn reset') },
    { hasTable: true, throw: null }
  ];
  audit._setDepsForTests({
    withDb: async (fn) => {
      const plan = plans[step];
      step += 1;
      if (plan.throw) throw plan.throw;
      return fn(fakeConn());
    },
    hasAuditLogTable: async () => plans[step - 1].hasTable
  });
  await audit.tryWriteAdminAudit({ action: 'a' });
  await audit.tryWriteAdminAudit({ action: 'b' });
  await audit.tryWriteAdminAudit({ action: 'c' });
  await audit.tryWriteAdminAudit({ action: 'd' });
  const m = audit.getMetrics();
  assert.equal(m.attempts, 4);
  assert.equal(m.writes, 2);
  assert.equal(m.skippedNoTable, 1);
  assert.equal(m.failures, 1);
  assert.equal(m.skippedNoDb, 0);
});
