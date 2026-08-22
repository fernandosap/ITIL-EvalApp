'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const MODULE = path.join(__dirname, '..', 'shared', 'db-pool.js');
const HANA_MODULE = require.resolve('@sap/hana-client');

function snapshotEnv(keys) {
  const snap = {};
  for (const key of keys) snap[key] = process.env[key];
  return () => {
    for (const key of keys) {
      if (snap[key] == null) delete process.env[key];
      else process.env[key] = snap[key];
    }
  };
}

function makeFakeHana() {
  const state = {
    createPoolCalls: [],
    pools: [],
    getConnectionCalls: 0
  };
  return {
    __state: state,
    createPool(connOpts, poolOpts) {
      state.createPoolCalls.push({ connOpts, poolOpts });
      const pool = {
        connOpts,
        poolOpts,
        clearCount: 0,
        clear() { this.clearCount += 1; },
        getConnection(cb) {
          state.getConnectionCalls += 1;
          if (typeof cb === 'function') cb(null, { disconnect() {} });
          else return { disconnect() {} };
        }
      };
      state.pools.push(pool);
      return pool;
    }
  };
}

function loadDbPoolWithFakeHana({ installFake = true } = {}) {
  delete require.cache[MODULE];
  let original = require.cache[HANA_MODULE];
  if (installFake) {
    require.cache[HANA_MODULE] = {
      id: HANA_MODULE,
      filename: HANA_MODULE,
      loaded: true,
      exports: makeFakeHana(),
      children: [],
      paths: []
    };
  }
  const mod = require(MODULE);
  mod.__restoreHana = () => {
    delete require.cache[MODULE];
    if (original) require.cache[HANA_MODULE] = original;
    else delete require.cache[HANA_MODULE];
  };
  return mod;
}

const ENV_KEYS = [
  'HANA_POOL_SIZE',
  'HANA_POOL_PING_CHECK',
  'HANA_POOL_TTL_SECONDS',
  'HANA_HOST',
  'HANA_PORT',
  'HANA_USER',
  'HANA_PASSWORD',
  'HANA_ENCRYPT',
  'HANA_SSL_VALIDATE_CERTIFICATE'
];

test.afterEach(() => {
  delete require.cache[MODULE];
});

test('readPoolConfig: returns null when HANA_POOL_SIZE is unset', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    delete process.env.HANA_POOL_SIZE;
    assert.equal(dbPool.readPoolConfig(), null);
  } finally { restore(); }
});

test('readPoolConfig: returns null when HANA_POOL_SIZE=0', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '0';
    assert.equal(dbPool.readPoolConfig(), null);
  } finally { restore(); }
});

test('readPoolConfig: returns null when HANA_POOL_SIZE is negative or NaN', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '-2';
    assert.equal(dbPool.readPoolConfig(), null);
    process.env.HANA_POOL_SIZE = 'abc';
    assert.equal(dbPool.readPoolConfig(), null);
  } finally { restore(); }
});

test('readPoolConfig: returns a config object when HANA_POOL_SIZE > 0', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '5';
    const cfg = dbPool.readPoolConfig();
    assert.equal(cfg.maxConnectedOrPooled, 5);
  } finally { restore(); }
});

test('readPoolConfig: rounds fractional HANA_POOL_SIZE down to int', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '4.9';
    assert.equal(dbPool.readPoolConfig().maxConnectedOrPooled, 4);
  } finally { restore(); }
});

test('readPoolConfig: clamps HANA_POOL_SIZE to a minimum of 1', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '0.5';
    assert.equal(dbPool.readPoolConfig().maxConnectedOrPooled, 1);
  } finally { restore(); }
});

test('readPoolConfig: pingCheck defaults to true, can be turned off', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '2';
    delete process.env.HANA_POOL_PING_CHECK;
    assert.equal(dbPool.readPoolConfig().pingCheck, true);
    process.env.HANA_POOL_PING_CHECK = 'false';
    assert.equal(dbPool.readPoolConfig().pingCheck, false);
  } finally { restore(); }
});

test('readPoolConfig: expirationInSeconds has a 60-second floor', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '2';
    process.env.HANA_POOL_TTL_SECONDS = '5';
    assert.equal(dbPool.readPoolConfig().expirationInSeconds, 60);
    process.env.HANA_POOL_TTL_SECONDS = '600';
    assert.equal(dbPool.readPoolConfig().expirationInSeconds, 600);
  } finally { restore(); }
});

test('readConnConfig: builds serverNode from HANA_HOST:PORT', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_HOST = 'h';
    process.env.HANA_PORT = '444';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    const cfg = dbPool.readConnConfig();
    assert.equal(cfg.serverNode, 'h:444');
    assert.equal(cfg.uid, 'u');
    assert.equal(cfg.pwd, 'p');
  } finally { restore(); }
});

test('readConnConfig: defaults HANA_PORT to 443', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_HOST = 'h';
    delete process.env.HANA_PORT;
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    assert.equal(dbPool.readConnConfig().serverNode, 'h:443');
  } finally { restore(); }
});

test('readConnConfig: defaults encrypt=true, sslValidateCertificate=true', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_HOST = 'h';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    delete process.env.HANA_ENCRYPT;
    delete process.env.HANA_SSL_VALIDATE_CERTIFICATE;
    const cfg = dbPool.readConnConfig();
    assert.equal(cfg.encrypt, true);
    assert.equal(cfg.sslValidateCertificate, true);
  } finally { restore(); }
});

// ---------------------------------------------------------------------------
// Pool lifecycle tests — use the fake hana to observe the wrapper.
// ---------------------------------------------------------------------------

test('getPool: returns null when HANA_POOL_SIZE is unset (no hana call)', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fakeHanaState = require('@sap/hana-client').__state;
  try {
    delete process.env.HANA_POOL_SIZE;
    assert.equal(dbPool.getPool(), null);
    assert.equal(fakeHanaState.createPoolCalls.length, 0);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('getPool: returns null when HANA_POOL_SIZE=0 (no hana call)', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fakeHanaState = require('@sap/hana-client').__state;
  try {
    process.env.HANA_POOL_SIZE = '0';
    assert.equal(dbPool.getPool(), null);
    assert.equal(fakeHanaState.createPoolCalls.length, 0);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('getPool: calls hana.createPool SYNC with (connOpts, poolOpts) when enabled', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fake = require('@sap/hana-client');
  try {
    process.env.HANA_POOL_SIZE = '3';
    process.env.HANA_HOST = 'h';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    const pool = dbPool.getPool();
    assert.ok(pool);
    assert.equal(fake.__state.createPoolCalls.length, 1);
    assert.equal(fake.__state.createPoolCalls[0].poolOpts.maxConnectedOrPooled, 3);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('getPool: caches the pool across calls with the same config', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fake = require('@sap/hana-client');
  try {
    process.env.HANA_POOL_SIZE = '2';
    process.env.HANA_HOST = 'h';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    const a = dbPool.getPool();
    const b = dbPool.getPool();
    assert.equal(a, b);
    assert.equal(fake.__state.createPoolCalls.length, 1);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('getPool: re-creates the pool when HANA_POOL_SIZE changes', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fake = require('@sap/hana-client');
  try {
    process.env.HANA_POOL_SIZE = '2';
    process.env.HANA_HOST = 'h';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    const first = dbPool.getPool();
    process.env.HANA_POOL_SIZE = '3';
    const second = dbPool.getPool();
    assert.notEqual(first, second);
    assert.equal(first.clearCount, 1);
    assert.equal(fake.__state.createPoolCalls.length, 2);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('acquireConn: returns a Promise that resolves to pool.getConnection() result', async () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  try {
    const conn = { id: 1 };
    const pool = { getConnection(cb) { setImmediate(() => cb(null, conn)); } };
    assert.equal(await dbPool.acquireConn(pool), conn);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('acquireConn: rejects when the pool callback errors (e.g. queue exhausted)', async () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  try {
    const pool = { getConnection(cb) { cb(new Error('pool full')); } };
    await assert.rejects(dbPool.acquireConn(pool), /pool full/);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('acquireConn: throws when pool is null', async () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  try {
    await assert.rejects(() => dbPool.acquireConn(null), /pool is required/);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('releaseConn: calls conn.disconnect() (returns to pool)', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  try {
    let calls = 0;
    dbPool.releaseConn({}, { disconnect() { calls += 1; } });
    assert.equal(calls, 1);
  } finally { restore(); dbPool.__restoreHana(); }
});

test('releaseConn: no-op when conn is null/undefined', () => {
  const dbPool = loadDbPoolWithFakeHana();
  try {
    assert.doesNotThrow(() => dbPool.releaseConn({}, null));
  } finally { dbPool.__restoreHana(); }
});

test('releaseConn: swallows disconnect() exceptions (broken conn)', () => {
  const dbPool = loadDbPoolWithFakeHana();
  try {
    assert.doesNotThrow(() => dbPool.releaseConn({}, { disconnect() { throw new Error('broken'); } }));
  } finally { dbPool.__restoreHana(); }
});

test('clearPool: no-op when pool is null', () => {
  const dbPool = loadDbPoolWithFakeHana();
  try { assert.doesNotThrow(() => dbPool.clearPool(null)); }
  finally { dbPool.__restoreHana(); }
});

test('clearPool: tolerates pools without a clear() method', () => {
  const dbPool = loadDbPoolWithFakeHana();
  try { assert.doesNotThrow(() => dbPool.clearPool({})); }
  finally { dbPool.__restoreHana(); }
});

test('clearPool: invokes pool.clear() on a real pool', () => {
  const dbPool = loadDbPoolWithFakeHana();
  try {
    let count = 0;
    dbPool.clearPool({ clear() { count += 1; } });
    assert.equal(count, 1);
  } finally { dbPool.__restoreHana(); }
});
