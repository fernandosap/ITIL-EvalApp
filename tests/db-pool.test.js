'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');

function snapshotEnv(keys) {
  const saved = {};
  for (const k of keys) saved[k] = process.env[k];
  return () => {
    for (const k of keys) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  };
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

// ---------------------------------------------------------------------------
// Loader: inject a fake `@sap/hana-client` into require.cache BEFORE
// requiring db-pool, so the wrapper sees the fake hana. We also delete
// the cached db-pool so the `const hana = require('@sap/hana-client')`
// at the top of db-pool.js re-resolves to the fake.
//
// Pass `{ installFake: false }` to get the real module (no mock) — used
// for pure-function tests that never call hana.
// ---------------------------------------------------------------------------

function loadDbPoolWithFakeHana(opts = {}) {
  const { installFake = true } = opts;
  const fakeHanaPath = require.resolve('@sap/hana-client');
  const dbPoolPath = require.resolve('../shared/db-pool.js');

  // Always start clean for db-pool.
  delete require.cache[dbPoolPath];

  if (installFake) {
    delete require.cache[fakeHanaPath];
    // The "last createPool call" record is shared across requires within
    // a single load (the fake hana is a single object). We re-attach
    // bookkeeping on the exports below.
    require.cache[fakeHanaPath] = {
      id: fakeHanaPath,
      filename: fakeHanaPath,
      loaded: true,
      exports: buildFakeHana(),
      children: [],
      paths: []
    };
  }

  return require('../shared/db-pool.js');
}

function buildFakeHana() {
  const state = {
    lastCreateCall: null,
    fakePool: null,
    createPool(connOpts, poolOpts) {
      state.lastCreateCall = { connOpts, poolOpts };
      return state.fakePool;
    }
  };
  // Expose state on a side-channel for tests.
  state.fakeHana = {
    createPool: state.createPool.bind(state)
  };
  state.fakeHana.__state = state;
  return state.fakeHana;
}

function fakePoolObj(connId = 1) {
  const pool = {
    _cleared: false,
    _gets: [],
    clear() { this._cleared = true; }
  };
  // The real @sap/hana-client supports both SYNC and CALLBACK forms of
  // getConnection. acquireConn uses the callback form so the pool can
  // queue requests when all slots are in use. The fake below also
  // supports both forms: a no-arg call returns the conn directly
  // (legacy behavior — kept for tests that exercise that path), and a
  // call with a callback invokes the callback on the next tick.
  pool.getConnection = function (cb) {
    const conn = {
      _id: connId,
      _disconnects: 0,
      disconnect() { this._disconnects += 1; pool._gets.push(this); }
    };
    if (typeof cb === 'function') {
      setImmediate(() => cb(null, conn));
      return;
    }
    return conn;
  };
  return pool;
}

// ---------------------------------------------------------------------------
// Config reader tests — pure functions, no hana involved.
// ---------------------------------------------------------------------------

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
    process.env.HANA_POOL_SIZE = '-5';
    assert.equal(dbPool.readPoolConfig(), null);
    process.env.HANA_POOL_SIZE = 'not-a-number';
    assert.equal(dbPool.readPoolConfig(), null);
  } finally { restore(); }
});

test('readPoolConfig: returns a config object when HANA_POOL_SIZE > 0', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '10';
    const cfg = dbPool.readPoolConfig();
    assert.ok(cfg);
    assert.equal(cfg.maxConnectedOrPooled, 10);
    assert.equal(cfg.pingCheck, true);  // default
    assert.equal(cfg.expirationInSeconds, 300);  // default 5 min
  } finally { restore(); }
});

test('readPoolConfig: rounds fractional HANA_POOL_SIZE down to int', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '7.9';
    const cfg = dbPool.readPoolConfig();
    assert.equal(cfg.maxConnectedOrPooled, 7);
  } finally { restore(); }
});

test('readPoolConfig: clamps HANA_POOL_SIZE to a minimum of 1', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '0.4';
    const cfg = dbPool.readPoolConfig();
    assert.equal(cfg.maxConnectedOrPooled, 1);
  } finally { restore(); }
});

test('readPoolConfig: pingCheck defaults to true, can be turned off', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '5';
    assert.equal(dbPool.readPoolConfig().pingCheck, true);
    process.env.HANA_POOL_PING_CHECK = 'false';
    assert.equal(dbPool.readPoolConfig().pingCheck, false);
    process.env.HANA_POOL_PING_CHECK = 'FALSE';
    assert.equal(dbPool.readPoolConfig().pingCheck, false);
  } finally { restore(); }
});

test('readPoolConfig: expirationInSeconds has a 60-second floor', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_POOL_SIZE = '5';
    process.env.HANA_POOL_TTL_SECONDS = '5';
    assert.equal(dbPool.readPoolConfig().expirationInSeconds, 60);
  } finally { restore(); }
});

test('readConnConfig: builds serverNode from HANA_HOST:PORT', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana({ installFake: false });
  try {
    process.env.HANA_HOST = 'h';
    process.env.HANA_PORT = '443';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    const cfg = dbPool.readConnConfig();
    assert.equal(cfg.serverNode, 'h:443');
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
    const pool = dbPool.getPool();
    assert.equal(pool, null);
    assert.equal(fakeHanaState.lastCreateCall, null);
  } finally { restore(); }
});

test('getPool: returns null when HANA_POOL_SIZE=0 (no hana call)', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fakeHanaState = require('@sap/hana-client').__state;
  try {
    process.env.HANA_POOL_SIZE = '0';
    const pool = dbPool.getPool();
    assert.equal(pool, null);
    assert.equal(fakeHanaState.lastCreateCall, null);
  } finally { restore(); }
});

test('getPool: calls hana.createPool SYNC with (connOpts, poolOpts) when enabled', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fakeHanaState = require('@sap/hana-client').__state;
  fakeHanaState.fakePool = fakePoolObj();
  try {
    process.env.HANA_POOL_SIZE = '5';
    process.env.HANA_HOST = 'myhost.example';
    process.env.HANA_PORT = '443';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    process.env.HANA_ENCRYPT = 'true';
    process.env.HANA_SSL_VALIDATE_CERTIFICATE = 'false';

    const pool = dbPool.getPool();
    // The result must be the pool, NOT a Promise wrapping it. That was
    // the previous bug: getPool() returned `new Promise(...)` and
    // withDb did `await getPool()`, which never resolved because the
    // lib doesn't call the callback.
    assert.ok(pool);
    assert.equal(typeof pool.then, 'undefined', 'getPool must be SYNC, not a Promise');

    const call = fakeHanaState.lastCreateCall;
    assert.ok(call, 'createPool must have been called');
    assert.equal(call.connOpts.serverNode, 'myhost.example:443');
    assert.equal(call.connOpts.uid, 'u');
    assert.equal(call.connOpts.pwd, 'p');
    assert.equal(call.connOpts.encrypt, true);
    assert.equal(call.connOpts.sslValidateCertificate, false);
    assert.equal(call.poolOpts.maxConnectedOrPooled, 5);
    assert.equal(call.poolOpts.pingCheck, true);
    assert.equal(call.poolOpts.expirationInSeconds, 300);
  } finally { restore(); }
});

test('getPool: caches the pool across calls with the same config', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fakeHanaState = require('@sap/hana-client').__state;
  try {
    process.env.HANA_POOL_SIZE = '5';
    process.env.HANA_HOST = 'h';
    process.env.HANA_PORT = '443';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    fakeHanaState.fakePool = fakePoolObj(1);
    const p1 = dbPool.getPool();
    const p2 = dbPool.getPool();
    assert.equal(p1, p2, 'same config must return same pool');
    assert.equal(fakeHanaState.lastCreateCall.connOpts.serverNode, 'h:443');
  } finally { restore(); }
});

test('getPool: re-creates the pool when HANA_POOL_SIZE changes', () => {
  const restore = snapshotEnv(ENV_KEYS);
  const dbPool = loadDbPoolWithFakeHana();
  const fakeHanaState = require('@sap/hana-client').__state;
  try {
    process.env.HANA_HOST = 'h';
    process.env.HANA_PORT = '443';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';

    process.env.HANA_POOL_SIZE = '5';
    fakeHanaState.fakePool = fakePoolObj(1);
    const p1 = dbPool.getPool();

    process.env.HANA_POOL_SIZE = '10';
    fakeHanaState.fakePool = fakePoolObj(2);
    const p2 = dbPool.getPool();
    assert.notEqual(p1, p2, 'changing HANA_POOL_SIZE must invalidate cache');
    assert.equal(p1._cleared, true, 'old pool must have been cleared');
  } finally { restore(); }
});

test('acquireConn: returns a Promise that resolves to pool.getConnection() result', () => {
  const dbPool = loadDbPoolWithFakeHana();
  const pool = fakePoolObj();
  const result = dbPool.acquireConn(pool);
  assert.ok(result instanceof Promise, 'acquireConn must return a Promise (queues when full)');
  return result.then((conn) => {
    assert.ok(conn);
    assert.equal(conn._id, 1);
  });
});

test('acquireConn: rejects when the pool callback errors (e.g. queue exhausted)', () => {
  const dbPool = loadDbPoolWithFakeHana();
  // Pool whose getConnection synchronously invokes the callback with an error.
  const errPool = {
    getConnection(cb) { cb(new Error('maxConnectedOrPool limit has been reached')); }
  };
  return dbPool.acquireConn(errPool).then(
    () => { throw new Error('should have rejected'); },
    (err) => {
      assert.match(String(err.message), /maxConnectedOrPool/);
    }
  );
});

test('acquireConn: throws when pool is null', () => {
  const dbPool = loadDbPoolWithFakeHana();
  assert.throws(() => dbPool.acquireConn(null), /pool is required/);
  assert.throws(() => dbPool.acquireConn(undefined), /pool is required/);
});

test('releaseConn: calls conn.disconnect() (returns to pool)', () => {
  const dbPool = loadDbPoolWithFakeHana();
  const fakeConn = { _disconnects: 0, disconnect() { this._disconnects += 1; } };
  dbPool.releaseConn(/* pool unused */ {}, fakeConn);
  assert.equal(fakeConn._disconnects, 1);
});

test('releaseConn: no-op when conn is null/undefined', () => {
  const dbPool = loadDbPoolWithFakeHana();
  assert.doesNotThrow(() => dbPool.releaseConn({}, null));
  assert.doesNotThrow(() => dbPool.releaseConn({}, undefined));
});

test('releaseConn: swallows disconnect() exceptions (broken conn)', () => {
  const dbPool = loadDbPoolWithFakeHana();
  const fakeConn = { disconnect() { throw new Error('boom'); } };
  assert.doesNotThrow(() => dbPool.releaseConn({}, fakeConn));
});

test('clearPool: no-op when pool is null', () => {
  const dbPool = loadDbPoolWithFakeHana();
  assert.doesNotThrow(() => dbPool.clearPool(null));
  assert.doesNotThrow(() => dbPool.clearPool(undefined));
});

test('clearPool: tolerates pools without a clear() method', () => {
  const dbPool = loadDbPoolWithFakeHana();
  assert.doesNotThrow(() => dbPool.clearPool({ /* no clear */ }));
});

test('clearPool: invokes pool.clear() on a real pool', () => {
  const dbPool = loadDbPoolWithFakeHana();
  const fakePool = { _cleared: false, clear() { this._cleared = true; } };
  dbPool.clearPool(fakePool);
  assert.equal(fakePool._cleared, true);
});
