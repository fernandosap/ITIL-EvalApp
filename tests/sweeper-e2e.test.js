'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const sweeper = require('../lib/sweeper.js');

// Build a fake HANA connection that records every exec() call and
// returns pre-canned results. The sweeper module only needs
// conn.exec(sql, params, cb) — same shape as the real @sap/hana-client.
function makeFakeHanaConn(scenario) {
  const calls = [];
  return {
    _calls: calls,
    exec(sql, params, cb) {
      calls.push({ sql, params });
      const result = scenario(sql, params);
      // Callback form: cb(err, rows)
      setImmediate(() => cb(null, result));
    }
  };
}

test('clearStaleSessionsWithConn: throws without conn', async () => {
  await assert.rejects(
    () => sweeper.clearStaleSessionsWithConn(null, 30),
    /conn is required/
  );
});

test('clearStaleSessionsWithConn: throws on bad threshold', async () => {
  const conn = { exec(sql, p, cb) { setImmediate(() => cb(null, [])); } };
  await assert.rejects(
    () => sweeper.clearStaleSessionsWithConn(conn, 0),
    /thresholdMinutes must be a positive number/
  );
  await assert.rejects(
    () => sweeper.clearStaleSessionsWithConn(conn, -1),
    /thresholdMinutes must be a positive number/
  );
  await assert.rejects(
    () => sweeper.clearStaleSessionsWithConn(conn, NaN),
    /thresholdMinutes must be a positive number/
  );
  await assert.rejects(
    () => sweeper.clearStaleSessionsWithConn(conn, '30'),
    /thresholdMinutes must be a positive number/
  );
});

test('clearStaleSessionsWithConn: empty result set returns [] and does not delete', async () => {
  const conn = makeFakeHanaConn(() => []);
  const cleared = await sweeper.clearStaleSessionsWithConn(conn, 30);
  assert.deepEqual(cleared, []);
  // The first exec is the SELECT; no DELETE/UPDATE should follow.
  assert.equal(conn._calls.length, 1);
  assert.match(conn._calls[0].sql, /^SELECT ACCESS_CODE/);
});

test('clearStaleSessionsWithConn: 1 stale session → 1 DELETE + 1 UPDATE', async () => {
  let deleteCalled = 0;
  const conn = makeFakeHanaConn((sql) => {
    if (/^SELECT ACCESS_CODE/.test(sql)) return [{ ACCESS_CODE: 'ABC123' }];
    return [];
  });
  const deleteSession = async () => { deleteCalled += 1; };
  const cleared = await sweeper.clearStaleSessionsWithConn(conn, 30, deleteSession);
  assert.deepEqual(cleared, ['ABC123']);
  assert.equal(deleteCalled, 1, 'injected deleteSession must be called once per row');
  // 1 SELECT + 1 (injected) DELETE + 1 UPDATE = 3 calls
  assert.equal(conn._calls.length, 2, 'fake conn should have SELECT + UPDATE');
  assert.match(conn._calls[1].sql, /^UPDATE ACCESS_CODES/);
  assert.match(conn._calls[1].sql, /SET STATUS = 'unused'/);
  assert.match(conn._calls[1].sql, /AND STATUS = 'active'/);
  assert.equal(conn._calls[1].params[0], 'ABC123');
});

test('clearStaleSessionsWithConn: 3 stale sessions cleared in order, all uppercase', async () => {
  const conn = makeFakeHanaConn((sql) => {
    if (/^SELECT ACCESS_CODE/.test(sql)) {
      return [{ ACCESS_CODE: 'abc123' }, { ACCESS_CODE: 'DEF456' }, { ACCESS_CODE: '  ghi789  ' }];
    }
    return [];
  });
  const cleared = await sweeper.clearStaleSessionsWithConn(conn, 30, async () => {});
  assert.deepEqual(cleared, ['ABC123', 'DEF456', 'GHI789']);
  // 1 SELECT + 3 UPDATE = 4 calls
  assert.equal(conn._calls.length, 4);
  assert.equal(conn._calls[1].params[0], 'ABC123');
  assert.equal(conn._calls[2].params[0], 'DEF456');
  assert.equal(conn._calls[3].params[0], 'GHI789');
});

test('clearStaleSessionsWithConn: thresholdMinutes=30 → ADD_SECONDS(-30*60)', async () => {
  const conn = makeFakeHanaConn(() => []);
  await sweeper.clearStaleSessionsWithConn(conn, 30);
  const selectCall = conn._calls[0];
  assert.match(selectCall.sql, /ADD_SECONDS\(CURRENT_UTCTIMESTAMP, \?\)/);
  assert.equal(selectCall.params[0], -1800);
});

test('clearStaleSessionsWithConn: thresholdMinutes=5 → ADD_SECONDS(-5*60)', async () => {
  const conn = makeFakeHanaConn(() => []);
  await sweeper.clearStaleSessionsWithConn(conn, 5);
  assert.equal(conn._calls[0].params[0], -300);
});

test('clearStaleSessionsWithConn: skips rows with empty/whitespace codes', async () => {
  const conn = makeFakeHanaConn((sql) => {
    if (/^SELECT ACCESS_CODE/.test(sql)) {
      return [{ ACCESS_CODE: '' }, { ACCESS_CODE: '   ' }, { ACCESS_CODE: null }, { ACCESS_CODE: 'REAL1' }];
    }
    return [];
  });
  const cleared = await sweeper.clearStaleSessionsWithConn(conn, 30, async () => {});
  assert.deepEqual(cleared, ['REAL1']);
  // 1 SELECT + 1 UPDATE (only the real one) = 2 calls
  assert.equal(conn._calls.length, 2);
});

test('clearStaleSessionsWithConn: uses default DELETE FROM EXAM_SESSIONS when no deleteSession injected', async () => {
  const conn = makeFakeHanaConn((sql) => {
    if (/^SELECT ACCESS_CODE/.test(sql)) return [{ ACCESS_CODE: 'XYZ' }];
    return [];
  });
  const cleared = await sweeper.clearStaleSessionsWithConn(conn, 30);
  assert.deepEqual(cleared, ['XYZ']);
  // Should have seen the default DELETE FROM EXAM_SESSIONS on the fake conn.
  const sawDelete = conn._calls.some((c) => /^DELETE FROM EXAM_SESSIONS/.test(c.sql));
  assert.equal(sawDelete, true, 'default deleteSession must issue DELETE FROM EXAM_SESSIONS');
});

test('clearStaleSessionsWithConn: surfaces SQL errors', async () => {
  const conn = {
    exec(sql, params, cb) {
      setImmediate(() => cb(new Error('hana exploded')));
    }
  };
  await assert.rejects(
    () => sweeper.clearStaleSessionsWithConn(conn, 30),
    /hana exploded/
  );
});

test('clearStaleSessionsWithConn: surfaces deleteSession errors', async () => {
  const conn = makeFakeHanaConn((sql) => {
    if (/^SELECT ACCESS_CODE/.test(sql)) return [{ ACCESS_CODE: 'BOOM' }];
    return [];
  });
  const deleteSession = async () => { throw new Error('delete failed'); };
  await assert.rejects(
    () => sweeper.clearStaleSessionsWithConn(conn, 30, deleteSession),
    /delete failed/
  );
});

test('clearStaleSessionsWithConn: e2e flow — SELECT, DELETE, UPDATE all in order with correct SQL', async () => {
  // This is the realistic scenario: 1 stale session, verify the
  // full SQL flow matches what server.js would issue on a real sweep.
  const conn = makeFakeHanaConn((sql) => {
    if (/^SELECT ACCESS_CODE/.test(sql)) return [{ ACCESS_CODE: 'E2E001' }];
    return [];
  });
  const cleared = await sweeper.clearStaleSessionsWithConn(conn, 30, async () => {});
  assert.deepEqual(cleared, ['E2E001']);
  assert.equal(conn._calls.length, 2);
  // 1. SELECT for stale sessions
  const sel = conn._calls[0];
  assert.match(sel.sql, /SELECT ACCESS_CODE\s+FROM EXAM_SESSIONS/);
  assert.match(sel.sql, /UPDATED_AT < ADD_SECONDS\(CURRENT_UTCTIMESTAMP, \?\)/);
  assert.match(sel.sql, /ORDER BY UPDATED_AT ASC/);
  assert.equal(sel.params[0], -1800);
  // 2. UPDATE returns the code to the unused pool
  const upd = conn._calls[1];
  assert.equal(upd.sql.startsWith('UPDATE ACCESS_CODES'), true);
  assert.match(upd.sql, /SET STATUS = 'unused'/);
  assert.match(upd.sql, /AND STATUS = 'active'/);
  assert.match(upd.sql, /NOT EXISTS \(\s*SELECT 1 FROM EXAM_RESULTS/);
  assert.equal(upd.params[0], 'E2E001');
});
