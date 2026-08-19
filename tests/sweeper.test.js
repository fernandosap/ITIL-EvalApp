'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const serverModule = require('../server.js');

const { getSweeperStatus, startServer, stopBackgroundJobs } = serverModule;
const PORT = 0; // let the OS pick a free port; we never make requests

// Helper: the sweeper is enabled iff both env requirements are met. This
// mirrors the check inside startBackgroundJobs but we replicate the logic
// here so the test can reason about expected state without inspecting
// the private _runtimeState.
function expectedSweeperEnabled() {
  const hasDbConfig = Boolean(
    process.env.HANA_HOST && process.env.HANA_USER
    && process.env.HANA_PASSWORD && process.env.HANA_SCHEMA
  );
  const autoClear = String(process.env.AUTO_CLEAR_STALE_SESSIONS || 'true').toLowerCase() !== 'false';
  return hasDbConfig && autoClear;
}

test('getSweeperStatus: returns a well-formed status object', () => {
  const status = getSweeperStatus();
  // Always-present shape
  assert.equal(typeof status.enabled, 'boolean');
  assert.equal(typeof status.tickCount, 'number');
  assert.equal(typeof status.totalCleared, 'number');
  assert.equal(typeof status.lastClearedCount, 'number');
  assert.equal(Array.isArray(status.lastClearedCodes), true);
  assert.equal(typeof status.thresholdMinutes, 'number');
  assert.equal(typeof status.intervalMinutes, 'number');
  // Threshold and interval are min 5 by construction (see server.js startup)
  assert.ok(status.thresholdMinutes >= 5);
  assert.ok(status.intervalMinutes >= 5);
});

test('startServer(0): sweeper enabled flag matches env-derived expectation', () => {
  const expected = expectedSweeperEnabled();
  // Trigger the start path (the sweeper may or may not start depending on env).
  const server = startServer(PORT);
  try {
    const status = getSweeperStatus();
    assert.equal(status.enabled, expected);
    // If enabled, the startedAt timestamp should be set; otherwise null.
    if (expected) {
      assert.equal(typeof status.startedAt, 'number');
      assert.ok(status.startedAt > 0);
    } else {
      assert.equal(status.startedAt, null);
    }
  } finally {
    stopBackgroundJobs();
    server.close();
  }
  // After cleanup, sweeper must be off regardless of env.
  assert.equal(getSweeperStatus().enabled, false);
});

test('getSweeperStatus: tickCount and isStuck are consistent (isStuck requires prior tick)', () => {
  const status = getSweeperStatus();
  // If no tick has ever happened, isStuck must be false even if enabled.
  if (status.tickCount === 0) {
    assert.equal(status.lastTickAt, null);
    assert.equal(status.isStuck, false);
  }
  // If enabled and lastTickAt is set, lastTickAgeMs must be set.
  if (status.enabled && status.lastTickAt) {
    assert.ok(typeof status.lastTickAgeMs === 'number' && status.lastTickAgeMs >= 0);
  }
});
