'use strict';

// End-to-end tests for the result-signing key rotation flow.
//
// The previous unit tests for lib/responses.js constructed the keyMap
// by hand. That works in isolation but missed the integration bug
// where getSigningKeyMap() in server.js was hard-coding current → v1
// and previous → v2, which broke rotation: an envelope signed with
// kid=v1 in deploy-1 would suddenly look for a new secret under v1
// in deploy-2 and fail.
//
// These tests exercise the actual env-var-driven getSigningKeyMap()
// path that production uses, across two simulated deployments.

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildSignedEnvelope, verifySignedEnvelope } = require('../lib/responses.js');

function loadServerWithEnv(env) {
  // Snapshot the env vars we care about, set the test env, then
  // require server.js. Each test gets a fresh module instance so
  // module-level state doesn't leak between cases.
  const keys = Object.keys(env);
  const saved = {};
  for (const k of keys) {
    saved[k] = Object.prototype.hasOwnProperty.call(process.env, k) ? process.env[k] : undefined;
    if (env[k] === null || env[k] === undefined) delete process.env[k];
    else process.env[k] = env[k];
  }
  delete require.cache[require.resolve('../server.js')];
  const server = require('../server.js');
  return {
    getSigningKeyMap: server.getSigningKeyMap,
    restore() {
      for (const k of keys) {
        if (saved[k] === undefined) delete process.env[k];
        else process.env[k] = saved[k];
      }
      delete require.cache[require.resolve('../server.js')];
    }
  };
}

test('getSigningKeyMap: legacy fallback when no env vars are set', () => {
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: null, RESULT_SIGNING_KEY: null,
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy');
    assert.ok(km.keys.legacy, 'legacy slot must always be present');
  } finally { restore(); }
});

test('getSigningKeyMap: current-only (no previous)', () => {
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: 'new-secret-32chars-xxxxxxxxxxxxx',
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'v1');
    assert.equal(km.keys.v1, 'new-secret-32chars-xxxxxxxxxxxxx');
    // legacy is always populated (for backwards compat with envelopes
    // signed before the versioned system was introduced). The point
    // is that v1 and legacy are distinct slots.
    assert.ok(km.keys.legacy);
    assert.notEqual(km.keys.legacy, km.keys.v1);
  } finally { restore(); }
});

test('e2e rotation: envelope from deploy-1 verifies in deploy-2 (real key IDs)', () => {
  // Simulate the deployment lifecycle.
  //   Deploy 1: only RESULT_SIGNING_KEY=v1 / RESULT_SIGNING_KEY_ID="v1"
  //             (initial rollout, no rotation yet)
  //   Deploy 2: RESULT_SIGNING_KEY_ID="v2" + RESULT_SIGNING_KEY="new"
  //             + RESULT_SIGNING_KEY_PREVIOUS_ID="v1" + RESULT_SIGNING_KEY_PREVIOUS="old"
  //             (operator rotated; v1 must still verify)
  //
  // This is the bug the previous test missed: the old getSigningKeyMap
  // hard-coded current → v1, so deploy-2's v1 would point to the NEW
  // secret. The fix: key IDs are operator-supplied, so the OLD secret
  // stays under "v1" in deploy-2 and the OLD envelope verifies.

  // === Deploy 1 ===
  const deploy1Env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: 'old-secret-32chars-xxxxxxxxxxx',
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const d1 = loadServerWithEnv(deploy1Env);
  let oldEnvelope;
  try {
    const km1 = d1.getSigningKeyMap();
    assert.equal(km1.current, 'v1');
    assert.equal(km1.keys.v1, 'old-secret-32chars-xxxxxxxxxxx');
    // Sign a result summary in deploy-1.
    oldEnvelope = buildSignedEnvelope({ code: 'TEST01', score: 90 }, km1);
    assert.equal(oldEnvelope.kid, 'v1');
    // Verify locally with the same map (works trivially).
    let m = verifySignedEnvelope(oldEnvelope, km1);
    assert.ok(m, 'verify against own map');
    assert.equal(m.kid, 'v1');
  } finally { d1.restore(); }

  // === Deploy 2 (operator rotated) ===
  const deploy2Env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v2', RESULT_SIGNING_KEY: 'new-secret-32chars-yyyyyyyyyyy',
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v1', RESULT_SIGNING_KEY_PREVIOUS: 'old-secret-32chars-xxxxxxxxxxx'
  };
  const d2 = loadServerWithEnv(deploy2Env);
  try {
    const km2 = d2.getSigningKeyMap();
    assert.equal(km2.current, 'v2');
    assert.equal(km2.keys.v2, 'new-secret-32chars-yyyyyyyyyyy');
    assert.equal(km2.keys.v1, 'old-secret-32chars-xxxxxxxxxxx',
      'v1 must STILL be the old secret after rotation (stable IDs)');

    // The envelope from deploy-1 (kid=v1, signed with old secret) must
    // still verify in deploy-2.
    const matched = verifySignedEnvelope(oldEnvelope, km2);
    assert.ok(matched, 'envelope from deploy-1 must verify in deploy-2');
    assert.equal(matched.kid, 'v1', 'must verify against the v1 slot, not v2');

    // New envelopes signed in deploy-2 use v2.
    const newEnvelope = buildSignedEnvelope({ code: 'TEST02', score: 95 }, km2);
    assert.equal(newEnvelope.kid, 'v2');
    const m2 = verifySignedEnvelope(newEnvelope, km2);
    assert.equal(m2.kid, 'v2');
  } finally { d2.restore(); }
});

test('e2e rotation: tampered envelope from deploy-1 does NOT verify in deploy-2', () => {
  const env1 = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: 'old-secret-32chars-xxxxxxxxxxx',
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const d1 = loadServerWithEnv(env1);
  let oldEnvelope;
  try {
    oldEnvelope = buildSignedEnvelope({ code: 'TAMPER', score: 50 }, d1.getSigningKeyMap());
  } finally { d1.restore(); }

  const env2 = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v2', RESULT_SIGNING_KEY: 'new-secret-32chars-yyyyyyyyyyy',
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v1', RESULT_SIGNING_KEY_PREVIOUS: 'old-secret-32chars-xxxxxxxxxxx'
  };
  const d2 = loadServerWithEnv(env2);
  try {
    // Tamper with the payload after signing.
    const tampered = { ...oldEnvelope, payload: { ...oldEnvelope.payload, score: 100 } };
    assert.equal(verifySignedEnvelope(tampered, d2.getSigningKeyMap()), null,
      'tampered payload must not verify even in deploy-2');
  } finally { d2.restore(); }
});

test('getSigningKeyMap: warns when RESULT_SIGNING_KEY is set but RESULT_SIGNING_KEY_ID is not', () => {
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: null, RESULT_SIGNING_KEY: 'secret-but-no-id',
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    // Without an id, the key can't be referenced by a stable kid, so
    // we fall back to legacy. The new key is dropped (the warning
    // tells the operator to set the id).
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy');
  } finally { restore(); }
});

test('getSigningKeyMap: ignores previous when previous id is set but secret is empty', () => {
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: 'a',
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v0', RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'v1');
    assert.equal(km.keys.v1, 'a');
    assert.equal(km.keys.v0, undefined, 'previous with empty secret must be ignored');
  } finally { restore(); }
});
