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

// Helper: produce a 32+ char "secret" of the form "<tag>-<pad>". Two
// distinct tags always produce distinct secrets; both pass the
// 32-char minimum.
function secret(tag) {
  return `${tag}-${'x'.repeat(32)}`;  // 4 + 1 + 32 = 37 chars
}

function loadServerWithEnv(env) {
  // Snapshot the env vars we care about, set the test env, then
  // require server.js. Each test gets a fresh module instance so
  // module-level state doesn't leak between cases.
  //
  // Important: server.js loads .env on require, which sets
  // STARTUP_STRICT=true by default. We force it to 'false' unless
  // the test explicitly opts in, so a test that wants to assert
  // "fall back to legacy" doesn't accidentally hit the
  // STARTUP_STRICT throw path.
  const merged = { STARTUP_STRICT: 'false', ...env };
  const keys = Object.keys(merged);
  const saved = {};
  for (const k of keys) {
    saved[k] = Object.prototype.hasOwnProperty.call(process.env, k) ? process.env[k] : undefined;
    if (merged[k] === null || merged[k] === undefined) delete process.env[k];
    else process.env[k] = merged[k];
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
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: secret('new'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'v1');
    assert.equal(km.keys.v1, secret('new'));
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
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: secret('old'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const d1 = loadServerWithEnv(deploy1Env);
  let oldEnvelope;
  try {
    const km1 = d1.getSigningKeyMap();
    assert.equal(km1.current, 'v1');
    assert.equal(km1.keys.v1, secret('old'));
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
    RESULT_SIGNING_KEY_ID: 'v2', RESULT_SIGNING_KEY: secret('new2'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v1', RESULT_SIGNING_KEY_PREVIOUS: secret('old')
  };
  const d2 = loadServerWithEnv(deploy2Env);
  try {
    const km2 = d2.getSigningKeyMap();
    assert.equal(km2.current, 'v2');
    assert.equal(km2.keys.v2, secret('new2'));
    assert.equal(km2.keys.v1, secret('old'),
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
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: secret('old'),
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
    RESULT_SIGNING_KEY_ID: 'v2', RESULT_SIGNING_KEY: secret('new2'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v1', RESULT_SIGNING_KEY_PREVIOUS: secret('old')
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
    RESULT_SIGNING_KEY_ID: null, RESULT_SIGNING_KEY: secret('no-id'),
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

test('getSigningKeyMap: rejects previous id set without secret (probably a typo)', () => {
  // The previous behavior was to silently ignore the previous slot
  // and fall back to legacy with a warning. The hardened behavior
  // is to treat it as a hard error (operator touched the config but
  // left it half-finished) and fall back to legacy. Either way,
  // the result is `current: 'legacy'` — the difference is that the
  // operator now gets a clear error message at boot instead of a
  // silent fallback that might mask a real config bug.
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: secret('curr'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v0', RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy',
      'half-configured previous slot must fall back to legacy, not silently ignore');
    assert.equal(km.keys.v0, undefined, 'previous slot must not be populated when secret is empty');
  } finally { restore(); }
});

// ---------------------------------------------------------------------------
// Edge-case validation tests (post-rotation hardening)
// ---------------------------------------------------------------------------

test('getSigningKeyMap: rejects currentId === previousId (silent overwrite would be a footgun)', () => {
  // If the operator accidentally sets both ids to the same value,
  // the previous secret would silently overwrite the current. We
  // detect that, warn, and fall back to legacy.
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v2', RESULT_SIGNING_KEY: secret('new'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v2', RESULT_SIGNING_KEY_PREVIOUS: secret('old')
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy',
      'duplicate current/previous id must fall back to legacy, not silently overwrite');
  } finally { restore(); }
});

test('getSigningKeyMap: rejects kid "legacy" (reserved id)', () => {
  // The legacy slot is always populated by getLegacySigningSecret().
  // An operator-set kid of "legacy" would be silently overwritten.
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'legacy', RESULT_SIGNING_KEY: secret('new'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy',
      '"legacy" is reserved and must not be set explicitly');
    // The operator's "legacy" entry should NOT have made it into keys.
    assert.equal(km.keys.legacy.length, 64, 'legacy slot is 64-char hex derived secret');
  } finally { restore(); }
});

test('getSigningKeyMap: rejects kid with invalid characters', () => {
  // Disallow whitespace, slashes, NUL bytes, etc. — anything that
  // could confuse logging, log scanning, or shell quoting.
  // Whitespace is rejected by the regex (no trim), so 'v 1' is
  // rejected as-is.
  const invalidIds = ['v 1', 'v1;DROP', 'v1/../etc', 'v1$VAR', 'a'.repeat(65), ''];
  for (const bad of invalidIds) {
    const env = {
      HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
      REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
      RESULT_SIGNING_KEY_ID: bad, RESULT_SIGNING_KEY: secret('new'),
      RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
    };
    const { getSigningKeyMap, restore } = loadServerWithEnv(env);
    try {
      const km = getSigningKeyMap();
      assert.equal(km.current, 'legacy',
        `bad kid ${JSON.stringify(bad)} must fall back to legacy, not silently accept`);
    } finally { restore(); }
  }
});

test('getSigningKeyMap: accepts valid kid formats (letters, digits, dots, underscores, dashes)', () => {
  const validIds = ['v1', 'v2', 'prod-2026-08', 'key_a', 'release.1.2', 'A', 'a'.repeat(64)];
  for (const good of validIds) {
    const env = {
      HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
      REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
      RESULT_SIGNING_KEY_ID: good, RESULT_SIGNING_KEY: secret('new'),
      RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
    };
    const { getSigningKeyMap, restore } = loadServerWithEnv(env);
    try {
      const km = getSigningKeyMap();
      assert.equal(km.current, good, `kid ${JSON.stringify(good)} must be accepted`);
    } finally { restore(); }
  }
});

test('getSigningKeyMap: rejects secret shorter than 32 chars', () => {
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: 'short-secret-31-chars-xxxx',
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy',
      'a 31-char secret must be rejected, not silently accepted');
  } finally { restore(); }
});

test('getSigningKeyMap: keys map is Object.create(null) (prototype-pollution safe)', () => {
  // Even though the format check prevents "__proto__" as a kid,
  // a defense-in-depth check: the keys object should not be a
  // regular {} (so "__proto__" / "constructor" can't shadow
  // built-ins if the format check ever regresses).
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY_ID: 'v1', RESULT_SIGNING_KEY: secret('new'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(Object.getPrototypeOf(km.keys), null,
      'keys should be a null-prototype object');
    // 'hasOwnProperty' should not be a real key
    assert.equal(km.keys.hasOwnProperty, undefined,
      'null-prototype object should not have hasOwnProperty');
  } finally { restore(); }
});

test('getSigningKeyMap: STARTUP_STRICT=true + invalid config throws on first call', () => {
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    STARTUP_STRICT: 'true',
    RESULT_SIGNING_KEY_ID: 'v2', RESULT_SIGNING_KEY: secret('new'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v2', RESULT_SIGNING_KEY_PREVIOUS: secret('old')
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    assert.throws(() => getSigningKeyMap(), /RESULT_SIGNING_KEY_ID .*RESULT_SIGNING_KEY_PREVIOUS_ID .* are the same/);
  } finally { restore(); }
});

test('getSigningKeyMap: STARTUP_STRICT=false + invalid config falls back to legacy with warning', () => {
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    STARTUP_STRICT: 'false',
    RESULT_SIGNING_KEY_ID: 'v2', RESULT_SIGNING_KEY: secret('new'),
    RESULT_SIGNING_KEY_PREVIOUS_ID: 'v2', RESULT_SIGNING_KEY_PREVIOUS: secret('old')
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy');
  } finally { restore(); }
});

test('getSigningKeyMap: missing RESULT_SIGNING_KEY env with STARTUP_STRICT=true does NOT throw (no operator config = use legacy)', () => {
  // STARTUP_STRICT should only fail when the operator has set
  // SOMETHING but it's invalid. A clean env (no signing config
  // at all) is the "use legacy" path and should not throw.
  const env = {
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    STARTUP_STRICT: 'true',
    RESULT_SIGNING_KEY_ID: null, RESULT_SIGNING_KEY: null,
    RESULT_SIGNING_KEY_PREVIOUS_ID: null, RESULT_SIGNING_KEY_PREVIOUS: null
  };
  const { getSigningKeyMap, restore } = loadServerWithEnv(env);
  try {
    const km = getSigningKeyMap();
    assert.equal(km.current, 'legacy',
      'no config = legacy, not throw');
  } finally { restore(); }
});
