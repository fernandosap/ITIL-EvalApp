'use strict';

// Tests for the legacy-key freeze env var (Point 3 of the
// signing-system hardening pass).
//
// Without RESULT_SIGNING_LEGACY_KEY, the legacy slot is filled
// with sha256(HANA_PASSWORD|...|APP_REVISION) — derived from
// env vars that the operator might rotate. After a rotation
// of any of those, the derived secret CHANGES, and any envelope
// signed before the rotation can no longer be verified.
//
// RESULT_SIGNING_LEGACY_KEY lets the operator freeze the
// secret used for the `legacy` kid. Set it ONCE, after the
// historical envelopes are signed, and verification keeps
// working even if HANA_PASSWORD etc. change.

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

function loadSigningContext(env) {
  // Wipe process.env first so the test is hermetic.
  const saved = {};
  for (const k of Object.keys(process.env)) {
    if (k.startsWith('HANA_') || k.startsWith('ADMIN_') || k.startsWith('MANAGER_') ||
        k.startsWith('REVIEWER_') || k.startsWith('CONTENT_EDITOR_') ||
        k === 'APP_REVISION' || k.startsWith('RESULT_SIGNING_')) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
  }
  // Set test env.
  const merged = { STARTUP_STRICT: 'false', ...env };
  for (const k of Object.keys(merged)) {
    if (merged[k] === null || merged[k] === undefined) delete process.env[k];
    else process.env[k] = merged[k];
  }
  delete require.cache[require.resolve('../lib/signing-context.js')];
  delete require.cache[require.resolve('../server.js')];
  const signing = require('../lib/signing-context.js');
  return {
    signing,
    ctx: signing.getSigningContext(),
    restore() {
      for (const k of Object.keys(process.env)) {
        if (k.startsWith('HANA_') || k.startsWith('ADMIN_') || k.startsWith('MANAGER_') ||
            k.startsWith('REVIEWER_') || k.startsWith('CONTENT_EDITOR_') ||
            k === 'APP_REVISION' || k.startsWith('RESULT_SIGNING_')) {
          delete process.env[k];
        }
      }
      for (const k of Object.keys(saved)) process.env[k] = saved[k];
      delete require.cache[require.resolve('../lib/signing-context.js')];
      delete require.cache[require.resolve('../server.js')];
    }
  };
}

test('legacy: derived secret is HANA_PASSWORD + hashes + APP_REVISION (default)', () => {
  const { ctx, restore } = loadSigningContext({
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_KEY: null
  });
  try {
    const expectedLegacy = crypto
      .createHash('sha256')
      .update(['p', 'a', 'm', 'r', 'c', 'v'].join('|'))
      .digest('hex');
    assert.equal(ctx.keys.legacy, expectedLegacy);
  } finally { restore(); }
});

test('legacy: RESULT_SIGNING_LEGACY_KEY overrides the derived secret', () => {
  const { ctx, restore } = loadSigningContext({
    HANA_PASSWORD: 'p', ADMIN_HASH: 'a', MANAGER_HASH: 'm',
    REVIEWER_HASH: 'r', CONTENT_EDITOR_HASH: 'c', APP_REVISION: 'v',
    RESULT_SIGNING_LEGACY_KEY: 'frozen-legacy-secret-not-derived-from-env'
  });
  try {
    assert.equal(ctx.keys.legacy, 'frozen-legacy-secret-not-derived-from-env',
      'explicit RESULT_SIGNING_LEGACY_KEY wins over the derived secret');
  } finally { restore(); }
});

test('legacy: rotation scenario — old envelope verifies after HANA_PASSWORD rotates', () => {
  // This is the scenario the env var was designed for:
  // 1. Operator had a config where HANA_PASSWORD = 'old-pw'
  // 2. They froze the legacy secret at that point:
  //    RESULT_SIGNING_LEGACY_KEY = sha256('old-pw|...|old-rev')
  // 3. Later, HANA_PASSWORD rotates to 'new-pw' for security
  // 4. The historical envelopes still verify because the legacy
  //    secret is now frozen, not derived.
  const historicalSecret = 'frozen-at-old-deploy-time';
  // First boot: legacy uses the frozen value
  const { ctx: ctx1, restore: r1 } = loadSigningContext({
    HANA_PASSWORD: 'old-pw', APP_REVISION: 'old-rev',
    RESULT_SIGNING_LEGACY_KEY: historicalSecret
  });
  let envelope;
  try {
    // The current kid is 'legacy' (no current key configured).
    envelope = ctx1.sign({ code: 'HIST', score: 80 });
  } finally { r1(); }
  // Second boot: HANA_PASSWORD has rotated, no legacy key set
  const { ctx: ctx2, restore: r2 } = loadSigningContext({
    HANA_PASSWORD: 'new-pw', APP_REVISION: 'new-rev'
  });
  try {
    // ctx2's legacy is the NEW derived secret (would be different)
    const derivedAfterRotation = crypto
      .createHash('sha256')
      .update(['new-pw', '', '', '', '', 'new-rev'].join('|'))
      .digest('hex');
    assert.notEqual(ctx2.keys.legacy, historicalSecret,
      'after rotation, the derived legacy secret has changed');
    // But the historical envelope doesn't verify under ctx2's
    // derived secret — it was signed with the frozen one.
    assert.equal(ctx2.verify(envelope), null,
      'without RESULT_SIGNING_LEGACY_KEY, post-rotation ctx cannot verify the historical envelope');
  } finally { r2(); }
  // Third boot: operator re-applies the frozen legacy key
  const { ctx: ctx3, restore: r3 } = loadSigningContext({
    HANA_PASSWORD: 'new-pw', APP_REVISION: 'new-rev',
    RESULT_SIGNING_LEGACY_KEY: historicalSecret
  });
  try {
    // Now ctx3 has the frozen legacy secret. The historical
    // envelope verifies.
    const matched = ctx3.verify(envelope);
    assert.ok(matched, 'frozen legacy secret preserves historical verification');
    assert.equal(matched.kid, 'legacy');
  } finally { r3(); }
});

test('legacy: kid in keyMap is the literal string "legacy"', () => {
  const { ctx, restore } = loadSigningContext({});
  try {
    assert.ok('legacy' in ctx.keys,
      'keys must always have a "legacy" slot');
    assert.equal(ctx.keys.legacy.length, 64,
      'legacy secret is 64-char hex (sha256 derived or explicit)');
  } finally { restore(); }
});
