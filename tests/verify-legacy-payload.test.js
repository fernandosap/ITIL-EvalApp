'use strict';

// Tests for verifyLegacyPayload() — the rotation-safe counterpart
// to signLegacyPayload().
//
// Background: the legacy /api/admin/results/verify-signature
// endpoint accepts a { payload, signature } pair where signature
// is HMAC(secret, JSON.stringify(payload)) (the v1 contract, no
// envelope, no version). The endpoint used to recompute the
// signature with the current key only, which broke historical
// signatures after a real key rotation.
//
// verifyLegacyPayload() instead walks the full key ring
// (current → previous → legacy) and returns the first matching
// kid. This file exercises that behavior, with an explicit
// rotation scenario to lock in the regression fix.

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const {
  verifyLegacyPayload
} = require('../lib/responses.js');

const SECRET_CURRENT  = 'a'.repeat(32) + 'current-padding-xxxxxxxxxx';
const SECRET_PREVIOUS = 'b'.repeat(32) + 'previous-padding-xxxxxxxx';
const SECRET_LEGACY   = 'c'.repeat(32) + 'legacy-padding-xxxxxxxxxx';

function hmacHex(secret, input) {
  return crypto.createHmac('sha256', secret).update(input).digest('hex');
}

function makeKeyMap(current, extra) {
  const keys = Object.assign(Object.create(null), { legacy: SECRET_LEGACY }, extra || {});
  return { current, keys };
}

test('verifyLegacyPayload: matches the current key', () => {
  const payload = { code: 'ABC123', score: 95 };
  const sig = hmacHex(SECRET_CURRENT, JSON.stringify(payload));
  const km = makeKeyMap('v2', { v2: SECRET_CURRENT });
  const matched = verifyLegacyPayload(payload, sig, km);
  assert.ok(matched);
  assert.equal(matched.kid, 'v2');
});

test('verifyLegacyPayload: matches the previous key (rotation scenario)', () => {
  // The endpoint verifies a historical signature that was produced
  // under the PREVIOUS key. After rotation, the new current key
  // is different, but the previous one is still in the keyMap.
  const payload = { code: 'XYZ789', score: 72, ts: '2026-08-01' };
  const sig = hmacHex(SECRET_PREVIOUS, JSON.stringify(payload));
  const km = makeKeyMap('v2', {
    v2: SECRET_CURRENT,
    v1: SECRET_PREVIOUS
  });
  const matched = verifyLegacyPayload(payload, sig, km);
  assert.ok(matched, 'historical signature under previous key must verify');
  assert.equal(matched.kid, 'v1',
    'matched kid should be the previous key id');
});

test('verifyLegacyPayload: matches the legacy slot when no current/previous match', () => {
  // Legacy-derived configuration (no operator key set): current
  // is 'legacy' and the legacy slot holds the derived secret.
  // A raw signature produced under the legacy derived secret
  // must still verify.
  const payload = { code: 'LGC001', score: 60 };
  const sig = hmacHex(SECRET_LEGACY, JSON.stringify(payload));
  const km = makeKeyMap('legacy'); // current = 'legacy' fallback
  const matched = verifyLegacyPayload(payload, sig, km);
  assert.ok(matched);
  assert.equal(matched.kid, 'legacy');
});

test('verifyLegacyPayload: returns null when no key in the ring matches', () => {
  const payload = { code: 'NOPE01', score: 50 };
  const sig = hmacHex('totally-different-secret-32chars-xxxxxx', JSON.stringify(payload));
  const km = makeKeyMap('v2', { v2: SECRET_CURRENT, v1: SECRET_PREVIOUS });
  const matched = verifyLegacyPayload(payload, sig, km);
  assert.equal(matched, null);
});

test('verifyLegacyPayload: rejects null/missing keyMap', () => {
  const payload = { code: 'ABC' };
  const sig = hmacHex(SECRET_CURRENT, JSON.stringify(payload));
  assert.equal(verifyLegacyPayload(payload, sig, null), null);
  assert.equal(verifyLegacyPayload(payload, sig, undefined), null);
  assert.equal(verifyLegacyPayload(payload, sig, {}), null);
  assert.equal(verifyLegacyPayload(payload, sig, { keys: null }), null);
});

test('verifyLegacyPayload: rejects missing or malformed signature', () => {
  const payload = { code: 'ABC' };
  const km = makeKeyMap('v2', { v2: SECRET_CURRENT });
  assert.equal(verifyLegacyPayload(payload, '', km), null);
  assert.equal(verifyLegacyPayload(payload, null, km), null);
  assert.equal(verifyLegacyPayload(payload, undefined, km), null);
  assert.equal(verifyLegacyPayload(payload, 12345, km), null);
});

test('verifyLegacyPayload: rejects undefined payload', () => {
  const km = makeKeyMap('v2', { v2: SECRET_CURRENT });
  const sig = hmacHex(SECRET_CURRENT, 'undefined');
  assert.equal(verifyLegacyPayload(undefined, sig, km), null);
});

test('verifyLegacyPayload: still verifies when payload contains nested objects', () => {
  // JSON.stringify is order-deterministic in modern Node for
  // objects with string keys inserted in declaration order, but
  // we construct the payload identically in both places so the
  // round-trip is reliable.
  const payload = {
    code: 'COMPLEX',
    score: 88,
    nested: { a: 1, b: [2, 3, { c: 4 }] },
    flags: ['a', 'b']
  };
  const sig = hmacHex(SECRET_CURRENT, JSON.stringify(payload));
  const km = makeKeyMap('v2', { v2: SECRET_CURRENT });
  const matched = verifyLegacyPayload(payload, sig, km);
  assert.ok(matched);
  assert.equal(matched.kid, 'v2');
});

test('verifyLegacyPayload: end-to-end rotation — historical payload signed under legacy, then current rotates', () => {
  // 1) Operator starts with no signing key configured. Legacy
  //    slot holds the derived secret; current = 'legacy'.
  // 2) Operator captures a historical payload signature under
  //    the legacy derived secret.
  // 3) Operator configures a dedicated current key and freezes
  //    the legacy secret in RESULT_SIGNING_LEGACY_KEY so the
  //    historical signature keeps verifying.
  // 4) A new verify-signature call comes in with the historical
  //    payload+signature; verifyLegacyPayload must match it
  //    against the legacy slot (not the new current).
  const historicalPayload = { code: 'HIST01', score: 70, ts: '2026-07-01' };
  const historicalSig = hmacHex(SECRET_LEGACY, JSON.stringify(historicalPayload));
  // Post-rotation keyMap: current = 'v2', keys = { v2: NEW, legacy: FROZEN_LEGACY }
  const km = makeKeyMap('v2', { v2: SECRET_CURRENT });
  const matched = verifyLegacyPayload(historicalPayload, historicalSig, km);
  assert.ok(matched,
    'historical legacy-derived signature must still verify under frozen RESULT_SIGNING_LEGACY_KEY');
  assert.equal(matched.kid, 'legacy',
    'matched kid must be the literal "legacy" slot, not the new current key');
});

test('verifyLegacyPayload: end-to-end rotation — payload signed under previous still verifies after promotion', () => {
  // 1) Operator has v1 current.
  // 2) A raw payload is signed under v1.
  // 3) Operator rotates: v2 becomes current, v1 becomes previous.
  // 4) The historical raw signature must still verify under v1
  //    (now the previous key).
  const historicalPayload = { code: 'PROMO1', score: 81 };
  const sigUnderV1 = hmacHex(SECRET_PREVIOUS, JSON.stringify(historicalPayload));
  // Post-promotion keyMap: v2 is current, v1 is previous.
  const km = makeKeyMap('v2', {
    v2: SECRET_CURRENT,
    v1: SECRET_PREVIOUS
  });
  const matched = verifyLegacyPayload(historicalPayload, sigUnderV1, km);
  assert.ok(matched, 'promoted-to-previous signature must still verify');
  assert.equal(matched.kid, 'v1');
});

test('verifyLegacyPayload: does not double-try the same key when current and legacy are equal', () => {
  // Pathological config: the operator set RESULT_SIGNING_LEGACY_KEY
  // to the SAME value as the current key. The dedup Set should
  // make this harmless — one try, one match, return current.
  const sharedSecret = 'a'.repeat(64);
  const payload = { code: 'DEDUP1', score: 50 };
  const sig = hmacHex(sharedSecret, JSON.stringify(payload));
  const km = {
    current: 'v2',
    keys: Object.assign(Object.create(null), {
      v2: sharedSecret,
      legacy: sharedSecret
    })
  };
  const matched = verifyLegacyPayload(payload, sig, km);
  assert.ok(matched);
  // Either kid is acceptable; the dedup means we return the first
  // one tried, which is current.
  assert.equal(matched.kid, 'v2');
});

test('verifyLegacyPayload: timing-safe — different signatures on same payload all return null consistently', () => {
  // Sanity: wrong signature returns null even with valid keyMap.
  const payload = { code: 'TIMING', score: 50 };
  const km = makeKeyMap('v2', { v2: SECRET_CURRENT });
  assert.equal(verifyLegacyPayload(payload, 'a'.repeat(64), km), null);
  assert.equal(verifyLegacyPayload(payload, '0'.repeat(64), km), null);
  // Truncated signature (timing-safe equality should still
  // return null without throwing).
  assert.equal(verifyLegacyPayload(payload, 'short', km), null);
});
