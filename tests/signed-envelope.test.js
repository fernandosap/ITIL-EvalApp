'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

// Tests for the v2 envelope format + the runtime signing context.
// The runtime builds a SigningContext once (cached per env
// snapshot) and exposes sign()/signPayload()/buildSignedEnvelope().
// Envelopes are now v2 (version + algorithm + kid + payload all
// bound to the signature). The legacy v1 format (no version
// field, signature over payload only) is also tested because
// /verify-signature must still accept historical envelopes.

const { signPayload, buildSignedEnvelope } = require('../server.js');
const signing = require('../lib/signing-context.js');
const { buildSignedEnvelope: buildV2, verifySignedEnvelope, SIGNING_VERSION } = require('../lib/responses.js');

test('signPayload: deterministic for the same input + cached context', () => {
  signing._resetForTests();
  const sig1 = signPayload({ x: 1, y: 2 });
  const sig2 = signPayload({ x: 1, y: 2 });
  assert.equal(sig1, sig2);
});

test('signPayload: changes when payload changes', () => {
  signing._resetForTests();
  const sig1 = signPayload({ x: 1 });
  const sig2 = signPayload({ x: 2 });
  assert.notEqual(sig1, sig2);
});

test('signPayload: returns a 64-char hex string', () => {
  signing._resetForTests();
  const sig = signPayload({ x: 1 });
  assert.equal(typeof sig, 'string');
  assert.equal(sig.length, 64);
  assert.match(sig, /^[a-f0-9]{64}$/);
});

test('buildSignedEnvelope: returns v2 envelope with version + algorithm + kid', () => {
  signing._resetForTests();
  const payload = { code: 'ABC', generatedAt: '2026-08-18T00:00:00Z' };
  const envelope = buildSignedEnvelope(payload);
  assert.deepEqual(envelope.payload, payload);
  assert.equal(envelope.version, SIGNING_VERSION);
  assert.equal(envelope.algorithm, 'HMAC-SHA256');
  assert.equal(envelope.kid, 'legacy',
    'no operator config means the active kid is "legacy"');
  assert.equal(envelope.signature, signPayload(payload),
    'signPayload is just envelope.signature (extracted)');
});

test('buildSignedEnvelope: signature is over the v2 canonical (version+algorithm+kid+payload)', () => {
  signing._resetForTests();
  const payload = { foo: 'bar' };
  const envelope = buildSignedEnvelope(payload);
  // Build the same canonical the signer would have used:
  const expectedCanonical = JSON.stringify({
    version: SIGNING_VERSION,
    algorithm: envelope.algorithm,
    kid: envelope.kid,
    payload: envelope.payload
  });
  const ctx = signing.getSigningContext();
  const expectedSig = crypto
    .createHmac('sha256', ctx.keys[envelope.kid])
    .update(expectedCanonical)
    .digest('hex');
  assert.equal(envelope.signature, expectedSig,
    'v2 signature must be over the canonical envelope, not just payload');
});

test('buildSignedEnvelope: signature changes if you tamper with kid in the envelope (cannot verify)', () => {
  signing._resetForTests();
  const envelope = buildSignedEnvelope({ x: 1 });
  // Flip the kid in the canonical and recompute the signature
  // manually — the v2 signature should not match.
  const ctx = signing.getSigningContext();
  const tamperedCanonical = JSON.stringify({
    version: SIGNING_VERSION,
    algorithm: envelope.algorithm,
    kid: 'legacy',  // same as original, but if we change it...
    payload: envelope.payload
  });
  // Actually build a real tampered envelope: change kid to a
  // different value the verifier doesn't have. Use 'evilkid' which
  // is not in the keyMap.
  const realCanonical = JSON.stringify({
    version: SIGNING_VERSION,
    algorithm: envelope.algorithm,
    kid: envelope.kid,
    payload: envelope.payload
  });
  const realSig = crypto
    .createHmac('sha256', ctx.keys[envelope.kid])
    .update(realCanonical)
    .digest('hex');
  // Tamper: same signature, different kid in envelope
  const tampered = { ...envelope, kid: 'evilkid' };
  // The verifier tries 'evilkid' first (not in keyMap) then legacy.
  // Either way, no candidate with the right secret for 'evilkid'
  // exists, AND the canonical the verifier computes for the
  // 'legacy' candidate will use kid='legacy' (not 'evilkid'),
  // so the signature won't match the v2 hash.
  const matched = verifySignedEnvelope(tampered, ctx.keyMap);
  assert.equal(matched, null,
    'a v2 envelope with a tampered kid must not verify under any key');
});
