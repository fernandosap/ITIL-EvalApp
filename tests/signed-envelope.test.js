'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

// signPayload and buildSignedEnvelope are exported from server.js.
// They use getSigningSecret() which derives a secret from env vars.
// We snapshot the env, set the values we want to test, and restore.

const { signPayload, buildSignedEnvelope, getSigningSecret } = require('../server.js');

// Use the real getSigningSecret from server.js to derive the same secret
// the production code uses. (Mutating process.env does not affect the
// HANA_PASSWORD/ADMIN_HASH/... consts that server.js captured at load
// time; the only way to test against the real secret is to call the
// function the production code uses.)
const currentSecret = getSigningSecret();

test('signPayload: deterministic for the same input + secret', () => {
  const sig1 = signPayload({ x: 1, y: 2 });
  const sig2 = signPayload({ x: 1, y: 2 });
  assert.equal(sig1, sig2);
});

test('signPayload: changes when payload changes', () => {
  const sig1 = signPayload({ x: 1 });
  const sig2 = signPayload({ x: 2 });
  assert.notEqual(sig1, sig2);
});

test('signPayload: matches a manual HMAC-SHA256 of the canonical JSON', () => {
  const payload = { code: 'ABC123', score: 90, pct: 95 };
  const sig = signPayload(payload);
  const expectedSig = crypto
    .createHmac('sha256', currentSecret)
    .update(JSON.stringify(payload))
    .digest('hex');
  assert.equal(sig, expectedSig);
});

test('signPayload: returns a 64-char hex string', () => {
  const sig = signPayload({ x: 1 });
  assert.equal(typeof sig, 'string');
  assert.equal(sig.length, 64);
  assert.match(sig, /^[a-f0-9]{64}$/);
});

test('buildSignedEnvelope: returns { payload, signature, algorithm }', () => {
  const payload = { code: 'ABC', generatedAt: '2026-08-18T00:00:00Z' };
  const envelope = buildSignedEnvelope(payload);
  assert.deepEqual(envelope.payload, payload);
  assert.equal(envelope.algorithm, 'HMAC-SHA256');
  assert.equal(envelope.signature, signPayload(payload));
});

test('buildSignedEnvelope: signature is verifiable with the same secret', () => {
  const payload = { foo: 'bar' };
  const env1 = buildSignedEnvelope(payload);
  const expectedSig = crypto
    .createHmac('sha256', currentSecret)
    .update(JSON.stringify(payload))
    .digest('hex');
  assert.equal(env1.signature, expectedSig);
});
