'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

// Tests for the v2 envelope format + the runtime signing context.
// Envelopes are now v2 (version + algorithm + kid + payload all
// bound to the signature). The legacy v1 format (no version field,
// signature over payload only) is also tested because
// /verify-signature must still accept historical envelopes.
//
// IMPORTANT: `buildSignedEnvelope` here is imported from
// `lib/responses.js`, not from `server.js`. The server's wrapper
// goes through the cached SigningContext (which only knows about
// the operator's current env vars), so it's wrong for protocol
// tests that need to construct an envelope against a specific
// keyMap.

const { signPayload } = require('../server.js');
const signing = require('../lib/signing-context.js');
const {
  buildSignedEnvelope,
  verifySignedEnvelope,
  SIGNING_VERSION
} = require('../lib/responses.js');

const TEST_KEYMAP_V2 = {
  current: 'v1',
  keys: Object.assign(Object.create(null), {
    v1: 'somerandomsecret-32chars-xxxxxxxxxxx',
    legacy: 'legacy-secret-32chars-xxxxxxxxxxx'
  })
};

test('buildSignedEnvelope: returns v2 envelope with version + algorithm + kid', () => {
  signing._resetForTests();
  const payload = { code: 'ABC', generatedAt: '2026-08-18T00:00:00Z' };
  const envelope = buildSignedEnvelope(payload, TEST_KEYMAP_V2);
  assert.deepEqual(envelope.payload, payload);
  assert.equal(envelope.version, SIGNING_VERSION);
  assert.equal(envelope.algorithm, 'HMAC-SHA256');
  assert.equal(envelope.kid, 'v1');
  // v2: signature is over the canonical envelope, not just the
  // payload. Reconstruct the canonical and check the HMAC.
  const expectedCanonical = JSON.stringify({
    version: SIGNING_VERSION,
    algorithm: envelope.algorithm,
    kid: envelope.kid,
    payload: envelope.payload
  });
  const expectedSig = crypto
    .createHmac('sha256', TEST_KEYMAP_V2.keys[envelope.kid])
    .update(expectedCanonical)
    .digest('hex');
  assert.equal(envelope.signature, expectedSig);
  // And the legacy path (signPayload/signLegacyPayload) produces
  // a DIFFERENT signature (payload-only HMAC, not canonical).
  assert.notEqual(envelope.signature, signPayload(payload),
    'v2 envelope.signature is canonical; signPayload is payload-only legacy');
});

test('buildSignedEnvelope: signature is over the v2 canonical (version+algorithm+kid+payload)', () => {
  signing._resetForTests();
  const payload = { foo: 'bar' };
  const envelope = buildSignedEnvelope(payload, TEST_KEYMAP_V2);
  const expectedCanonical = JSON.stringify({
    version: SIGNING_VERSION,
    algorithm: envelope.algorithm,
    kid: envelope.kid,
    payload: envelope.payload
  });
  const expectedSig = crypto
    .createHmac('sha256', TEST_KEYMAP_V2.keys[envelope.kid])
    .update(expectedCanonical)
    .digest('hex');
  assert.equal(envelope.signature, expectedSig);
});

test('buildSignedEnvelope: tampered kid in envelope does not verify', () => {
  signing._resetForTests();
  const envelope = buildSignedEnvelope({ x: 1 }, TEST_KEYMAP_V2);
  // Tamper: same signature, different kid in envelope
  const tampered = { ...envelope, kid: 'evilkid' };
  // The verifier tries 'evilkid' (not in keyMap); v2 doesn't apply
  // the legacy fallback, so the result is null.
  const matched = verifySignedEnvelope(tampered, TEST_KEYMAP_V2);
  assert.equal(matched, null);
});

// ---------------------------------------------------------------------------
// v2 protocol hardening: kid-authoritative + strict version dispatch
// (regression tests for the bugs found in the v2 review pass)
// ---------------------------------------------------------------------------

function makeKeyMapWithCurrent() {
  return {
    current: 'v2',
    keys: Object.assign(Object.create(null), {
      v2: 'current-secret-32chars-xxxxxxxxxxxx',
      v1: 'previous-secret-32chars-xxxxxxxxxx',
      legacy: 'legacy-secret-32chars-xxxxxxxxxxx'
    })
  };
}

test('v2: kid-authoritative — v2 envelope signed with legacy but claiming kid=v2 must NOT verify', () => {
  // Bug 1 from the review: the previous verifier always added the
  // legacy key as a fallback candidate, which let a v2 envelope
  // claiming kid=v2 verify under the legacy secret. The strict v2
  // verifier must reject this.
  const km = makeKeyMapWithCurrent();
  const evil = {
    version: 2,
    algorithm: 'HMAC-SHA256',
    kid: 'v2',
    payload: { score: 100, code: 'X' }
  };
  const canonical = JSON.stringify({
    version: 2, algorithm: 'HMAC-SHA256', kid: 'v2', payload: evil.payload
  });
  evil.signature = crypto
    .createHmac('sha256', 'legacy-secret-32chars-xxxxxxxxxxx')
    .update(canonical)
    .digest('hex');
  assert.equal(verifySignedEnvelope(evil, km), null,
    'v2 envelope signed with legacy secret but claiming kid=v2 must not verify');
});

test('v2: kid-authoritative — v2 envelope signed with v2 verifies under v2 (positive control)', () => {
  const km = makeKeyMapWithCurrent();
  const env = buildSignedEnvelope({ score: 80 }, km);
  const matched = verifySignedEnvelope(env, km);
  assert.ok(matched);
  assert.equal(matched.kid, 'v2',
    'v2 envelope verifies under its own kid, not legacy');
});

test('v1 (no version field) still gets the legacy fallback', () => {
  // v1 envelopes (signed before v2 was introduced) didn't have
  // a version field. They must STILL verify under the legacy
  // fallback. The strict-dispatch change only affects v2+.
  const km = makeKeyMapWithCurrent();
  const v1 = {
    algorithm: 'HMAC-SHA256',
    payload: { code: 'OLD', score: 70 }
  };
  v1.signature = crypto
    .createHmac('sha256', 'legacy-secret-32chars-xxxxxxxxxxx')
    .update(JSON.stringify(v1.payload))
    .digest('hex');
  const matched = verifySignedEnvelope(v1, km);
  assert.ok(matched, 'v1 envelope with no version must verify via legacy fallback');
  assert.equal(matched.kid, 'legacy');
});

test('strict dispatch: version=3 (unknown) is rejected (not v1)', () => {
  const km = makeKeyMapWithCurrent();
  const env = {
    version: 3,
    algorithm: 'HMAC-SHA256',
    payload: { x: 1 }
  };
  env.signature = crypto
    .createHmac('sha256', km.keys.legacy)
    .update(JSON.stringify(env.payload))
    .digest('hex');
  assert.equal(verifySignedEnvelope(env, km), null,
    'version=3 must be rejected (not silently treated as v1)');
});

test('strict dispatch: version=1 (declared) is rejected (only undefined = v1)', () => {
  const km = makeKeyMapWithCurrent();
  const env = {
    version: 1,
    algorithm: 'HMAC-SHA256',
    payload: { x: 1 }
  };
  env.signature = crypto
    .createHmac('sha256', km.keys.legacy)
    .update(JSON.stringify(env.payload))
    .digest('hex');
  assert.equal(verifySignedEnvelope(env, km), null,
    'explicit version=1 is not the same as missing version; reject');
});

test('strict dispatch: version="banana" is rejected', () => {
  const km = makeKeyMapWithCurrent();
  const env = {
    version: 'banana',
    algorithm: 'HMAC-SHA256',
    payload: { x: 1 }
  };
  env.signature = crypto
    .createHmac('sha256', km.keys.legacy)
    .update(JSON.stringify(env.payload))
    .digest('hex');
  assert.equal(verifySignedEnvelope(env, km), null);
});

test('v2: missing version on a v2-signed envelope is rejected (not v1)', () => {
  // An envelope was built as v2 (canonical includes version: 2)
  // but somebody stripped the version field. The verifier must
  // NOT fall back to v1 + payload-only — that would let the
  // canonical binding be defeated.
  const km = makeKeyMapWithCurrent();
  const env = buildSignedEnvelope({ x: 1 }, km);
  // eslint-disable-next-line no-unused-vars
  const { version, ...stripped } = env;
  assert.equal(verifySignedEnvelope(stripped, km), null,
    'a v2 envelope with version stripped must NOT verify as v1');
});

test('signLegacyPayload: produces a payload-only HMAC (not v2 canonical)', () => {
  signing._resetForTests();
  const ctx = signing.getSigningContext();
  const payload = { x: 1 };
  // signLegacyPayload gives just the hex string (raw HMAC of
  // payload, no envelope, no version).
  const sig = ctx.signLegacyPayload(payload);
  assert.match(sig, /^[a-f0-9]{64}$/);
  const expected = crypto
    .createHmac('sha256', ctx.keys[ctx.currentKid])
    .update(JSON.stringify(payload))
    .digest('hex');
  assert.equal(sig, expected,
    'signLegacyPayload must produce a payload-only HMAC, not the v2 canonical signature');
  // The legacy signature must NOT match the v2 envelope's
  // signature (which signs the canonical envelope).
  const env = ctx.sign(payload);
  assert.notEqual(sig, env.signature,
    'signLegacyPayload is the payload-only path; v2 envelope signature is the canonical path');
});

test('legacy /verify-signature endpoint contract: payload + raw sig verifies via signLegacyPayload', () => {
  signing._resetForTests();
  const ctx = signing.getSigningContext();
  const payload = { code: 'X', score: 50 };
  // Legacy /verify-signature contract: { payload, signature }
  // pair, no envelope, no version. The endpoint computes
  // expected = signPayload(payload) and compares. signPayload now
  // delegates to signLegacyPayload.
  const signature = ctx.signLegacyPayload(payload);
  // Same key, same payload, same algorithm: server.js's
  // signPayload (now a wrapper around signLegacyPayload) must
  // produce the same hex.
  const expected = signPayload(payload);
  assert.equal(signature, expected,
    'signPayload (legacy) and signLegacyPayload must produce the same payload-only HMAC');
});
