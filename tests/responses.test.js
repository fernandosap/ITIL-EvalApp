'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  jsonError, jsonOk, toCsvCell, toCsvRow, parseJsonOrNull,
  parseAnthropicText, buildSignedEnvelope, verifySignedEnvelope, SIGNING_ALGORITHM
} = require('../lib/responses.js');

function makeRes() {
  const res = {
    _status: 0,
    _body: null,
    status(code) { this._status = code; return this; },
    json(body) { this._body = body; return this; }
  };
  return res;
}

test('jsonError: returns consistent shape with extra fields merged', () => {
  const res = makeRes();
  jsonError(res, 400, 'invalid_input', { field: 'code', hint: 'try again' });
  assert.equal(res._status, 400);
  assert.deepEqual(res._body, { ok: false, error: 'invalid_input', field: 'code', hint: 'try again' });
});

test('jsonError: works without extras', () => {
  const res = makeRes();
  jsonError(res, 500, 'boom');
  assert.equal(res._status, 500);
  assert.deepEqual(res._body, { ok: false, error: 'boom' });
});

test('jsonOk: returns ok:true with payload merged', () => {
  const res = makeRes();
  jsonOk(res, { token: 'abc', role: 'admin' });
  // jsonOk is a 200, so we don't set res.status. Just check the body.
  assert.deepEqual(res._body, { ok: true, token: 'abc', role: 'admin' });
});

test('jsonOk: works without payload', () => {
  const res = makeRes();
  jsonOk(res);
  assert.deepEqual(res._body, { ok: true });
});

test('toCsvCell: returns empty string for null/undefined', () => {
  assert.equal(toCsvCell(null), '');
  assert.equal(toCsvCell(undefined), '');
});

test('toCsvCell: wraps non-null values in double quotes', () => {
  assert.equal(toCsvCell('abc'), '"abc"');
  assert.equal(toCsvCell(42), '"42"');
  assert.equal(toCsvCell(true), '"true"');
});

test('toCsvCell: doubles internal quotes (Excel-compatible)', () => {
  assert.equal(toCsvCell('She said "hi"'), '"She said ""hi"""');
  assert.equal(toCsvCell('"'), '""""');
});

test('toCsvRow: joins cells with commas and applies toCsvCell to each', () => {
  assert.equal(toCsvRow(['a', 'b', 'c']), '"a","b","c"');
  assert.equal(toCsvRow([null, 'x', undefined]), ',"x",');
});

test('toCsvRow: returns "" for empty array', () => {
  assert.equal(toCsvRow([]), '');
  assert.equal(toCsvRow(null), '');
});

test('parseJsonOrNull: returns null for falsy input', () => {
  assert.equal(parseJsonOrNull(''), null);
  assert.equal(parseJsonOrNull(null), null);
  assert.equal(parseJsonOrNull(undefined), null);
  assert.equal(parseJsonOrNull(0), null);
});

test('parseJsonOrNull: returns parsed value on success', () => {
  assert.deepEqual(parseJsonOrNull('{"a":1}'), { a: 1 });
  assert.deepEqual(parseJsonOrNull('[1,2,3]'), [1, 2, 3]);
  assert.equal(parseJsonOrNull('"x"'), 'x');
  assert.equal(parseJsonOrNull('42'), 42);
});

test('parseJsonOrNull: returns null on invalid JSON (does not throw)', () => {
  assert.equal(parseJsonOrNull('not json'), null);
  assert.equal(parseJsonOrNull('{'), null);
  assert.equal(parseJsonOrNull('undefined'), null);
});

test('parseAnthropicText: returns "" for non-array input', () => {
  assert.equal(parseAnthropicText(null), '');
  assert.equal(parseAnthropicText(undefined), '');
  assert.equal(parseAnthropicText('string'), '');
  assert.equal(parseAnthropicText({}), '');
});

test('parseAnthropicText: joins text blocks with newlines', () => {
  const content = [
    { type: 'text', text: 'hello' },
    { type: 'text', text: 'world' }
  ];
  assert.equal(parseAnthropicText(content), 'hello\nworld');
});

test('parseAnthropicText: ignores non-text blocks', () => {
  const content = [
    { type: 'text', text: 'visible' },
    { type: 'tool_use', id: 'x', name: 'y' },
    { type: 'image', source: { type: 'base64' } }
  ];
  assert.equal(parseAnthropicText(content), 'visible');
});

test('parseAnthropicText: handles missing text field gracefully', () => {
  const content = [
    { type: 'text' },
    { type: 'text', text: 42 },
    { type: 'text', text: 'ok' }
  ];
  // Only text blocks with a string `text` are kept.
  assert.equal(parseAnthropicText(content), 'ok');
});

test('buildSignedEnvelope: throws without keyMap', () => {
  assert.throws(() => buildSignedEnvelope({ x: 1 }, null), /keyMap must have/);
  assert.throws(() => buildSignedEnvelope({ x: 1 }, {}), /keyMap must have/);
  assert.throws(() => buildSignedEnvelope({ x: 1 }, { current: 'v1' }), /keyMap must have/);
});

test('buildSignedEnvelope: throws when the current kid is missing from keys', () => {
  const km = { current: 'v9', keys: { v1: 's' } };
  assert.throws(() => buildSignedEnvelope({ x: 1 }, km), /v9.*is missing/);
});

test('buildSignedEnvelope: returns payload, signature, algorithm, kid', () => {
  const km = { current: 'v1', keys: { v1: 'mysecret' } };
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, km);
  assert.equal(env.algorithm, SIGNING_ALGORITHM);
  assert.equal(env.algorithm, 'HMAC-SHA256');
  assert.deepEqual(env.payload, { code: 'ABC', score: 80 });
  assert.equal(env.kid, 'v1');
  assert.match(env.signature, /^[a-f0-9]{64}$/);
});

test('buildSignedEnvelope: signature is deterministic for same payload + same key', () => {
  const km = { current: 'v1', keys: { v1: 's' } };
  const a = buildSignedEnvelope({ x: 1 }, km);
  const b = buildSignedEnvelope({ x: 1 }, km);
  assert.equal(a.signature, b.signature);
});

test('buildSignedEnvelope: signature differs for different keys', () => {
  const a = buildSignedEnvelope({ x: 1 }, { current: 'v1', keys: { v1: 's1' } });
  const b = buildSignedEnvelope({ x: 1 }, { current: 'v1', keys: { v1: 's2' } });
  assert.notEqual(a.signature, b.signature);
});

test('buildSignedEnvelope: uses the kid named in `current` for new envelopes', () => {
  const km = { current: 'v2', keys: { v1: 'old', v2: 'new' } };
  const env = buildSignedEnvelope({ x: 1 }, km);
  assert.equal(env.kid, 'v2');
});

test('verifySignedEnvelope: round-trip with matching current kid', () => {
  const km = { current: 'v1', keys: { v1: 'mysecret' } };
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, km);
  const matched = verifySignedEnvelope(env, km);
  assert.ok(matched);
  assert.equal(matched.kid, 'v1');
});

test('verifySignedEnvelope: round-trip with rotated key (v2 current, envelope v1)', () => {
  // After rotation: current=v2, v1 kept as a "previous" key.
  // Envelopes signed with v1 must still verify.
  const kmOld = { current: 'v1', keys: { v1: 'old-secret' } };
  const env = buildSignedEnvelope({ x: 1 }, kmOld);
  const kmNew = { current: 'v2', keys: { v1: 'old-secret', v2: 'new-secret' } };
  const matched = verifySignedEnvelope(env, kmNew);
  assert.ok(matched);
  assert.equal(matched.kid, 'v1', 'should fall back to v1 from the keyMap');
});

test('verifySignedEnvelope: round-trip via legacy slot for envelopes without kid', () => {
  // Historical envelopes predate the kid system. The legacy slot
  // catches them by checking with the derived-secret key.
  const km = { current: 'v1', keys: { v1: 'new-secret', legacy: 'legacy-derived' } };
  // Simulate an envelope with no kid — what legacy systems produced.
  const legacyEnv = {
    payload: { x: 1 },
    signature: require('crypto').createHmac('sha256', 'legacy-derived').update(JSON.stringify({ x: 1 })).digest('hex'),
    algorithm: 'HMAC-SHA256'
    // no kid
  };
  const matched = verifySignedEnvelope(legacyEnv, km);
  assert.ok(matched);
  assert.equal(matched.kid, 'legacy');
});

test('verifySignedEnvelope: detects tampered payload', () => {
  const km = { current: 'v1', keys: { v1: 'mysecret' } };
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, km);
  const tampered = { ...env, payload: { code: 'ABC', score: 100 } };
  assert.equal(verifySignedEnvelope(tampered, km), null);
});

test('verifySignedEnvelope: detects tampered signature', () => {
  const km = { current: 'v1', keys: { v1: 'mysecret' } };
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, km);
  const tampered = { ...env, signature: 'a'.repeat(64) };
  assert.equal(verifySignedEnvelope(tampered, km), null);
});

test('verifySignedEnvelope: rejects when no key in the map matches', () => {
  const env = buildSignedEnvelope({ x: 1 }, { current: 'v1', keys: { v1: 's1' } });
  // Now try to verify with a completely different keyMap (no v1, no legacy).
  const km = { current: 'v2', keys: { v2: 'something-else' } };
  assert.equal(verifySignedEnvelope(env, km), null);
});

test('verifySignedEnvelope: rejects malformed envelope', () => {
  const km = { current: 'v1', keys: { v1: 's' } };
  assert.equal(verifySignedEnvelope(null, km), null);
  assert.equal(verifySignedEnvelope(undefined, km), null);
  assert.equal(verifySignedEnvelope({}, km), null);
  assert.equal(verifySignedEnvelope({ payload: {}, signature: 'x' }, km), null); // wrong algorithm
  assert.equal(verifySignedEnvelope({ algorithm: SIGNING_ALGORITHM, signature: 'a'.repeat(64) }, km), null); // no payload
  assert.equal(verifySignedEnvelope({ payload: { x: 1 }, signature: 'a'.repeat(64), algorithm: SIGNING_ALGORITHM }, null), null); // no keyMap
});

test('verifySignedEnvelope: rejects signature of wrong length', () => {
  const km = { current: 'v1', keys: { v1: 's' } };
  const env = buildSignedEnvelope({ x: 1 }, km);
  const bad = { ...env, signature: 'short' };
  assert.equal(verifySignedEnvelope(bad, km), null);
});

test('verifySignedEnvelope: handles unknown kid (falls back to legacy)', () => {
  // An envelope claiming kid=v999, when v999 is not in the keyMap,
  // falls back to the legacy slot. Useful when an attacker copies
  // a valid envelope but changes the kid field; they still need to
  // match the legacy signature, which they can't forge.
  const km = { current: 'v1', keys: { v1: 's', legacy: 'l' } };
  const realEnv = buildSignedEnvelope({ x: 1 }, { current: 'v1', keys: { v1: 's' } });
  const tamperedKid = { ...realEnv, kid: 'v999' };
  // tamperedKid is NOT signed with v1 anymore (the signature was
  // computed with the v1 key but the verifier will try v999 first,
  // then legacy). Neither v999 nor legacy is the right key, so
  // verification fails.
  assert.equal(verifySignedEnvelope(tamperedKid, km), null);
});
