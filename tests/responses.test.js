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

test('buildSignedEnvelope: throws without secret', () => {
  assert.throws(() => buildSignedEnvelope({ x: 1 }, null), /secret is required/);
  assert.throws(() => buildSignedEnvelope({ x: 1 }, ''), /secret is required/);
});

test('buildSignedEnvelope: returns payload, signature, algorithm', () => {
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, 'mysecret');
  assert.equal(env.algorithm, SIGNING_ALGORITHM);
  assert.equal(env.algorithm, 'HMAC-SHA256');
  assert.deepEqual(env.payload, { code: 'ABC', score: 80 });
  assert.match(env.signature, /^[a-f0-9]{64}$/);
});

test('buildSignedEnvelope: signature is deterministic for same payload + secret', () => {
  const a = buildSignedEnvelope({ x: 1 }, 's');
  const b = buildSignedEnvelope({ x: 1 }, 's');
  assert.equal(a.signature, b.signature);
});

test('buildSignedEnvelope: signature differs for different secrets', () => {
  const a = buildSignedEnvelope({ x: 1 }, 's1');
  const b = buildSignedEnvelope({ x: 1 }, 's2');
  assert.notEqual(a.signature, b.signature);
});

test('verifySignedEnvelope: round-trip succeeds', () => {
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, 'mysecret');
  assert.equal(verifySignedEnvelope(env, 'mysecret'), true);
});

test('verifySignedEnvelope: detects tampered payload', () => {
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, 'mysecret');
  const tampered = { ...env, payload: { code: 'ABC', score: 100 } };
  assert.equal(verifySignedEnvelope(tampered, 'mysecret'), false);
});

test('verifySignedEnvelope: detects tampered signature', () => {
  const env = buildSignedEnvelope({ code: 'ABC', score: 80 }, 'mysecret');
  const tampered = { ...env, signature: 'a'.repeat(64) };
  assert.equal(verifySignedEnvelope(tampered, 'mysecret'), false);
});

test('verifySignedEnvelope: rejects wrong secret', () => {
  const env = buildSignedEnvelope({ x: 1 }, 'correct');
  assert.equal(verifySignedEnvelope(env, 'wrong'), false);
});

test('verifySignedEnvelope: rejects malformed envelope', () => {
  assert.equal(verifySignedEnvelope(null, 's'), false);
  assert.equal(verifySignedEnvelope(undefined, 's'), false);
  assert.equal(verifySignedEnvelope({}, 's'), false);
  assert.equal(verifySignedEnvelope({ payload: {}, signature: 'x' }, 's'), false); // wrong algorithm
  assert.equal(verifySignedEnvelope({ algorithm: SIGNING_ALGORITHM, signature: 'a'.repeat(64) }, 's'), false); // no payload
});

test('verifySignedEnvelope: rejects signature of wrong length', () => {
  const env = buildSignedEnvelope({ x: 1 }, 's');
  const bad = { ...env, signature: 'short' };
  assert.equal(verifySignedEnvelope(bad, 's'), false);
});

test('verifySignedEnvelope: rejects without secret', () => {
  const env = buildSignedEnvelope({ x: 1 }, 's');
  assert.equal(verifySignedEnvelope(env, null), false);
  assert.equal(verifySignedEnvelope(env, ''), false);
});
