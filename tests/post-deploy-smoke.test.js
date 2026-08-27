'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { pathToFileURL } = require('node:url');

let smoke;
test.before(async () => {
  smoke = await import(pathToFileURL(path.join(__dirname, '..', 'scripts', 'post-deploy-smoke.mjs')).href);
});

function headers(values = {}) {
  const normalized = new Map(Object.entries(values).map(([k, v]) => [k.toLowerCase(), String(v)]));
  return { get(name) { return normalized.get(String(name).toLowerCase()) || null; } };
}

function response(body, options = {}) {
  return {
    ok: options.ok !== false,
    status: options.status || 200,
    headers: headers(options.headers || {}),
    async text() { return typeof body === 'string' ? body : JSON.stringify(body); },
    async json() { return typeof body === 'string' ? JSON.parse(body) : body; }
  };
}

test('normalizeBaseUrl requires https by default', () => {
  assert.equal(smoke.normalizeBaseUrl('https://example.test/'), 'https://example.test');
  assert.throws(() => smoke.normalizeBaseUrl('http://example.test'), /must use https/);
});

test('security header assertion rejects broad executable unsafe-inline', () => {
  const good = headers({
    'content-security-policy': "default-src 'self'; script-src 'self' 'sha256-demo'; frame-ancestors 'none'; object-src 'none'",
    'x-content-type-options': 'nosniff'
  });
  assert.doesNotThrow(() => smoke.assertSecurityHeaders(good));

  const bad = headers({
    'content-security-policy': "default-src 'self'; script-src 'self' 'unsafe-inline'; frame-ancestors 'none'; object-src 'none'",
    'x-content-type-options': 'nosniff'
  });
  assert.throws(() => smoke.assertSecurityHeaders(bad), /broad_script_unsafe_inline_detected/);
});

test('post-deploy smoke is non-destructive and validates core production surfaces', async () => {
  const calls = [];
  const fetchImpl = async (url, init = {}) => {
    calls.push({ url, method: init.method || 'GET' });
    if (url.endsWith('/')) return response('<html><body><div id="app"></div></body></html>', {
      headers: {
        'content-security-policy': "default-src 'self'; script-src 'self' 'sha256-demo'; frame-ancestors 'none'; object-src 'none'",
        'x-content-type-options': 'nosniff',
        'x-request-id': 'req-1'
      }
    });
    if (url.endsWith('/api/status')) return response({ examActive: true, examName: 'Exam' });
    if (url.endsWith('/client/main.js')) return response('window.IE = window.IE || {};');
    throw new Error(`unexpected:${url}`);
  };

  const result = await smoke.runSmoke({ baseUrl: 'https://example.test', fetchImpl });
  assert.equal(result.ok, true);
  assert.deepEqual(result.checks, ['landing', 'status', 'client-module']);
  assert.deepEqual(calls.map((c) => c.method), ['GET', 'GET', 'GET']);
});

test('optional smoke access code performs validation only, not session creation', async () => {
  const calls = [];
  const fetchImpl = async (url, init = {}) => {
    calls.push({ url, method: init.method || 'GET' });
    if (url.endsWith('/')) return response('<div id="app"></div>', {
      headers: {
        'content-security-policy': "default-src 'self'; script-src 'self'; frame-ancestors 'none'; object-src 'none'",
        'x-content-type-options': 'nosniff',
        'x-request-id': 'req-2'
      }
    });
    if (url.endsWith('/api/status')) return response({ examActive: true });
    if (url.endsWith('/client/main.js')) return response('IE');
    if (url.endsWith('/api/validate')) return response({ valid: true, status: 'unused' });
    throw new Error(`unexpected:${url}`);
  };

  await smoke.runSmoke({ baseUrl: 'https://example.test', accessCode: 'ABC234', fetchImpl });
  assert.ok(calls.some((c) => c.url.endsWith('/api/validate') && c.method === 'POST'));
  assert.equal(calls.some((c) => c.url.endsWith('/api/session/start')), false);
});
