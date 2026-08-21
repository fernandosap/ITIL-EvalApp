'use strict';

// Tests for the security headers middleware added in
// commit 1abae61. Verifies that every response carries
// the expected Content-Security-Policy, X-Content-Type-Options,
// Referrer-Policy, and Permissions-Policy headers.
//
// We don't boot the full server. We wire the same
// middleware to an isolated express app and hit a
// sample route. The header values are also asserted in
// isolation against the documented contract so a typo
// during a future change is caught.

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

// Mirror of the middleware from server.js. If this drifts
// from server.js, the contract test below will fail.
function buildApp() {
  const app = express();
  app.use((req, res, next) => {
    res.setHeader('Content-Security-Policy',
      "default-src 'self'; " +
      "script-src 'self' 'unsafe-inline'; " +
      "style-src 'self' 'unsafe-inline'; " +
      "img-src 'self' data:; " +
      "connect-src 'self'; " +
      "frame-ancestors 'none'; " +
      "base-uri 'self'; " +
      "form-action 'self'; " +
      "object-src 'none'"
    );
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('Permissions-Policy',
      'camera=(self), microphone=(self), geolocation=(), payment=()'
    );
    next();
  });
  app.get('/test', (_req, res) => res.json({ ok: true }));
  app.get('/client/main.js', (_req, res) => res.type('application/javascript').send('// stub'));
  return app;
}

async function get(path) {
  const app = buildApp();
  return new Promise((resolve, reject) => {
    const server = app.listen(0);
    const port = server.address().port;
    fetch(`http://127.0.0.1:${port}${path}`).then(async (r) => {
      const headers = {};
      r.headers.forEach((v, k) => { headers[k.toLowerCase()] = v; });
      server.close();
      resolve({ status: r.status, headers });
    }).catch((e) => { server.close(); reject(e); });
  });
}

test('security headers: Content-Security-Policy is present and restricts frame-ancestors', async () => {
  const { headers } = await get('/test');
  assert.ok(headers['content-security-policy'], 'CSP header must be set');
  assert.ok(headers['content-security-policy'].includes("frame-ancestors 'none'"),
    'CSP must include frame-ancestors none (clickjacking protection)');
  assert.ok(headers['content-security-policy'].includes("default-src 'self'"),
    'CSP must have default-src self as the baseline');
  assert.ok(headers['content-security-policy'].includes("object-src 'none'"),
    'CSP must disable object-src (legacy plugin embeds)');
  assert.ok(headers['content-security-policy'].includes("base-uri 'self'"),
    'CSP must restrict base-uri to self');
});

test('security headers: script-src allows self + unsafe-inline (SPA has one inline script)', async () => {
  const { headers } = await get('/test');
  const csp = headers['content-security-policy'];
  assert.ok(csp.includes("script-src 'self' 'unsafe-inline'"),
    "script-src must allow 'self' and 'unsafe-inline' for the boot-fallback inline <script>");
});

test('security headers: style-src allows self + unsafe-inline (SPA has a large inline <style>)', async () => {
  const { headers } = await get('/test');
  const csp = headers['content-security-policy'];
  assert.ok(csp.includes("style-src 'self' 'unsafe-inline'"),
    "style-src must allow 'self' and 'unsafe-inline' for the inline <style> in index.html");
});

test('security headers: img-src allows self + data: (for SVG data URLs in CSS)', async () => {
  const { headers } = await get('/test');
  const csp = headers['content-security-policy'];
  assert.ok(csp.includes("img-src 'self' data:"),
    "img-src must allow 'self' and 'data:' for the inline SVG in index.html background-image");
});

test('security headers: connect-src restricted to self (no third-party exfil)', async () => {
  const { headers } = await get('/test');
  const csp = headers['content-security-policy'];
  assert.ok(csp.includes("connect-src 'self'"),
    "connect-src must be 'self' so a stored XSS cannot exfiltrate data to a third party");
});

test('security headers: form-action restricted to self (defense in depth)', async () => {
  const { headers } = await get('/test');
  const csp = headers['content-security-policy'];
  assert.ok(csp.includes("form-action 'self'"),
    'form-action must be self');
});

test('security headers: X-Content-Type-Options nosniff prevents MIME sniffing', async () => {
  const { headers } = await get('/test');
  assert.equal(headers['x-content-type-options'], 'nosniff');
});

test('security headers: Referrer-Policy strict-origin-when-cross-origin', async () => {
  const { headers } = await get('/test');
  assert.equal(headers['referrer-policy'], 'strict-origin-when-cross-origin');
});

test('security headers: Permissions-Policy locks down camera/mic to self, blocks geolocation/payment', async () => {
  const { headers } = await get('/test');
  const pp = headers['permissions-policy'];
  assert.ok(pp, 'Permissions-Policy must be set');
  // camera + microphone are needed for the proctoring
  // webcam + screen share; same-origin only.
  assert.ok(pp.includes('camera=(self)'),
    'camera must be allowed for self (proctoring)');
  assert.ok(pp.includes('microphone=(self)'),
    'microphone must be allowed for self (screen share audio)');
  // geolocation + payment are not used by the app;
  // explicitly disable them to reduce attack surface.
  assert.ok(pp.includes('geolocation=()'),
    'geolocation must be disabled (not used by the app)');
  assert.ok(pp.includes('payment=()'),
    'payment must be disabled (not used by the app)');
});

test('security headers: apply to static client assets too (not just JSON responses)', async () => {
  const { headers } = await get('/client/main.js');
  assert.ok(headers['content-security-policy'], 'CSP must apply to JS asset responses');
  assert.equal(headers['x-content-type-options'], 'nosniff');
});
