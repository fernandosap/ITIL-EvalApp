// XSUAA / OAuth helpers, shared between server.js and tests.
// Pure logic; no Express, no I/O. Safe to require from anywhere.
'use strict';

const crypto = require('node:crypto');

// ---------------------------------------------------------------------------
// VCAP_SERVICES parsing
// ---------------------------------------------------------------------------

// Read the XSUAA credentials from VCAP_SERVICES (set by Cloud Foundry when
// an XSUAA service is bound to the app). Returns null if no XSUAA binding
// is present (e.g. local dev). VCAP_SERVICES is a JSON string.
function readXsuaaFromVcap(vcapServices) {
  if (!vcapServices) return null;
  let parsed;
  try {
    parsed = typeof vcapServices === 'string' ? JSON.parse(vcapServices) : vcapServices;
  } catch (_e) {
    return null;
  }
  const bindings = parsed && parsed.xsuaa;
  if (!Array.isArray(bindings) || bindings.length === 0) return null;
  const creds = bindings[0].credentials;
  if (!creds) return null;
  return {
    clientid: creds.clientid,
    clientsecret: creds.clientsecret,
    url: creds.url,
    apiurl: creds.apiurl,
    identityzone: creds.identityzone,
    verificationkey: creds.verificationkey,
    xsappname: creds.xsappname,
    sburl: creds.sburl,
    uaadomain: creds.uaadomain
  };
}

// Convenience: read from process.env (use this in server.js, not the
// direct parameter). Tests call readXsuaaFromVcap with a mock value.
function getXsuaaConfig() {
  return readXsuaaFromVcap(process.env.VCAP_SERVICES);
}

// ---------------------------------------------------------------------------
// JWT parsing and verification
// ---------------------------------------------------------------------------

// Decode a base64url-encoded segment. Throws on malformed input.
function b64urlDecode(input) {
  const pad = '='.repeat((4 - (input.length % 4)) % 4);
  return Buffer.from(input.replace(/-/g, '+').replace(/_/g, '/') + pad, 'base64');
}

// Verify a JWT against an XSUAA verification key. RS256 only (XSUAA
// default). Returns the claims object on success, or null on any failure
// (bad signature, expired, wrong audience, etc.). Never throws.
function verifyXsuaaJwt(token, xsuaa, nowSeconds = Math.floor(Date.now() / 1000)) {
  if (typeof token !== 'string' || !token) return null;
  if (!xsuaa || !xsuaa.verificationkey) return null;
  const parts = token.split('.');
  if (parts.length !== 3) return null;
  const [headerB64, payloadB64, signatureB64] = parts;
  let header, payload, signatureBuf;
  try {
    header = JSON.parse(b64urlDecode(headerB64).toString('utf8'));
    payload = JSON.parse(b64urlDecode(payloadB64).toString('utf8'));
    signatureBuf = b64urlDecode(signatureB64);
  } catch (_e) {
    return null;
  }
  if (!header || header.alg !== 'RS256') return null;

  // Verify signature
  try {
    const verifier = crypto.createVerify('RSA-SHA256');
    verifier.update(headerB64 + '.' + payloadB64);
    verifier.end();
    const ok = verifier.verify(xsuaa.verificationkey, signatureBuf);
    if (!ok) return null;
  } catch (_e) {
    return null;
  }

  // Standard claims checks
  if (typeof payload.exp === 'number' && nowSeconds >= payload.exp) return null;
  if (typeof payload.nbf === 'number' && nowSeconds < payload.nbf) return null;
  if (xsuaa.xsappname && payload.aud) {
    // aud can be a string or array; check any match
    const auds = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
    if (!auds.includes(xsuaa.xsappname) && !auds.includes('*')) return null;
  }
  return payload;
}

// ---------------------------------------------------------------------------
// Scope -> role mapping
// ---------------------------------------------------------------------------

// XSUAA returns scopes as a space-separated string in the 'scope' claim.
// Each scope in our config is $XSAPPNAME.<role>. We map to internal roles
// by stripping the prefix and looking up the role in our permissions set.
const KNOWN_ROLES = new Set(['admin', 'manager', 'reviewer', 'content_editor']);

// Roles in priority order for the case where a token has multiple scopes.
// 'admin' wins (most permissive), then manager/reviewer/content_editor.
const ROLE_PRIORITY = ['admin', 'manager', 'reviewer', 'content_editor'];

// Map a scope array to the highest-priority known role. Returns null if
// the token has no recognized scopes.
function mapScopesToRole(scopes) {
  if (!Array.isArray(scopes) || scopes.length === 0) return null;
  const rolesPresent = new Set();
  for (const s of scopes) {
    if (typeof s !== 'string') continue;
    // Accept both '$XSAPPNAME.admin' and bare 'admin' (defensive)
    const parts = s.split('.');
    const last = parts[parts.length - 1];
    if (KNOWN_ROLES.has(last)) rolesPresent.add(last);
  }
  for (const role of ROLE_PRIORITY) {
    if (rolesPresent.has(role)) return role;
  }
  return null;
}

// Combined helper: extract the 'scope' string from a verified claims object
// and turn it into a role.
function roleFromClaims(claims) {
  if (!claims) return null;
  const raw = claims.scope || claims.scp;
  let scopes;
  if (Array.isArray(raw)) scopes = raw;
  else if (typeof raw === 'string') scopes = raw.split(/\s+/).filter(Boolean);
  else return null;
  return mapScopesToRole(scopes);
}

// ---------------------------------------------------------------------------
// OAuth 2.0 authorization-code flow helpers
// ---------------------------------------------------------------------------

// Build the XSUAA /oauth/authorize URL. Caller must pass a fully-qualified
// redirectUri that matches the one registered in xs-security.json (XSUAA
// checks it byte-for-byte).
//
//   xsuaa         { url, clientid } from readXsuaaFromVcap
//   redirectUri   e.g. 'https://app.example.com/oauth/callback'
//   state         random opaque string for CSRF protection
function buildAuthorizeUrl(xsuaa, redirectUri, state) {
  if (!xsuaa || !xsuaa.url || !xsuaa.clientid) {
    throw new Error('buildAuthorizeUrl: xsuaa config is incomplete');
  }
  if (!redirectUri || typeof redirectUri !== 'string') {
    throw new Error('buildAuthorizeUrl: redirectUri is required');
  }
  const params = new URLSearchParams({
    response_type: 'code',
    client_id: xsuaa.clientid,
    redirect_uri: redirectUri,
    state: state || ''
  });
  return `${xsuaa.url.replace(/\/$/, '')}/oauth/authorize?${params.toString()}`;
}

// Exchange an authorization code for an access token.
//
// Return shape (always an object — no more `null`):
//   { ok: true,  accessToken, expiresIn, tokenType, body }  // success
//   { ok: false, error: 'not_configured' | 'missing_code' | 'network' |
//                   'upstream', statusCode, errorDescription, body }
//   { ok: false, error: 'parse', body }                     // 2xx but bad JSON
//
// Pure: this function accepts an `executor` (a function that takes
// options, a body string, and a callback) so tests can inject a fake
// HTTP caller. The default executor uses the global `https` module.
// XSUAA's /oauth/token endpoint requires application/x-www-form-urlencoded
// with grant_type and HTTP Basic auth using clientid:clientsecret.
function exchangeCodeForToken(xsuaa, code, redirectUri, executor) {
  if (!xsuaa || !xsuaa.clientid || !xsuaa.clientsecret) {
    return Promise.resolve({ ok: false, error: 'not_configured' });
  }
  if (!code || typeof code !== 'string') {
    return Promise.resolve({ ok: false, error: 'missing_code' });
  }
  const url = new URL(`${xsuaa.url.replace(/\/$/, '')}/oauth/token`);
  const auth = Buffer.from(`${xsuaa.clientid}:${xsuaa.clientsecret}`).toString('base64');
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: redirectUri || ''
  }).toString();
  const opts = {
    method: 'POST',
    hostname: url.hostname,
    port: url.port || 443,
    path: url.pathname,
    headers: {
      'Authorization': `Basic ${auth}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body)
    }
  };
  const exec = executor || defaultHttpsPost;
  return new Promise((resolve) => {
    exec(opts, body, (err, statusCode, rawBody) => {
      if (err) return resolve({ ok: false, error: 'network', message: String(err.message || err) });
      if (statusCode < 200 || statusCode >= 300) {
        // XSUAA returns { error, error_description } on 4xx. Try to
        // parse and surface that — it's what makes "why did this fail"
        // debuggable in the logs.
        let errorDescription = null;
        try {
          const parsed = JSON.parse(rawBody);
          errorDescription = parsed && parsed.error_description;
        } catch (_e) { /* not JSON */ }
        return resolve({
          ok: false,
          error: 'upstream',
          statusCode,
          errorDescription,
          body: String(rawBody || '').slice(0, 500)
        });
      }
      let parsed;
      try { parsed = JSON.parse(rawBody); }
      catch (_e) { return resolve({ ok: false, error: 'parse', body: String(rawBody || '').slice(0, 500) }); }
      resolve({
        ok: true,
        accessToken: parsed.access_token,
        expiresIn: parsed.expires_in,
        tokenType: parsed.token_type,
        body: parsed
      });
    });
  });
}

// Default HTTPS executor using the global `https` module. Exposed for
// tests via a setter.
let _httpsImpl = null;
function _setHttpsForTests(mod) { _httpsImpl = mod; }
function defaultHttpsPost(opts, body, cb) {
  const https = _httpsImpl || require('https');
  const req = https.request(opts, (res) => {
    const chunks = [];
    res.on('data', (c) => chunks.push(c));
    res.on('end', () => cb(null, res.statusCode, Buffer.concat(chunks).toString('utf8')));
  });
  req.on('error', (err) => cb(err));
  req.write(body);
  req.end();
}

// Generate a random opaque state string for CSRF protection on the
// authorize redirect. 32 bytes of random data, base64url-encoded.
function generateState() {
  return crypto.randomBytes(32).toString('base64url');
}

// Parse a Cookie header into a plain object. No URL-encoding of values
// beyond the standard decodeURIComponent. Returns {} on missing/empty.
function parseCookieHeader(header) {
  const out = {};
  if (!header || typeof header !== 'string') return out;
  for (const part of header.split(';')) {
    const idx = part.indexOf('=');
    if (idx < 0) continue;
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    if (!k) continue;
    try { out[k] = decodeURIComponent(v); } catch (_e) { out[k] = v; }
  }
  return out;
}

module.exports = {
  readXsuaaFromVcap,
  getXsuaaConfig,
  verifyXsuaaJwt,
  mapScopesToRole,
  roleFromClaims,
  buildAuthorizeUrl,
  exchangeCodeForToken,
  generateState,
  parseCookieHeader,
  _setHttpsForTests,
  // Exported for tests
  KNOWN_ROLES,
  ROLE_PRIORITY
};
