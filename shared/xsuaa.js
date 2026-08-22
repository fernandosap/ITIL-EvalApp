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

// Inspect a JWT against an XSUAA verification key. The reason is safe for
// structured logs: it describes only which server-side validation failed,
// never token contents or user claims.
function inspectXsuaaJwt(token, xsuaa, nowSeconds = Math.floor(Date.now() / 1000)) {
  if (typeof token !== 'string' || !token) return { claims: null, reason: 'missing_token' };
  if (!xsuaa || !xsuaa.verificationkey) return { claims: null, reason: 'missing_verification_key' };
  const parts = token.split('.');
  if (parts.length !== 3) return { claims: null, reason: 'malformed_token' };
  const [headerB64, payloadB64, signatureB64] = parts;
  let header, payload, signatureBuf;
  try {
    header = JSON.parse(b64urlDecode(headerB64).toString('utf8'));
    payload = JSON.parse(b64urlDecode(payloadB64).toString('utf8'));
    signatureBuf = b64urlDecode(signatureB64);
  } catch (_e) {
    return { claims: null, reason: 'malformed_token' };
  }
  if (!header || header.alg !== 'RS256') return { claims: null, reason: 'unsupported_algorithm' };

  // Verify signature
  try {
    const verifier = crypto.createVerify('RSA-SHA256');
    verifier.update(headerB64 + '.' + payloadB64);
    verifier.end();
    const ok = verifier.verify(xsuaa.verificationkey, signatureBuf);
    if (!ok) return { claims: null, reason: 'invalid_signature' };
  } catch (_e) {
    return { claims: null, reason: 'invalid_signature' };
  }

  // Standard claims checks
  if (typeof payload.exp === 'number' && nowSeconds >= payload.exp) return { claims: null, reason: 'expired' };
  if (typeof payload.nbf === 'number' && nowSeconds < payload.nbf) return { claims: null, reason: 'not_yet_valid' };
  // Audience: REQUIRED (do not accept tokens that omit aud). aud can
  // be a string or array; we accept any match. The wildcard '*' is
  // honored for tokens issued by a tenant without an xsappname set
  // (e.g. a test IdP) but is uncommon in production XSUAA tokens.
  if (!xsuaa.xsappname) return { claims: null, reason: 'missing_xsappname' };
  if (!payload.aud) return { claims: null, reason: 'missing_audience' };
  const auds = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  if (!auds.includes(xsuaa.xsappname) && !auds.includes('*')) return { claims: null, reason: 'audience_mismatch' };
  // Issuer: REQUIRED when the XSUAA config exposes one. XSUAA issues
  // tokens with `iss` set to the tenant URL (e.g.
  // "https://<tenant>.authentication.us10.hana.ondemand.com"). Without
  // an explicit iss check, a token signed by some other XSUAA tenant
  // that happens to share the same xsappname + verification key (e.g.
  // during a key rotation or in a misconfigured sub-account) would be
  // accepted. XSUAA deployments use either the tenant URL or its
  // canonical /oauth/token endpoint as the iss value.
  if (xsuaa.url) {
    const expectedIss = String(xsuaa.url).replace(/\/+$/, '');
    const iss = String(payload.iss || '').replace(/\/+$/, '');
    const validIssuers = new Set([expectedIss, `${expectedIss}/oauth/token`]);
    if (!validIssuers.has(iss)) return { claims: null, reason: 'issuer_mismatch' };
  }
  return { claims: payload, reason: null };
}

// Verify a JWT against an XSUAA verification key. RS256 only (XSUAA
// default). Returns the claims object on success, or null on any failure.
function verifyXsuaaJwt(token, xsuaa, nowSeconds = Math.floor(Date.now() / 1000)) {
  return inspectXsuaaJwt(token, xsuaa, nowSeconds).claims;
}

// ---------------------------------------------------------------------------
// Scope -> role mapping
// ---------------------------------------------------------------------------

const KNOWN_ROLES = new Set(['admin', 'manager', 'reviewer', 'content_editor']);
const ROLE_PRIORITY = ['admin', 'manager', 'reviewer', 'content_editor'];

// Map scopes to the highest-priority known role. When xsappname is supplied
// (the production path), only exact `${xsappname}.<role>` scopes are accepted.
// This prevents a token containing an unrelated `some-other-app.admin` scope
// from being treated as an admin for this application. Bare-role matching is
// retained only for unit tests/local helpers where no XSUAA binding exists.
function mapScopesToRole(scopes, xsappname = null) {
  if (!Array.isArray(scopes) || scopes.length === 0) return null;
  const rolesPresent = new Set();
  const expectedPrefix = xsappname ? `${String(xsappname)}.` : null;
  for (const scope of scopes) {
    if (typeof scope !== 'string') continue;
    if (expectedPrefix) {
      if (!scope.startsWith(expectedPrefix)) continue;
      const role = scope.slice(expectedPrefix.length);
      if (KNOWN_ROLES.has(role)) rolesPresent.add(role);
      continue;
    }
    const parts = scope.split('.');
    const role = parts[parts.length - 1];
    if (KNOWN_ROLES.has(role)) rolesPresent.add(role);
  }
  for (const role of ROLE_PRIORITY) {
    if (rolesPresent.has(role)) return role;
  }
  return null;
}

// Combined helper: extract scope/scp and turn it into an internal role.
// In production we derive the expected xsappname from the bound XSUAA
// credentials automatically, so existing server.js call sites remain strict
// without needing to pass configuration through every layer.
function roleFromClaims(claims, xsappname = null) {
  if (!claims) return null;
  const raw = claims.scope || claims.scp;
  let scopes;
  if (Array.isArray(raw)) scopes = raw;
  else if (typeof raw === 'string') scopes = raw.split(/\s+/).filter(Boolean);
  else return null;
  const configuredXsappname = xsappname || getXsuaaConfig()?.xsappname || null;
  return mapScopesToRole(scopes, configuredXsappname);
}

// ---------------------------------------------------------------------------
// OAuth 2.0 authorization-code flow helpers
// ---------------------------------------------------------------------------

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

function generateState() {
  return crypto.randomBytes(32).toString('base64url');
}

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
  inspectXsuaaJwt,
  verifyXsuaaJwt,
  mapScopesToRole,
  roleFromClaims,
  buildAuthorizeUrl,
  exchangeCodeForToken,
  generateState,
  parseCookieHeader,
  _setHttpsForTests,
  KNOWN_ROLES,
  ROLE_PRIORITY
};
