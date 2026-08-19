'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const {
  readXsuaaFromVcap,
  verifyXsuaaJwt,
  mapScopesToRole,
  roleFromClaims,
  KNOWN_ROLES,
  ROLE_PRIORITY
} = require('../shared/xsuaa.js');

// Helper: build a minimal valid XSUAA VCAP_SERVICES payload
function makeVcap(overrides = {}) {
  return JSON.stringify({
    xsuaa: [
      {
        credentials: {
          clientid: 'sb-foo!t1',
          clientsecret: 'secret',
          url: 'https://sapacademy.authentication.us10.hana.ondemand.com',
          apiurl: 'https://api.authentication.us10.hana.ondemand.com',
          identityzone: 'sapacademy',
          xsappname: 'academy-cf-cs-itil4-evalapp!t1',
          verificationkey: '-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtest\n-----END PUBLIC KEY-----',
          ...overrides
        }
      }
    ]
  });
}

// Helper: generate a test key pair (RS256) and return both keys + sign()
function makeKeyPair() {
  const { publicKey, privateKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' }
  });
  return { publicKey, privateKey };
}

// Helper: base64url-encode a Buffer (or string)
function b64url(buf) {
  return Buffer.from(buf).toString('base64')
    .replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
}

// Helper: sign a JWT with the given private key and claims
function signJwt(privateKey, claims, { alg = 'RS256', header = {} } = {}) {
  const fullHeader = { alg, typ: 'JWT', ...header };
  const headerB64 = b64url(JSON.stringify(fullHeader));
  const payloadB64 = b64url(JSON.stringify(claims));
  const data = `${headerB64}.${payloadB64}`;
  const sig = crypto.createSign('RSA-SHA256').update(data).sign(privateKey);
  return `${data}.${b64url(sig)}`;
}

// ---------------------------------------------------------------------------
// readXsuaaFromVcap
// ---------------------------------------------------------------------------

test('readXsuaaFromVcap: returns null when no VCAP_SERVICES', () => {
  assert.equal(readXsuaaFromVcap(undefined), null);
  assert.equal(readXsuaaFromVcap(''), null);
  assert.equal(readXsuaaFromVcap('not-json'), null);
  assert.equal(readXsuaaFromVcap('{}'), null);
  assert.equal(readXsuaaFromVcap('{"xsuaa": []}'), null);
});

test('readXsuaaFromVcap: extracts the first xsuaa binding', () => {
  const cfg = readXsuaaFromVcap(makeVcap());
  assert.equal(cfg.clientid, 'sb-foo!t1');
  assert.equal(cfg.xsappname, 'academy-cf-cs-itil4-evalapp!t1');
  assert.ok(cfg.verificationkey.startsWith('-----BEGIN PUBLIC KEY-----'));
});

test('readXsuaaFromVcap: accepts an already-parsed object', () => {
  const vcap = JSON.parse(makeVcap());
  const cfg = readXsuaaFromVcap(vcap);
  assert.equal(cfg.clientid, 'sb-foo!t1');
});

// ---------------------------------------------------------------------------
// mapScopesToRole
// ---------------------------------------------------------------------------

test('mapScopesToRole: returns the highest-priority role for a multi-scope token', () => {
  assert.equal(mapScopesToRole(['admin', 'manager']), 'admin');
  assert.equal(mapScopesToRole(['manager', 'content_editor']), 'manager');
  assert.equal(mapScopesToRole(['reviewer', 'content_editor']), 'reviewer');
});

test('mapScopesToRole: handles prefixed scopes (XSAPPNAME.role)', () => {
  assert.equal(
    mapScopesToRole(['academy-cf-cs-itil4-evalapp.admin']),
    'admin'
  );
});

test('mapScopesToRole: returns null for empty or unknown scopes', () => {
  assert.equal(mapScopesToRole([]), null);
  assert.equal(mapScopesToRole(['unknown_role']), null);
  assert.equal(mapScopesToRole(null), null);
  assert.equal(mapScopesToRole(undefined), null);
});

test('mapScopesToRole: ignores non-string entries', () => {
  assert.equal(mapScopesToRole([null, 42, 'admin', { foo: 'bar' }]), 'admin');
});

// ---------------------------------------------------------------------------
// roleFromClaims
// ---------------------------------------------------------------------------

test('roleFromClaims: extracts role from a string "scope" claim', () => {
  assert.equal(roleFromClaims({ scope: 'admin codes:read' }), 'admin');
  assert.equal(roleFromClaims({ scope: 'manager' }), 'manager');
});

test('roleFromClaims: accepts an array "scope" claim', () => {
  assert.equal(roleFromClaims({ scope: ['admin', 'manager'] }), 'admin');
});

test('roleFromClaims: returns null on bad claims', () => {
  assert.equal(roleFromClaims(null), null);
  assert.equal(roleFromClaims({}), null);
  assert.equal(roleFromClaims({ scope: '' }), null);
});

// ---------------------------------------------------------------------------
// verifyXsuaaJwt
// ---------------------------------------------------------------------------

test('verifyXsuaaJwt: returns claims for a valid RS256 token', () => {
  const { publicKey, privateKey } = makeKeyPair();
  const xsuaa = { verificationkey: publicKey, xsappname: 'app!t1' };
  const now = Math.floor(Date.now() / 1000);
  const token = signJwt(privateKey, {
    iss: 'https://sapacademy.authentication.us10.hana.ondemand.com',
    aud: 'app!t1',
    sub: 'user-1',
    exp: now + 3600,
    iat: now,
    scope: 'app!t1.admin app!t1.manager'
  });
  const claims = verifyXsuaaJwt(token, xsuaa, now);
  assert.ok(claims);
  assert.equal(claims.sub, 'user-1');
  assert.equal(claims.aud, 'app!t1');
  assert.equal(roleFromClaims(claims), 'admin');
});

test('verifyXsuaaJwt: rejects a token with the wrong signature', () => {
  const { privateKey: a } = makeKeyPair();
  const { publicKey: b } = makeKeyPair();
  const xsuaa = { verificationkey: b, xsappname: 'app!t1' };
  const now = Math.floor(Date.now() / 1000);
  const token = signJwt(a, { exp: now + 3600, aud: 'app!t1', scope: 'app!t1.admin' });
  assert.equal(verifyXsuaaJwt(token, xsuaa, now), null);
});

test('verifyXsuaaJwt: rejects an expired token', () => {
  const { publicKey, privateKey } = makeKeyPair();
  const xsuaa = { verificationkey: publicKey, xsappname: 'app!t1' };
  const now = Math.floor(Date.now() / 1000);
  const token = signJwt(privateKey, { exp: now - 60, aud: 'app!t1', scope: 'app!t1.admin' });
  assert.equal(verifyXsuaaJwt(token, xsuaa, now), null);
});

test('verifyXsuaaJwt: rejects a token with not-yet-valid nbf', () => {
  const { publicKey, privateKey } = makeKeyPair();
  const xsuaa = { verificationkey: publicKey, xsappname: 'app!t1' };
  const now = Math.floor(Date.now() / 1000);
  const token = signJwt(privateKey, { nbf: now + 600, exp: now + 3600, aud: 'app!t1' });
  assert.equal(verifyXsuaaJwt(token, xsuaa, now), null);
});

test('verifyXsuaaJwt: rejects a token with wrong audience', () => {
  const { publicKey, privateKey } = makeKeyPair();
  const xsuaa = { verificationkey: publicKey, xsappname: 'app!t1' };
  const now = Math.floor(Date.now() / 1000);
  const token = signJwt(privateKey, { exp: now + 3600, aud: 'some-other-app!t1' });
  assert.equal(verifyXsuaaJwt(token, xsuaa, now), null);
});

test('verifyXsuaaJwt: rejects an HS256 (symmetric) token', () => {
  const xsuaa = { verificationkey: 'FAKE', xsappname: 'app!t1' };
  // Manually craft an HS256 token (symmetric). Even if signature is "right",
  // the alg check should reject.
  const header = b64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const now = Math.floor(Date.now() / 1000);
  const payload = b64url(JSON.stringify({ exp: now + 3600, aud: 'app!t1', scope: 'app!t1.admin' }));
  const data = `${header}.${payload}`;
  const sig = b64url(crypto.createHmac('sha256', 'shared-secret').update(data).digest());
  const token = `${data}.${sig}`;
  assert.equal(verifyXsuaaJwt(token, xsuaa, now), null);
});

test('verifyXsuaaJwt: rejects malformed tokens (does not throw)', () => {
  const xsuaa = { verificationkey: 'whatever', xsappname: 'app!t1' };
  assert.equal(verifyXsuaaJwt('', xsuaa), null);
  assert.equal(verifyXsuaaJwt('one.two', xsuaa), null);
  assert.equal(verifyXsuaaJwt('a.b.c.d', xsuaa), null);
  assert.equal(verifyXsuaaJwt('not.a.jwt', xsuaa), null);
  assert.equal(verifyXsuaaJwt(null, xsuaa), null);
});

test('verifyXsuaaJwt: accepts a token with aud=array including xsappname', () => {
  const { publicKey, privateKey } = makeKeyPair();
  const xsuaa = { verificationkey: publicKey, xsappname: 'app!t1' };
  const now = Math.floor(Date.now() / 1000);
  const token = signJwt(privateKey, {
    exp: now + 3600,
    aud: ['other-app!t1', 'app!t1', 'yet-another'],
    scope: 'app!t1.admin'
  });
  const claims = verifyXsuaaJwt(token, xsuaa, now);
  assert.ok(claims);
  assert.deepEqual(claims.aud, ['other-app!t1', 'app!t1', 'yet-another']);
});

// ---------------------------------------------------------------------------
// End-to-end: sign with a real key, verify with the public key
// ---------------------------------------------------------------------------

test('end-to-end: real RSA-2048 sign + verify end-to-end', () => {
  const { publicKey, privateKey } = makeKeyPair();
  const xsuaa = readXsuaaFromVcap(makeVcap({ verificationkey: publicKey, xsappname: 'academy-cf-cs-itil4-evalapp!t11367' }));
  const now = Math.floor(Date.now() / 1000);
  const token = signJwt(privateKey, {
    iss: 'https://sapacademy.authentication.us10.hana.ondemand.com',
    aud: 'academy-cf-cs-itil4-evalapp!t11367',
    sub: 'fernando.sanchez@sap.com',
    exp: now + 3600,
    iat: now,
    scope: 'academy-cf-cs-itil4-evalapp!t11367.admin academy-cf-cs-itil4-evalapp!t11367.manager'
  });
  const claims = verifyXsuaaJwt(token, xsuaa, now);
  assert.ok(claims);
  assert.equal(claims.sub, 'fernando.sanchez@sap.com');
  // The scope strip should give us 'admin' (highest priority)
  assert.equal(roleFromClaims(claims), 'admin');
});
