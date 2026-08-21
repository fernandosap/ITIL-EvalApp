'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

test('browser OAuth stores its JWT server-side behind an opaque session cookie', () => {
  assert.match(server, /const _xsuaaBrowserSessions = new Map\(\)/);
  assert.match(server, /xsuaa_session=\$\{session\.id\}/);
  assert.doesNotMatch(server, /xsuaa_jwt=\$\{encodeURIComponent\(tokenResp\.accessToken\)\}/);
  assert.match(server, /getXsuaaBrowserSession\(cookies\['xsuaa_session'\]\)/);
});
