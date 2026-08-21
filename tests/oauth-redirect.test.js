'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

test('OAuth callback returns to the admin bootstrap route', () => {
  const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
  assert.match(server, /res\.redirect\(302, '\/\?admin=1&auth=ok'\)/);
});
