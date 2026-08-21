'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

test('global UI styles do not overlay decorative content across every screen', () => {
  assert.doesNotMatch(html, /body::before\s*\{/);
  assert.doesNotMatch(html, /body::after\s*\{/);
});

test('reading surfaces use opaque backgrounds', () => {
  assert.match(html, /--brand-panel:#fff;/);
  assert.match(html, /\.glass-card\{background:var\(--brand-panel\);/);
  assert.doesNotMatch(html, /\.glass-card\{[^}]*backdrop-filter/);
});
