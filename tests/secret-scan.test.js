'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { pathToFileURL } = require('node:url');

let scanner;
test.before(async () => {
  scanner = await import(pathToFileURL(path.join(__dirname, '..', 'scripts', 'secret-scan.mjs')).href);
});

test('secret scanner supports JavaScript whitespace escapes used by patterns', () => {
  const patterns = scanner.loadPatterns(String.raw`hana://[A-Za-z0-9_]+:[^@\s]+@`);
  assert.equal(patterns.length, 1);
  const validCandidate = 'hana://' + 'USER' + ':' + 'supersecret' + '@example.hana';
  const whitespaceCandidate = 'hana://' + 'USER' + ':' + 'not allowed' + '@example.hana';
  assert.equal(scanner.scanText(validCandidate, patterns).length, 1);
  assert.equal(scanner.scanText(whitespaceCandidate, patterns).length, 0);
});

test('secret scanner ignores comments and blank pattern lines', () => {
  const patterns = scanner.loadPatterns('# comment\n\nAKIA[0-9A-Z]{16}\n');
  assert.equal(patterns.length, 1);
  const candidate = 'AKIA' + '1234567890ABCDEF';
  assert.equal(scanner.scanText(candidate, patterns).length, 1);
});

test('line-level secretscan opt-out suppresses an intentional false positive', () => {
  const patterns = scanner.loadPatterns('sk-[A-Za-z0-9]{20,}\n');
  const value = 'sk-' + 'abcdefghijklmnopqrstuvwxyz' + ' # secretscan: ok';
  assert.equal(scanner.scanText(value, patterns).length, 0);
});

test('staged diff parser preserves file and added-line numbers', () => {
  const diff = [
    'diff --git a/demo.txt b/demo.txt',
    '--- a/demo.txt',
    '+++ b/demo.txt',
    '@@ -2,0 +3,2 @@',
    '+first added',
    '+second added'
  ].join('\n');
  assert.deepEqual(scanner.parseAddedLines(diff), [
    { file: 'demo.txt', line: 3, text: 'first added' },
    { file: 'demo.txt', line: 4, text: 'second added' }
  ]);
});

test('invalid regex patterns fail closed instead of silently weakening scanning', () => {
  assert.throws(() => scanner.loadPatterns('[unterminated'), /invalid_secret_pattern/);
});
