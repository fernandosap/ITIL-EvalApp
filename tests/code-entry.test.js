'use strict';

// JSDOM-light tests for the candidate-flow helpers. We don't pull in
// jsdom as a dep — instead we load the UMD IIFE under a fake window +
// document so the file's side-effect wiring runs in isolation, then
// exercise the pure helpers it exposes via window.IE.codeEntry.__test__.
//
// These tests cover the buildSessionBanner helper specifically (the
// "Session secure · N questions · N min · N/X to pass" banner shown
// on the candidate landing). End-to-end rendering of showCodeEntry
// remains a manual smoke test against a live server.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function loadCodeEntryWithStubs() {
  // Build a fake window with the minimum surface the IIFE touches at
  // load time. We need stubs for IE.util (because the IIFE destructures
  // it on line 1) and IE.state (used inside showCodeEntry etc.). We
  // also expose `__test__.buildSessionBanner` so the unit tests can
  // hit it without going through the full showCodeEntry flow.
  const utilStub = {
    $: () => null,
    render: () => {},
    modal: () => {},
    brandLockup: () => '',
    apiJson: async () => null,
    _esc: (s) => String(s == null ? '' : s),
    normalizeExamTitle: (s) => String(s || ''),
    durationLabel: (s) => `${s}s`
  };
  const stateStub = {
    fetchStatus: async () => null,
    connectivityBanner: () => '',
    logIncident: () => {}
  };
  const win = {
    IE: { util: utilStub, state: stateStub },
    S: {},
    _adminToken: null,
    _adminRole: null,
    _adminAuthMethod: null
  };
  const sandbox = {
    window: win,
    globalThis: win,
    document: {
      getElementById: () => null,
      createElement: () => ({ setAttribute() {}, appendChild() {} })
    },
    console: { log: () => {}, warn: () => {}, error: () => {} }
  };
  vm.createContext(sandbox);
  const code = fs.readFileSync(path.join(__dirname, '..', 'client', 'code-entry.js'), 'utf8');
  vm.runInContext(code, sandbox);
  return sandbox.window.IE.codeEntry;
}

test('codeEntry module loads and exposes __test__.buildSessionBanner', () => {
  const codeEntry = loadCodeEntryWithStubs();
  assert.ok(codeEntry, 'codeEntry must be exported');
  assert.equal(typeof codeEntry.__test__, 'object');
  assert.equal(typeof codeEntry.__test__.buildSessionBanner, 'function');
});

test('buildSessionBanner: returns empty string for null/undefined status', () => {
  const ce = loadCodeEntryWithStubs();
  assert.equal(ce.__test__.buildSessionBanner(null), '');
  assert.equal(ce.__test__.buildSessionBanner(undefined), '');
  assert.equal(ce.__test__.buildSessionBanner(0), '');
});

test('buildSessionBanner: returns empty string when status has no usable fields', () => {
  const ce = loadCodeEntryWithStubs();
  assert.equal(ce.__test__.buildSessionBanner({}), '');
  assert.equal(ce.__test__.buildSessionBanner({ total: 0, durationSecs: 0, passScore: 0 }), '');
});

test('buildSessionBanner: full status renders all three items', () => {
  const ce = loadCodeEntryWithStubs();
  const out = ce.__test__.buildSessionBanner({
    total: 40,
    durationSecs: 45 * 60,
    passPct: 80,
    passScore: 32
  });
  assert.match(out, /class="session-banner"/);
  assert.match(out, /role="status" aria-live="polite"/);
  assert.match(out, /Session secure/);
  assert.match(out, /<strong>40<\/strong> questions/);
  assert.match(out, /<strong>45<\/strong> min/);
  assert.match(out, /<strong>32\/40<\/strong> to pass \(80%\)/);
  // Items separated by ·
  assert.match(out, /·/);
});

test('buildSessionBanner: only total renders just the questions item', () => {
  const ce = loadCodeEntryWithStubs();
  const out = ce.__test__.buildSessionBanner({ total: 12 });
  assert.match(out, /<strong>12<\/strong> questions/);
  assert.doesNotMatch(out, /min/);
  assert.doesNotMatch(out, /to pass/);
});

test('buildSessionBanner: passScore without total falls back to "?" in the denominator', () => {
  const ce = loadCodeEntryWithStubs();
  const out = ce.__test__.buildSessionBanner({ passScore: 24, passPct: 80 });
  assert.match(out, /<strong>24\/\?<\/strong> to pass/);
});

test('buildSessionBanner: rounds durationSecs to nearest minute', () => {
  const ce = loadCodeEntryWithStubs();
  // 45 min + 30 sec → 46 (Math.round(45.5) = 46)
  const out = ce.__test__.buildSessionBanner({ total: 10, durationSecs: 45 * 60 + 30 });
  assert.match(out, /<strong>46<\/strong> min/);
});

test('buildSessionBanner: tolerates string-typed numeric fields', () => {
  const ce = loadCodeEntryWithStubs();
  const out = ce.__test__.buildSessionBanner({
    total: '40',
    durationSecs: '2700',
    passPct: '80',
    passScore: '32'
  });
  assert.match(out, /<strong>40<\/strong> questions/);
  assert.match(out, /<strong>45<\/strong> min/);
  assert.match(out, /<strong>32\/40<\/strong> to pass/);
});

test('buildSessionBanner: zero passScore omits the pass item', () => {
  const ce = loadCodeEntryWithStubs();
  const out = ce.__test__.buildSessionBanner({ total: 10, durationSecs: 600, passScore: 0 });
  assert.match(out, /<strong>10<\/strong> questions/);
  assert.match(out, /<strong>10<\/strong> min/);
  assert.doesNotMatch(out, /to pass/);
});
