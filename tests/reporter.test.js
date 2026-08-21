'use strict';

// Tests for client/reporter.js — the fire-and-forget client
// error telemetry. This is the only client-side module with
// non-trivial logic worth unit-testing in isolation; the
// rest of the SPA renders DOM and is integration territory.
//
// We stub navigator.sendBeacon + fetch + Blob, then verify
// the right URL/method/body shape and the dedupe behavior.

const test = require('node:test');
const assert = require('node:assert/strict');

// ---- Test environment stubs ----
let lastBeacon = null;
let lastFetch = null;
let beaconCallCount = 0;
let fetchCallCount = 0;
class FakeBlob {
  constructor(parts, opts) { this.parts = parts; this.type = (opts && opts.type) || ''; }
}
function installBrowserStubs({ beaconFails = false } = {}) {
  lastBeacon = null;
  lastFetch = null;
  beaconCallCount = 0;
  fetchCallCount = 0;
  // Node 22 makes `navigator` a read-only getter on globalThis,
  // so a plain assignment throws. Use defineProperty to
  // override it for the duration of the test.
  Object.defineProperty(global, 'navigator', {
    value: {
      userAgent: 'StubUserAgent/1.0 (test)',
      sendBeacon: (url, blob) => {
        beaconCallCount += 1;
        if (beaconFails) return false;
        lastBeacon = { url, body: blob.parts.join(''), type: blob.type };
        return true;
      }
    },
    writable: true,
    configurable: true
  });
  global.Blob = FakeBlob;
  global.fetch = (url, opts) => {
    fetchCallCount += 1;
    lastFetch = { url, opts };
    return Promise.resolve({ ok: true, status: 200 });
  };
}
function clearBrowserStubs() {
  try {
    Object.defineProperty(global, 'navigator', {
      value: undefined, writable: true, configurable: true
    });
  } catch (_e) { /* ignore */ }
  delete global.Blob;
  delete global.fetch;
}

// ---- Reporter loading ----
// The reporter IIFE captures `root` at load time. To make the
// reporter see a specific S, we need to set up `global.window`
// BEFORE the require(), and re-require to get a fresh IIFE.
function loadReporterWithS(S, getDiagnosticSessionId) {
  const prevDescriptor = Object.getOwnPropertyDescriptor(global, 'window');
  Object.defineProperty(global, 'window', {
    value: {
      S: S || {},
      IE: getDiagnosticSessionId
        ? { state: { getDiagnosticSessionId } }
        : {}
    },
    writable: true,
    configurable: true
  });
  delete require.cache[require.resolve('../client/reporter.js')];
  const reporter = require('../client/reporter.js');
  return {
    reporter,
    restore() {
      if (prevDescriptor) {
        Object.defineProperty(global, 'window', prevDescriptor);
      } else {
        delete global.window;
      }
    }
  };
}

function withReporter(S, fn, getDiagnosticSessionId) {
  const { reporter, restore } = loadReporterWithS(S, getDiagnosticSessionId);
  try { return fn(reporter); } finally { restore(); }
}

// ---- Payload shape ----

test('reporter.buildPayload: includes type, message, filename, line, col, stack', () => {
  installBrowserStubs();
  withReporter({ screen: 'exam' }, (reporter) => {
    const p = reporter.buildPayload('error', {
      message: 'boom',
      filename: '/client/exam.js',
      line: 42,
      col: 7,
      stack: 'Error: boom\n    at renderQ (exam.js:42:7)'
    });
    assert.equal(p.type, 'error');
    assert.equal(p.message, 'boom');
    assert.equal(p.filename, '/client/exam.js');
    assert.equal(p.line, 42);
    assert.equal(p.col, 7);
    assert.ok(p.stack.includes('renderQ'));
    assert.equal(p.screen, 'exam');
  });
  clearBrowserStubs();
});

test('reporter.buildPayload: does NOT include the access code (it is a credential)', () => {
  // The exam access code is a credential. The reporter
  // must never include it in the diagnostic payload,
  // even if S.code is set. The admin dashboard correlates
  // errors via diagnosticSessionId (a random UUID per
  // tab) instead.
  installBrowserStubs();
  withReporter({ screen: 'results', code: 'ABC2DE' }, (reporter) => {
    const p = reporter.buildPayload('error', { message: 'x' });
    assert.equal(p.screen, 'results');
    assert.equal(p.accessCode, undefined,
      'payload must not include accessCode (credential leak)');
    // The code is also not in the message/stack/filename
    // (sanitization is server-side, but the browser
    // should not even ship it).
    assert.equal(JSON.stringify(p).includes('ABC2DE'), false,
      'raw access code must not appear anywhere in the payload');
  });
  clearBrowserStubs();
});

test('reporter.buildPayload: pulls diagnosticSessionId from S', () => {
  installBrowserStubs();
  withReporter({
    screen: 'results',
    code: 'ABC2DE',  // ignored
    diagnosticSessionId: 'sess-uuid-1234'
  }, (reporter) => {
    const p = reporter.buildPayload('error', { message: 'x' });
    assert.equal(p.diagnosticSessionId, 'sess-uuid-1234');
  });
  clearBrowserStubs();
});

test('reporter.buildPayload: gets distinct IDs for separate fresh sessions', () => {
  installBrowserStubs();
  let firstId;
  let secondId;
  withReporter({ screen: 'exam' }, (reporter) => {
    firstId = reporter.buildPayload('error', { message: 'one' }).diagnosticSessionId;
  }, () => 'session-first');
  withReporter({ screen: 'exam' }, (reporter) => {
    secondId = reporter.buildPayload('error', { message: 'two' }).diagnosticSessionId;
  }, () => 'session-second');
  assert.equal(firstId, 'session-first');
  assert.equal(secondId, 'session-second');
  assert.notEqual(firstId, secondId);
  clearBrowserStubs();
});

test('reporter.buildPayload: defaults line/col to 0 when non-numeric', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    const p = reporter.buildPayload('error', { message: 'x', line: 'oops', col: null });
    assert.equal(p.line, 0);
    assert.equal(p.col, 0);
  });
  clearBrowserStubs();
});

test('reporter.buildPayload: clientTs is an ISO timestamp', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    const p = reporter.buildPayload('error', { message: 'x' });
    assert.ok(/^\d{4}-\d{2}-\d{2}T/.test(p.clientTs), `clientTs must be ISO-like (got ${p.clientTs})`);
  });
  clearBrowserStubs();
});

test('reporter.buildPayload: includes userAgent from navigator', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    const p = reporter.buildPayload('error', { message: 'x' });
    assert.equal(p.userAgent, 'StubUserAgent/1.0 (test)');
  });
  clearBrowserStubs();
});

test('reporter.buildPayload: falls back to "unknown" screen when S has none', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    const p = reporter.buildPayload('error', { message: 'x' });
    assert.equal(p.screen, 'unknown');
  });
  clearBrowserStubs();
});

// ---- Transport ----

test('reporter.report: uses sendBeacon when available, with JSON Blob', () => {
  installBrowserStubs();
  withReporter({ screen: 'exam' }, (reporter) => {
    const sent = reporter.report('error', { message: 'hi', filename: 'f.js', line: 10 });
    assert.equal(sent, true);
    assert.equal(lastBeacon.url, '/api/client-errors');
    assert.equal(lastBeacon.type, 'application/json');
    const body = JSON.parse(lastBeacon.body);
    assert.equal(body.type, 'error');
    assert.equal(body.message, 'hi');
    assert.equal(body.filename, 'f.js');
    assert.equal(body.line, 10);
    assert.equal(body.screen, 'exam');
  });
  clearBrowserStubs();
});

test('reporter.report: falls back to fetch(keepalive) when sendBeacon returns false', () => {
  installBrowserStubs({ beaconFails: true });
  withReporter({}, (reporter) => {
    reporter.report('error', { message: 'beacon-fail', filename: 'f.js', line: 1 });
    assert.equal(beaconCallCount, 1, 'beacon was attempted once');
    assert.equal(fetchCallCount, 1, 'fetch fallback was used');
    assert.equal(lastFetch.url, '/api/client-errors');
    assert.equal(lastFetch.opts.method, 'POST');
    assert.equal(lastFetch.opts.keepalive, true);
    assert.equal(lastFetch.opts.headers['Content-Type'], 'application/json');
  });
  clearBrowserStubs();
});

test('reporter.report: never throws even if both sendBeacon and fetch throw', () => {
  installBrowserStubs();
  Object.defineProperty(global, 'navigator', {
    value: { userAgent: 'X', sendBeacon: () => { throw new Error('beacon broken'); } },
    writable: true, configurable: true
  });
  global.fetch = () => { throw new Error('fetch broken'); };
  withReporter({}, (reporter) => {
    assert.doesNotThrow(() => {
      reporter.report('error', { message: 'both broken' });
    });
  });
  clearBrowserStubs();
});

// ---- Dedupe ----

test('reporter.report: identical consecutive reports within 4s collapse to one', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    reporter.report('error', { message: 'same', filename: 'f.js', line: 1 });
    reporter.report('error', { message: 'same', filename: 'f.js', line: 1 });
    reporter.report('error', { message: 'same', filename: 'f.js', line: 1 });
    assert.equal(beaconCallCount, 1,
      '3 identical calls within 4s must collapse to 1 beacon (got ' + beaconCallCount + ')');
  });
  clearBrowserStubs();
});

test('reporter.report: different messages always pass dedupe', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    reporter.report('error', { message: 'one', filename: 'f.js', line: 1 });
    reporter.report('error', { message: 'two', filename: 'f.js', line: 1 });
    reporter.report('error', { message: 'three', filename: 'f.js', line: 1 });
    assert.equal(beaconCallCount, 3,
      '3 different messages must all send (got ' + beaconCallCount + ')');
  });
  clearBrowserStubs();
});

test('reporter.report: different lines pass dedupe even with same message', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    reporter.report('error', { message: 'same', filename: 'f.js', line: 1 });
    reporter.report('error', { message: 'same', filename: 'f.js', line: 2 });
    assert.equal(beaconCallCount, 2, 'different line numbers must produce 2 sends');
    assert.ok(lastBeacon.body.includes('"line":2'));
  });
  clearBrowserStubs();
});

test('reporter._resetDedup: clears the dedupe window so a re-send is possible', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    reporter.report('error', { message: 'same', filename: 'f.js', line: 1 });
    reporter.report('error', { message: 'same', filename: 'f.js', line: 1 });
    assert.equal(beaconCallCount, 1, 'second identical call must be deduped');
    reporter._resetDedup();
    reporter.report('error', { message: 'same', filename: 'f.js', line: 1 });
    assert.equal(beaconCallCount, 2, 'after _resetDedup the same call must send again');
  });
  clearBrowserStubs();
});

// ---- Type passthrough ----

test('reporter.report: passes the type field through (server is source of truth for whitelist)', () => {
  installBrowserStubs();
  withReporter({}, (reporter) => {
    reporter.report('unhandledrejection', { message: 'x' });
    const body = JSON.parse(lastBeacon.body);
    assert.equal(body.type, 'unhandledrejection');
  });
  clearBrowserStubs();
});
