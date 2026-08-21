/* eslint-disable no-console */
// client/reporter.js — fire-and-forget client error telemetry.
//
// Extracted from util.js so it can be unit-tested without
// pulling in the full SPA surface (apiFetch, $modal, etc.).
// Loaded as a UMD IIFE alongside the other client modules.
//
// Public surface (attached to window.IE.reporter):
//   report(type, info)             -- build payload + send
//   buildPayload(type, info, ctx)  -- pure helper, exported for tests
//   _resetDedup()                  -- test hook
//
// The function NEVER throws and NEVER blocks. Safe to call
// from window.onerror / unhandledrejection / <script onerror>
// / the bootstrap try/catch.
//
// Transport:
//   1. navigator.sendBeacon('/api/client-errors', Blob) if
//      available — preferred because it survives page unload
//      and runs off the main thread.
//   2. fetch(..., { keepalive: true }) as fallback.
//
// Local dedupe:
//   Identical consecutive reports (same type + message +
//   filename + line) within 4s collapse to a single send.
//   Prevents a tight error loop (e.g. ReferenceError on
//   every render frame) from burning the server's 30/min/IP
//   rate limit before distinct errors can get through.

(function (root) {
  var _lastReportKey = '';
  var _lastReportAt = 0;
  var _dedupWindowMs = 4000;

  // Pure helper. Builds the sanitized payload. Exported so
  // tests can verify the shape without mocking the network.
  //
  // IMPORTANT: we deliberately do NOT include the exam
  // access code in the payload. The access code is a
  // credential (anyone with it can sit the exam under
  // another candidate's name) and it has no place in
  // anonymous diagnostic telemetry. For per-candidate
  // correlation, the admin can join by (timestamp, IP) or
  // use the diagnosticSessionId below — which is a random
  // UUID, not a secret.
  function buildPayload(type, info, ctx) {
    info = info || {};
    ctx = ctx || {};
    var S = ctx.S || (root && root.S) || {};
    return {
      type: type,
      message: (info.message == null ? '' : String(info.message)).slice(0, 240),
      filename: (info.filename == null ? '' : String(info.filename)).slice(0, 200),
      line: Number.isFinite(info.line) ? info.line : 0,
      col: Number.isFinite(info.col) ? info.col : 0,
      stack: (info.stack == null ? '' : String(info.stack)).slice(0, 1200),
      screen: S.screen ? String(S.screen) : 'unknown',
      lastAction: S.lastAction ? String(S.lastAction) : '',
      // Non-secret per-browser-session correlation ID. Read
      // from sessionStorage if available; otherwise the
      // caller (state.js) is expected to have placed one
      // on the S object. Falls back to a derived value
      // based on the userAgent (so different browsers on
      // the same machine still get distinct IDs) but that
      // is best-effort, not for security purposes.
      diagnosticSessionId: S.diagnosticSessionId
        ? String(S.diagnosticSessionId)
        : ((typeof navigator !== 'undefined' && navigator.userAgent) ? 'ua-' + String(navigator.userAgent).slice(0, 32) : 'unknown'),
      clientTs: new Date().toISOString(),
      userAgent: (ctx.userAgent != null ? ctx.userAgent :
                  (typeof navigator !== 'undefined' ? navigator.userAgent : '')) || ''
    };
  }

  // The signature key for dedupe. We only use 4 fields to
  // keep false-positives low; an empty message counts as a
  // distinct event from an empty filename (the line number
  // usually disambiguates).
  function _signatureKey(p) {
    return (p.type || '') + '|' + (p.message || '') + '|' + (p.filename || '') + '|' + p.line;
  }

  function report(type, info) {
    try {
      var payload = buildPayload(type, info, root && root.S ? { S: root.S } : {});
      var key = _signatureKey(payload);
      var now = Date.now();
      if (key === _lastReportKey && now - _lastReportAt < _dedupWindowMs) return false;
      _lastReportKey = key;
      _lastReportAt = now;
      var body = JSON.stringify(payload);
      var sent = false;
      if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
        try {
          sent = navigator.sendBeacon('/api/client-errors', new Blob([body], { type: 'application/json' }));
        } catch (_e) { sent = false; }
      }
      if (!sent && typeof fetch === 'function') {
        try {
          fetch('/api/client-errors', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: body,
            keepalive: true
          });
        } catch (_e) { /* ignore */ }
      }
      return true;
    } catch (_e) {
      // never throw from a reporter
      return false;
    }
  }

  function _resetDedup() {
    _lastReportKey = '';
    _lastReportAt = 0;
  }

  var api = {
    report: report,
    buildPayload: buildPayload,
    _resetDedup: _resetDedup
  };

  root.IE = root.IE || {};
  root.IE.reporter = api;

  // Also expose via CommonJS exports for unit tests in Node.
  // The browser side just ignores `module` (it's undefined in
  // a UMD/IIFE without a require() context).
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }
})(typeof window !== 'undefined' ? window : globalThis);
