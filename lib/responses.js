'use strict';

// Response + envelope helpers. Extracted from server.js so:
//   1. Routes use a consistent shape (jsonError / jsonOk) and an error
//      taxonomy is enforceable in one place.
//   2. The CSV cell formatter is testable in isolation (it does string
//      escaping that's easy to regress).
//   3. The signed-envelope helpers (build/verify) are testable without
//      a HANA connection or an Express server.

// ---------------------------------------------------------------------------
// JSON responses
// ---------------------------------------------------------------------------

function jsonError(res, status, error, extra) {
  const body = { ok: false, error };
  if (extra && typeof extra === 'object') Object.assign(body, extra);
  return res.status(status).json(body);
}

function jsonOk(res, payload) {
  const body = { ok: true };
  if (payload && typeof payload === 'object') Object.assign(body, payload);
  return res.json(body);
}

// ---------------------------------------------------------------------------
// CSV formatting (Excel-friendly: wrap each cell in quotes, escape inner
// quotes by doubling). Used by /api/admin/audit/export.json and
// /api/admin/export.csv.
// ---------------------------------------------------------------------------

function toCsvCell(v) {
  if (v === null || v === undefined) return '';
  return `"${String(v).replace(/"/g, '""')}"`;
}

function toCsvRow(values) {
  return (values || []).map(toCsvCell).join(',');
}

// ---------------------------------------------------------------------------
// Safe parse helpers
// ---------------------------------------------------------------------------

function parseJsonOrNull(s) {
  if (!s) return null;
  try {
    return JSON.parse(s);
  } catch (_e) {
    return null;
  }
}

// Pull the text out of an Anthropic messages API content array. The
// model is allowed to return interleaved thinking + text blocks; the
// proctor verdict is always in the last text block.
function parseAnthropicText(content) {
  if (!Array.isArray(content)) return '';
  return content
    .filter((c) => c && c.type === 'text' && typeof c.text === 'string')
    .map((c) => c.text)
    .join('\n')
    .trim();
}

// ---------------------------------------------------------------------------
// HMAC signed envelope (for tamper-evident result summaries).
// Envelope shape: { payload, signature, algorithm }.
// ---------------------------------------------------------------------------

const SIGNING_ALGORITHM = 'HMAC-SHA256';

function buildSignedEnvelope(payload, secret) {
  if (!secret) throw new Error('buildSignedEnvelope: secret is required');
  const json = JSON.stringify(payload);
  const signature = require('crypto')
    .createHmac('sha256', String(secret))
    .update(json)
    .digest('hex');
  return {
    payload,
    signature,
    algorithm: SIGNING_ALGORITHM
  };
}

function verifySignedEnvelope(envelope, secret) {
  if (!envelope || typeof envelope !== 'object') return false;
  if (!secret) return false;
  if (envelope.algorithm !== SIGNING_ALGORITHM) return false;
  if (typeof envelope.signature !== 'string' || !envelope.payload) return false;
  const expected = require('crypto')
    .createHmac('sha256', String(secret))
    .update(JSON.stringify(envelope.payload))
    .digest('hex');
  if (expected.length !== envelope.signature.length) return false;
  // timingSafeEqual throws if buffers are different lengths, so we
  // already guarded with the length check above.
  const a = Buffer.from(expected, 'hex');
  const b = Buffer.from(envelope.signature, 'hex');
  if (a.length !== b.length) return false;
  return require('crypto').timingSafeEqual(a, b);
}

module.exports = {
  jsonError,
  jsonOk,
  toCsvCell,
  toCsvRow,
  parseJsonOrNull,
  parseAnthropicText,
  buildSignedEnvelope,
  verifySignedEnvelope,
  SIGNING_ALGORITHM
};
