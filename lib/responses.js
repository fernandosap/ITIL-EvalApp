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
//
// Envelope shape: { payload, signature, algorithm, kid }.
// `kid` is a key identifier. The verifier tries every key in the
// `keyMap` (current + previous) until one validates the signature.
// That allows graceful rotation: when a key is compromised, the
// operator adds a new `current` key, marks the old one as
// `previous`, deploys, and existing envelopes still verify. After
// a couple of deploys the old key can be dropped.
//
// Legacy envelopes (no `kid`, signed with the historical
// "derived from HANA_PASSWORD + role hashes" secret) still verify
// under the `legacy` kid as long as the derived secret is reachable.
//
// Key map shape (the value of `keyMap` is one of these):
//   {
//     current: 'v2',                                  // which kid is the primary for new signs
//     keys: {
//       v1: 'hex-secret-1',
//       v2: 'hex-secret-2',
//       legacy: 'derived-or-any-secret'               // for envelopes without a kid
//     }
//   }
// ---------------------------------------------------------------------------

const SIGNING_ALGORITHM = 'HMAC-SHA256';

function hmacHex(secret, payload) {
  return require('crypto')
    .createHmac('sha256', String(secret))
    .update(JSON.stringify(payload))
    .digest('hex');
}

function timingSafeEq(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  if (a.length !== b.length) return false;
  const ba = Buffer.from(a, 'hex');
  const bb = Buffer.from(b, 'hex');
  if (ba.length !== bb.length) return false;
  return require('crypto').timingSafeEqual(ba, bb);
}

// Sign `payload` using the `current` kid from `keyMap`. Throws if
// the keyMap is empty or the current key is missing.
function buildSignedEnvelope(payload, keyMap) {
  if (!keyMap || !keyMap.keys || !keyMap.current) {
    throw new Error('buildSignedEnvelope: keyMap must have current + keys');
  }
  const kid = String(keyMap.current);
  const secret = keyMap.keys[kid];
  if (!secret) throw new Error(`buildSignedEnvelope: keyMap.keys[${kid}] is missing`);
  return {
    payload,
    signature: hmacHex(secret, payload),
    algorithm: SIGNING_ALGORITHM,
    kid
  };
}

// Try every key in `keyMap` until one validates. Returns the
// { kid, key } that validated, or null if none matched. Envelopes
// without a `kid` are tried under the `legacy` slot for backwards
// compatibility.
function verifySignedEnvelope(envelope, keyMap) {
  if (!envelope || typeof envelope !== 'object') return null;
  if (envelope.algorithm !== SIGNING_ALGORITHM) return null;
  if (typeof envelope.signature !== 'string' || !envelope.payload) return null;
  if (!keyMap || !keyMap.keys) return null;
  const candidates = [];
  if (typeof envelope.kid === 'string' && keyMap.keys[envelope.kid]) {
    candidates.push({ kid: envelope.kid, key: keyMap.keys[envelope.kid] });
  }
  // Always also try the legacy slot for envelopes that predate the
  // kid system (or that someone tampered to remove the kid). If the
  // envelope already has a kid, we only try that kid + legacy; we
  // don't try OTHER kids because that would mask real tampering.
  if (keyMap.keys.legacy) {
    candidates.push({ kid: 'legacy', key: keyMap.keys.legacy });
  }
  for (const { kid, key } of candidates) {
    if (timingSafeEq(hmacHex(key, envelope.payload), envelope.signature)) {
      return { kid, key };
    }
  }
  return null;
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
