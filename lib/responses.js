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
// Envelope format — VERSION 2 (current):
//   {
//     version: 2,                  // protocol version
//     algorithm: "HMAC-SHA256",    // signing algorithm
//     kid: "v2",                   // key id (operator-supplied, stable across rotation)
//     payload: {...},              // actual signed content
//     signature: "<hex>"            // HMAC-SHA256(secret, canonical(version, algorithm, kid, payload))
//   }
//
// In v2, version + algorithm + kid + payload are ALL included in
// the HMAC input. That means an attacker who copies an envelope and
// changes its `kid` field can no longer cause it to verify under
// the legacy slot (or any other slot) — the kid in the signature
// is bound to the rest of the envelope.
//
// VERSION 1 (legacy, still readable):
//   {
//     algorithm: "HMAC-SHA256",
//     kid: "v1" or undefined,
//     payload: {...},
//     signature: HMAC-SHA256(secret, JSON.stringify(payload))
//   }
//
// In v1 only the payload is signed; kid/algorithm are not bound
// to the signature. We still verify v1 envelopes for back-compat
// with historical signed result summaries.
//
// `kid` is a key identifier. The verifier tries the envelope's
// `kid` first (v1 or v2), then the `legacy` slot. That allows
// graceful rotation: when a key is compromised, the operator
// adds a new `current` key, marks the old one as `previous`,
// deploys, and existing envelopes still verify. After a couple
// of deploys the old key can be dropped.
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
const SIGNING_VERSION = 2;
// v1 had no version field. v2 includes it. The verifier
// dispatches on the presence/absence of `envelope.version`:
//   - envelope.version === 2   -> canonical signing (full envelope)
//   - envelope.version missing -> legacy v1 signing (just payload)
const SIGNING_VERSION_V1 = 1;

function hmacHex(secret, input) {
  return require('crypto')
    .createHmac('sha256', String(secret))
    .update(input)
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

// Canonical input for v2 signing. Includes version, algorithm,
// kid, and payload so a tampered envelope header fails to
// verify. Object key order is fixed (insertion order in v8+)
// so the same logical input always serializes identically.
function canonicalV2(envelope) {
  return JSON.stringify({
    version: SIGNING_VERSION,
    algorithm: envelope.algorithm,
    kid: envelope.kid,
    payload: envelope.payload
  });
}

// Sign `payload` using the `current` kid from `keyMap`. Throws if
// the keyMap is empty or the current key is missing. The output
// envelope uses v2 format (version field included and bound to
// the signature).
function buildSignedEnvelope(payload, keyMap) {
  if (!keyMap || !keyMap.keys || !keyMap.current) {
    throw new Error('buildSignedEnvelope: keyMap must have current + keys');
  }
  const kid = String(keyMap.current);
  const secret = keyMap.keys[kid];
  if (!secret) throw new Error(`buildSignedEnvelope: keyMap.keys[${kid}] is missing`);
  const envelope = {
    version: SIGNING_VERSION,
    algorithm: SIGNING_ALGORITHM,
    kid,
    payload
  };
  envelope.signature = hmacHex(secret, canonicalV2(envelope));
  return envelope;
}

// Try every key in `keyMap` until one validates. Returns the
// { kid, key } that validated, or null if none matched.
//
// Version dispatch:
//   - v2 (envelope.version === 2): signature must match
//     HMAC(secret, canonical({version, algorithm, kid, payload})).
//     The kid in the signature is bound to the rest of the
//     envelope, so an attacker can't change `kid` and have the
//     envelope verify under a different key.
//   - v1 (no version field): signature is HMAC(secret, payload).
//     Kept for back-compat with envelopes signed before v2.
function verifySignedEnvelope(envelope, keyMap) {
  if (!envelope || typeof envelope !== 'object') return null;
  if (envelope.algorithm !== SIGNING_ALGORITHM) return null;
  if (typeof envelope.signature !== 'string' || !envelope.payload) return null;
  if (!keyMap || !keyMap.keys) return null;
  const isV2 = envelope.version === SIGNING_VERSION;
  const candidates = [];
  if (typeof envelope.kid === 'string' && keyMap.keys[envelope.kid]) {
    candidates.push({ kid: envelope.kid, key: keyMap.keys[envelope.kid] });
  }
  // Always also try the legacy slot for envelopes that predate
  // the kid system (or that someone tampered to remove the kid).
  if (keyMap.keys.legacy) {
    candidates.push({ kid: 'legacy', key: keyMap.keys.legacy });
  }
  for (const { kid, key } of candidates) {
    // Rebuild the canonical input that the signer would have used,
    // using the kid/key we just chose. v1 only signs payload;
    // v2 signs the full canonical form. For v2, this means we
    // re-derive the expected signature using the kid FROM THE
    // SIGNATURE'S CANDIDATE (which for v1 means the envelope's
    // own kid, for v2 we re-sign with the same kid in the body).
    const input = isV2
      ? canonicalV2(envelope)
      : JSON.stringify(envelope.payload);
    if (timingSafeEq(hmacHex(key, input), envelope.signature)) {
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
  SIGNING_ALGORITHM,
  SIGNING_VERSION,
  SIGNING_VERSION_V1
};
