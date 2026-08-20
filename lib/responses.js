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

// Try the right key in `keyMap` based on the envelope's
// declared version. Returns the { kid, key } that validated, or
// null if none matched.
//
// Strict version dispatch:
//   - envelope.version === 2 (v2): the kid is authoritative. We
//     verify ONLY against keys[envelope.kid]. Legacy fallback is
//     NOT applied — a v2 envelope that claims kid="current" but
//     happens to verify under the legacy key would otherwise leak
//     the historical fallback into the cryptographic binding.
//   - envelope.version === undefined (v1): we use the legacy
//     HMAC(secret, payload) signature. We try the envelope's kid
//     if present, then fall back to the legacy slot. v1 didn't
//     bind kid/algorithm to the signature, so the kid is
//     advisory, not authoritative — and the legacy fallback is
//     the only way historical envelopes verify.
//   - envelope.version is anything else (e.g. 1, 3, "banana"):
//     REJECT. Without strict dispatch, an envelope claiming
//     version=999 would silently get v1 treatment, which is a
//     protocol-confusion path. Only the missing-version case is
//     a historical v1 envelope; any other version string is
//     unknown and must be rejected.
function verifySignedEnvelope(envelope, keyMap) {
  if (!envelope || typeof envelope !== 'object') return null;
  if (envelope.algorithm !== SIGNING_ALGORITHM) return null;
  if (typeof envelope.signature !== 'string' || !envelope.payload) return null;
  if (!keyMap || !keyMap.keys) return null;

  // v2: kid is authoritative, no legacy fallback.
  if (envelope.version === SIGNING_VERSION) {
    if (typeof envelope.kid !== 'string') return null;
    const key = keyMap.keys[envelope.kid];
    if (!key) return null;
    const input = canonicalV2(envelope);
    if (timingSafeEq(hmacHex(key, input), envelope.signature)) {
      return { kid: envelope.kid, key };
    }
    return null;
  }

  // Historical v1 (no version field): kid is advisory, legacy
  // fallback applies.
  if (envelope.version === undefined) {
    const candidates = [];
    if (typeof envelope.kid === 'string' && keyMap.keys[envelope.kid]) {
      candidates.push({ kid: envelope.kid, key: keyMap.keys[envelope.kid] });
    }
    if (keyMap.keys.legacy) {
      candidates.push({ kid: 'legacy', key: keyMap.keys.legacy });
    }
    const input = JSON.stringify(envelope.payload);
    for (const { kid, key } of candidates) {
      if (timingSafeEq(hmacHex(key, input), envelope.signature)) {
        return { kid, key };
      }
    }
    return null;
  }

  // Any other explicit version is unknown — reject.
  return null;
}

// Verify a raw payload-only signature (the legacy
// /verify-signature endpoint contract: { payload, signature }
// pair, no envelope, no version). Iterates the FULL key ring
// (current first, then every other kid in the keyMap) so
// historical raw signatures still verify across key rotation.
// This is the rotation-safe counterpart to signLegacyPayload()
// — signLegacyPayload() re-signs with the current key, which
// is fine for the typical "I just signed this; verify it" use
// case, but breaks for historical raw signatures produced
// under a previous or legacy key.
//
// We try in this priority order:
//   1. keyMap.current (the canonical kid for new v2 envelopes;
//      this may itself be 'legacy' when no operator key is
//      configured — the legacy slot IS the current key in that
//      mode, so it must be tried)
//   2. Every other kid in keyMap.keys (the previous key during
//      rotation, and the legacy slot if it isn't the current)
//
// The `tried` Set dedups by secret value, so the pathological
// "current and legacy hold the same secret" case is harmless —
// one try, one match, return the first kid we hit.
//
// Returns the matching kid, or null. Safe under key rotation:
// historical raw signatures verify under their original key as
// long as the operator kept that key in the keyMap.
function verifyLegacyPayload(payload, signature, keyMap) {
  if (!keyMap || !keyMap.keys) return null;
  if (typeof signature !== 'string' || !signature) return null;
  if (payload === undefined) return null;
  const input = JSON.stringify(payload);
  const tried = new Set();

  function tryKey(kid, key) {
    if (!key) return null;
    if (tried.has(key)) return null;  // dedup by secret value
    tried.add(key);
    if (timingSafeEq(hmacHex(key, input), signature)) {
      return { kid, key };
    }
    return null;
  }

  // 1. Current key. Note: keyMap.current can be the literal
  // 'legacy' when no operator key is configured — in that mode
  // the legacy slot is the current key, so we still need to try
  // it here. The dedup Set in tryKey prevents double-credit if
  // some other kid also happens to share the same secret.
  const currentKid = keyMap.current;
  if (currentKid) {
    const m = tryKey(currentKid, keyMap.keys[currentKid]);
    if (m) return m;
  }

  // 2. Every other kid in the keyMap (previous key during
  // rotation, legacy slot when it isn't the current).
  for (const kid of Object.keys(keyMap.keys)) {
    if (kid === currentKid) continue;
    const m = tryKey(kid, keyMap.keys[kid]);
    if (m) return m;
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
  verifyLegacyPayload,
  SIGNING_ALGORITHM,
  SIGNING_VERSION,
  SIGNING_VERSION_V1
};
