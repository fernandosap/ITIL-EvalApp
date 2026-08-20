'use strict';

// Signing context — built once at startup, cached for the life
// of the process.
//
// Why a separate module:
//   - The env-var-driven getSigningKeyMap() was being called on
//     every sign/verify. That meant re-parsing 4 env vars and
//     re-validating on every request, and re-computing the legacy
//     derived secret each time. Worse, the "warning" log fired
//     every call (the comment claimed "one-shot" but the code
//     didn't enforce that).
//   - On Cloud Foundry the env doesn't change during the process
//     lifetime, so a single build at boot is safe and simpler.
//   - Encapsulating the construction in a factory + cacheable
//     instance makes the runtime path trivial (no env reads, no
//     validation, no log noise) and makes the boot path obvious.
//
// The context exposes a stable interface:
//   - sign(payload)            -> signed envelope (v2)
//   - verify(envelope)         -> { kid } | null
//   - currentKid                -> 'v2' | 'legacy' (informational)
//   - keys                      -> the same null-prototype key map
//                                 the runtime would have computed
//   - isValid                   -> true if operator config is OK
//                                 AND the active kid is the one
//                                 they configured (not legacy fallback)

const { buildSignedEnvelope, verifySignedEnvelope } = require('./responses.js');

const WARNING_FLAG = Symbol('signing-warning-emitted');

function loadLegacySecretFromEnv(env) {
  // Point 3: RESULT_SIGNING_LEGACY_KEY. If set, freeze the legacy
  // secret at the explicit value the operator provided. This
  // decouples legacy verification from HANA_PASSWORD / role
  // hashes / APP_REVISION — which is critical when those rotate
  // (the historical derived secret would otherwise change
  // silently and break verification of any envelope signed
  // before the rotation).
  if (env.RESULT_SIGNING_LEGACY_KEY) {
    return String(env.RESULT_SIGNING_LEGACY_KEY);
  }
  // Fall back to the historical derived secret. We require the
  // caller to pass the same dependencies that go into the hash
  // so this function is testable without env mutation.
  const crypto = require('node:crypto');
  return crypto.createHash('sha256').update([
    env.HANA_PASSWORD || '',
    env.ADMIN_HASH || '',
    env.MANAGER_HASH || '',
    env.REVIEWER_HASH || '',
    env.CONTENT_EDITOR_HASH || '',
    env.APP_REVISION || ''
  ].join('|')).digest('hex');
}

function parseSigningConfig(env) {
  return {
    currentId: env.RESULT_SIGNING_KEY_ID || '',
    currentSecret: env.RESULT_SIGNING_KEY || '',
    previousId: env.RESULT_SIGNING_KEY_PREVIOUS_ID || '',
    previousSecret: env.RESULT_SIGNING_KEY_PREVIOUS || ''
  };
}

function validateSigningConfig(config) {
  // Re-import the server's rules. We import the same helper that
  // server.js uses (parseSigningConfig + validateSigningConfig are
  // also exported from server.js for tests). We don't re-import
  // from server.js here because that would create a circular
  // dependency (server.js requires this module).
  //
  // To keep a single source of truth, the rules live here and
  // server.js re-uses them. This file is intentionally small and
  // independent so it can be unit-tested without booting the app.
  const SIGNING_KID_FORMAT = /^[A-Za-z0-9._-]{1,64}$/;
  const SIGNING_KID_RESERVED = 'legacy';
  const SIGNING_SECRET_MIN_LENGTH = 32;
  const errors = [];
  const { currentId, currentSecret, previousId, previousSecret } = config;

  if (currentId && !SIGNING_KID_FORMAT.test(currentId)) {
    errors.push(`RESULT_SIGNING_KEY_ID="${currentId}" is invalid. Kids must match ${SIGNING_KID_FORMAT} (1-64 chars, [A-Za-z0-9._-]).`);
  }
  if (previousId && !SIGNING_KID_FORMAT.test(previousId)) {
    errors.push(`RESULT_SIGNING_KEY_PREVIOUS_ID="${previousId}" is invalid. Kids must match ${SIGNING_KID_FORMAT} (1-64 chars, [A-Za-z0-9._-]).`);
  }
  if (currentId === SIGNING_KID_RESERVED || previousId === SIGNING_KID_RESERVED) {
    errors.push(`"${SIGNING_KID_RESERVED}" is reserved for the legacy derived secret. Pick a different id.`);
  }
  if (currentId && previousId && currentId === previousId) {
    errors.push(`RESULT_SIGNING_KEY_ID (${currentId}) and RESULT_SIGNING_KEY_PREVIOUS_ID (${previousId}) are the same. Use distinct ids so rotation actually moves traffic to the new key.`);
  }
  if (currentSecret && currentSecret.length < SIGNING_SECRET_MIN_LENGTH) {
    errors.push(`RESULT_SIGNING_KEY must be at least ${SIGNING_SECRET_MIN_LENGTH} chars (got ${currentSecret.length}). Generate a stronger secret.`);
  }
  if (previousSecret && previousSecret.length < SIGNING_SECRET_MIN_LENGTH) {
    errors.push(`RESULT_SIGNING_KEY_PREVIOUS must be at least ${SIGNING_SECRET_MIN_LENGTH} chars (got ${previousSecret.length}). Generate a stronger secret.`);
  }
  if (currentId && !currentSecret) errors.push('RESULT_SIGNING_KEY_ID is set but RESULT_SIGNING_KEY is empty.');
  if (currentSecret && !currentId) errors.push('RESULT_SIGNING_KEY is set but RESULT_SIGNING_KEY_ID is empty.');
  if (previousId && !previousSecret) errors.push('RESULT_SIGNING_KEY_PREVIOUS_ID is set but RESULT_SIGNING_KEY_PREVIOUS is empty.');
  if (previousSecret && !previousId) errors.push('RESULT_SIGNING_KEY_PREVIOUS is set but RESULT_SIGNING_KEY_PREVIOUS_ID is empty.');
  const currentConfigured = Boolean(currentId || currentSecret);
  const previousConfigured = Boolean(previousId || previousSecret);
  if (previousConfigured && !currentConfigured) {
    errors.push('RESULT_SIGNING_KEY_PREVIOUS(_ID) is set without a current RESULT_SIGNING_KEY(_ID). A previous key only makes sense during a rotation FROM a current key; configure both pairs.');
  }
  return errors;
}

function isOperatorSigningConfig(config) {
  return Boolean(config.currentId || config.currentSecret || config.previousId || config.previousSecret);
}

const SIGNING_KID_RESERVED = 'legacy';

// Build a SigningContext from an env-like object. Returns a frozen
// object whose methods can be called any number of times. The
// constructor emits the same one-shot warnings the inline
// getSigningKeyMap() used to emit (now actually one-shot).
function createSigningContext(env, { log = () => {} } = {}) {
  const config = parseSigningConfig(env);
  const errors = validateSigningConfig(config);
  const operatorTouched = isOperatorSigningConfig(config);

  if (errors.length) {
    if (operatorTouched) {
      log('warn', 'result_signing_key_invalid', { errors });
    } else {
      // No operator config -> no error. Don't even warn for the
      // 'no config at all' case here; that's surfaced separately
      // in startupSummary.
    }
  }
  if (errors.length && operatorTouched) {
    log('warn', 'result_signing_key_unusable', {
      hint: 'Signing key env vars are set but not usable. Falling back to legacy derived secret. kid=legacy on all new envelopes.'
    });
  } else if (!operatorTouched) {
    log('warn', 'result_signing_key_missing', {
      hint: 'RESULT_SIGNING_KEY env var is not set. Falling back to legacy derived secret. New envelopes will use kid=legacy.'
    });
  }

  const keys = Object.create(null);
  const currentValid = config.currentId && config.currentSecret && errors.length === 0;
  const previousValid = config.previousId && config.previousSecret && errors.length === 0;
  if (currentValid) keys[config.currentId] = String(config.currentSecret);
  if (previousValid) keys[config.previousId] = String(config.previousSecret);
  keys[SIGNING_KID_RESERVED] = loadLegacySecretFromEnv(env);

  const keyMap = {
    current: currentValid ? config.currentId : SIGNING_KID_RESERVED,
    keys
  };

  return Object.freeze({
    sign(payload) {
      return buildSignedEnvelope(payload, keyMap);
    },
    verify(envelope) {
      return verifySignedEnvelope(envelope, keyMap);
    },
    get currentKid() { return keyMap.current; },
    get keys() { return keys; },
    get keyMap() { return keyMap; },
    get isValid() { return errors.length === 0 && operatorTouched; },
    get errors() { return errors.slice(); },
    get operatorTouched() { return operatorTouched; }
  });
}

// One cached context per process. Reset only via the explicit
// _resetForTests() call. In production this is built once at
// boot (startServer) and reused for every sign/verify.
let _context = null;
let _envSnapshot = null;

function getSigningContext(env, options) {
  // If the env didn't change, return the cached context. The
  // comparison is by JSON stringification; it's cheap and the env
  // object is small. We capture only the keys we care about, not
  // the full process.env (which can be huge).
  const keys = [
    'HANA_PASSWORD', 'ADMIN_HASH', 'MANAGER_HASH', 'REVIEWER_HASH',
    'CONTENT_EDITOR_HASH', 'APP_REVISION',
    'RESULT_SIGNING_KEY_ID', 'RESULT_SIGNING_KEY',
    'RESULT_SIGNING_KEY_PREVIOUS_ID', 'RESULT_SIGNING_KEY_PREVIOUS',
    'RESULT_SIGNING_LEGACY_KEY'
  ];
  const snapshot = {};
  for (const k of keys) snapshot[k] = process.env[k] || '';
  const fingerprint = JSON.stringify(snapshot);
  if (_context && _envSnapshot === fingerprint) return _context;
  // eslint-disable-next-line no-console
  const log = (level, event, meta) => console.log(JSON.stringify({ ts: new Date().toISOString(), level, event, ...(meta || {}) }));
  _context = createSigningContext(process.env, { log });
  _envSnapshot = fingerprint;
  return _context;
}

function _resetForTests() {
  _context = null;
  _envSnapshot = null;
}

module.exports = {
  createSigningContext,
  getSigningContext,
  // Pure helpers re-exported so server.js and tests can use the
  // single source of truth without circular imports.
  parseSigningConfig,
  validateSigningConfig,
  isOperatorSigningConfig,
  loadLegacySecretFromEnv,
  // For tests.
  _resetForTests
};
