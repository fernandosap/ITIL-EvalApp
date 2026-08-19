'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  readPoolConfig,
  readConnConfig
} = require('../shared/db-pool.js');

function snapshotEnv(keys) {
  const saved = {};
  for (const k of keys) saved[k] = process.env[k];
  return () => {
    for (const k of keys) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  };
}

const ENV_KEYS = [
  'HANA_POOL_SIZE',
  'HANA_POOL_PING_CHECK',
  'HANA_POOL_TTL_SECONDS',
  'HANA_HOST',
  'HANA_PORT',
  'HANA_USER',
  'HANA_PASSWORD',
  'HANA_ENCRYPT',
  'HANA_SSL_VALIDATE_CERTIFICATE'
];

// ---------------------------------------------------------------------------
// readPoolConfig
// ---------------------------------------------------------------------------

test('readPoolConfig: returns null when HANA_POOL_SIZE is unset', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    delete process.env.HANA_POOL_SIZE;
    assert.equal(readPoolConfig(), null);
  } finally { restore(); }
});

test('readPoolConfig: returns null when HANA_POOL_SIZE=0', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_POOL_SIZE = '0';
    assert.equal(readPoolConfig(), null);
  } finally { restore(); }
});

test('readPoolConfig: returns null when HANA_POOL_SIZE is negative or NaN', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_POOL_SIZE = '-5';
    assert.equal(readPoolConfig(), null);
    process.env.HANA_POOL_SIZE = 'not-a-number';
    assert.equal(readPoolConfig(), null);
  } finally { restore(); }
});

test('readPoolConfig: returns a config object when HANA_POOL_SIZE > 0', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_POOL_SIZE = '10';
    const cfg = readPoolConfig();
    assert.ok(cfg);
    assert.equal(cfg.maxConnectedOrPooled, 10);
    assert.equal(cfg.pingCheck, true);  // default
    assert.equal(cfg.expirationInSeconds, 300);  // default 5 min
  } finally { restore(); }
});

test('readPoolConfig: rounds fractional HANA_POOL_SIZE down to int', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_POOL_SIZE = '7.9';
    const cfg = readPoolConfig();
    assert.equal(cfg.maxConnectedOrPooled, 7);
  } finally { restore(); }
});

test('readPoolConfig: clamps HANA_POOL_SIZE to a minimum of 1', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_POOL_SIZE = '0.4';
    const cfg = readPoolConfig();
    assert.equal(cfg.maxConnectedOrPooled, 1);
  } finally { restore(); }
});

test('readPoolConfig: pingCheck defaults to true, can be turned off', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_POOL_SIZE = '5';
    assert.equal(readPoolConfig().pingCheck, true);
    process.env.HANA_POOL_PING_CHECK = 'false';
    assert.equal(readPoolConfig().pingCheck, false);
    process.env.HANA_POOL_PING_CHECK = 'FALSE';
    assert.equal(readPoolConfig().pingCheck, false);
  } finally { restore(); }
});

test('readPoolConfig: expirationInSeconds has a 60-second floor', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_POOL_SIZE = '5';
    process.env.HANA_POOL_TTL_SECONDS = '5';
    assert.equal(readPoolConfig().expirationInSeconds, 60);
  } finally { restore(); }
});

// ---------------------------------------------------------------------------
// readConnConfig
// ---------------------------------------------------------------------------

test('readConnConfig: builds serverNode from HANA_HOST:PORT', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_HOST = 'h';
    process.env.HANA_PORT = '443';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    const cfg = readConnConfig();
    assert.equal(cfg.serverNode, 'h:443');
    assert.equal(cfg.uid, 'u');
    assert.equal(cfg.pwd, 'p');
  } finally { restore(); }
});

test('readConnConfig: defaults HANA_PORT to 443', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_HOST = 'h';
    delete process.env.HANA_PORT;
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    assert.equal(readConnConfig().serverNode, 'h:443');
  } finally { restore(); }
});

test('readConnConfig: defaults encrypt=true, sslValidateCertificate=false', () => {
  const restore = snapshotEnv(ENV_KEYS);
  try {
    process.env.HANA_HOST = 'h';
    process.env.HANA_USER = 'u';
    process.env.HANA_PASSWORD = 'p';
    delete process.env.HANA_ENCRYPT;
    delete process.env.HANA_SSL_VALIDATE_CERTIFICATE;
    const cfg = readConnConfig();
    assert.equal(cfg.encrypt, true);
    assert.equal(cfg.sslValidateCertificate, false);
  } finally { restore(); }
});
