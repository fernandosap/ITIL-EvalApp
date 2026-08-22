'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

function hashSource(value) {
  return `'sha256-${crypto.createHash('sha256').update(String(value), 'utf8').digest('base64')}'`;
}

function unique(values) { return [...new Set(values.filter(Boolean))]; }

function extractHashes(rootDir = path.join(__dirname, '..', '..')) {
  const html = fs.readFileSync(path.join(rootDir, 'index.html'), 'utf8');
  const scriptBlocks = [...html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)].map((m) => hashSource(m[1]));
  const styleBlocks = [...html.matchAll(/<style[^>]*>([\s\S]*?)<\/style>/gi)].map((m) => hashSource(m[1]));
  // Preserve the opening quote and use a backreference for the closing quote.
  // Handler bodies can legitimately contain the other quote type (for example
  // replace(..., '')), so stopping on either quote would hash only a prefix.
  const handlerValues = [...html.matchAll(/\bon[a-z]+\s*=\s*(["'])([\s\S]*?)\1/gi)].map((m) => hashSource(m[2]));
  return {
    scripts: unique(scriptBlocks),
    styles: unique(styleBlocks),
    handlers: unique(handlerValues)
  };
}

function buildPolicy(rootDir) {
  const hashes = extractHashes(rootDir);
  const script = ["'self'", ...hashes.scripts].join(' ');
  const scriptAttr = hashes.handlers.length
    ? ["'unsafe-hashes'", ...hashes.handlers].join(' ')
    : "'none'";
  // The SPA still uses element.style for dynamic progress/timer/layout values.
  // Keep that narrowly isolated to style attributes while removing the former
  // broad style-src unsafe-inline permission for executable style blocks.
  return [
    "default-src 'self'",
    `script-src ${script}`,
    `script-src-attr ${scriptAttr}`,
    `style-src 'self' ${hashes.styles.join(' ')}`.trim(),
    "style-src-attr 'unsafe-inline'",
    "img-src 'self' data:",
    "connect-src 'self'",
    "frame-ancestors 'none'",
    "base-uri 'self'",
    "form-action 'self'",
    "object-src 'none'"
  ].join('; ');
}

function middleware(rootDir) {
  const policy = buildPolicy(rootDir);
  return function strictCsp(_req, res, next) {
    const original = res.setHeader.bind(res);
    res.setHeader = function setHeader(name, value) {
      if (String(name).toLowerCase() === 'content-security-policy') return original(name, policy);
      return original(name, value);
    };
    return next();
  };
}

module.exports = { hashSource, extractHashes, buildPolicy, middleware };