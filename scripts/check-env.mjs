import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const envPath = path.join(root, '.env');

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  for (const rawLine of lines) {
    const line = String(rawLine || '').trim();
    if (!line || line.startsWith('#')) continue;
    const eqIdx = line.indexOf('=');
    if (eqIdx <= 0) continue;
    const key = line.slice(0, eqIdx).trim();
    if (!key || process.env[key] != null) continue;
    let value = line.slice(eqIdx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"'))
      || (value.startsWith("'") && value.endsWith("'"))
    ) value = value.slice(1, -1);
    process.env[key] = value;
  }
}

loadDotEnv(envPath);

const required = ['HANA_HOST', 'HANA_PORT', 'HANA_USER', 'HANA_PASSWORD', 'HANA_SCHEMA'];
const warnings = [];
const errors = [];
const missing = required.filter((key) => !String(process.env[key] || '').trim());
if (missing.length) errors.push(`Missing required env: ${missing.join(', ')}`);

if (!String(process.env.ADMIN_HASH || '').trim()) warnings.push('ADMIN_HASH missing. Legacy admin password login disabled.');
if (!String(process.env.MANAGER_HASH || '').trim()) warnings.push('MANAGER_HASH missing. Legacy manager password login disabled.');
if (!String(process.env.REVIEWER_HASH || '').trim()) warnings.push('REVIEWER_HASH missing. Legacy reviewer password login disabled.');
if (!String(process.env.CONTENT_EDITOR_HASH || '').trim()) warnings.push('CONTENT_EDITOR_HASH missing. Legacy content editor password login disabled.');

const tlsValidate = String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'true').toLowerCase() === 'true';
if (!tlsValidate) warnings.push('HANA_SSL_VALIDATE_CERTIFICATE=false. TLS peer validation off.');
if (String(process.env.STARTUP_STRICT || 'false').toLowerCase() !== 'true') {
  warnings.push('STARTUP_STRICT=false. App may boot with config errors.');
}

const ssoKey = String(process.env.SSO_SESSION_ENCRYPTION_KEY || '').trim();
const ssoKeyId = String(process.env.SSO_SESSION_KEY_ID || 'v1').trim();
const previousKey = String(process.env.SSO_SESSION_PREVIOUS_KEY || '').trim();
const previousId = String(process.env.SSO_SESSION_PREVIOUS_KEY_ID || '').trim();
const keyIdRe = /^[A-Za-z0-9._-]{1,64}$/;
if (ssoKey && Buffer.byteLength(ssoKey, 'utf8') < 32) errors.push('SSO_SESSION_ENCRYPTION_KEY must be at least 32 bytes.');
if (ssoKey && !keyIdRe.test(ssoKeyId)) errors.push('SSO_SESSION_KEY_ID is invalid.');
if (Boolean(previousKey) !== Boolean(previousId)) errors.push('SSO_SESSION_PREVIOUS_KEY_ID and SSO_SESSION_PREVIOUS_KEY must be configured together.');
if (previousKey && Buffer.byteLength(previousKey, 'utf8') < 32) errors.push('SSO_SESSION_PREVIOUS_KEY must be at least 32 bytes.');
if (previousId && !keyIdRe.test(previousId)) errors.push('SSO_SESSION_PREVIOUS_KEY_ID is invalid.');
if (previousId && previousId === ssoKeyId) errors.push('Current and previous SSO session key IDs must differ.');
if (!ssoKey) warnings.push('SSO_SESSION_ENCRYPTION_KEY missing. Required when XSUAA browser SSO is bound.');

if (errors.length) {
  for (const error of errors) console.error(error);
  process.exit(1);
}

console.log(JSON.stringify({
  ok: true,
  envFile: fs.existsSync(envPath),
  schema: process.env.HANA_SCHEMA,
  encrypt: String(process.env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
  sslValidateCertificate: tlsValidate,
  startupStrict: String(process.env.STARTUP_STRICT || 'false').toLowerCase() === 'true',
  ssoSessionKeyConfigured: Boolean(ssoKey),
  ssoSessionKeyId: ssoKey ? ssoKeyId : null,
  ssoPreviousKeyConfigured: Boolean(previousKey),
  warnings
}, null, 2));