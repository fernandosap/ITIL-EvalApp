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
      || (value.startsWith('\'') && value.endsWith('\''))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

loadDotEnv(envPath);

const required = [
  'HANA_HOST',
  'HANA_PORT',
  'HANA_USER',
  'HANA_PASSWORD',
  'HANA_SCHEMA'
];

const warnings = [];
const missing = required.filter((key) => !String(process.env[key] || '').trim());

if (!String(process.env.ADMIN_HASH || '').trim()) warnings.push('ADMIN_HASH missing. Admin login disabled.');
if (!String(process.env.MANAGER_HASH || '').trim()) warnings.push('MANAGER_HASH missing. Manager login disabled.');
if (!String(process.env.REVIEWER_HASH || '').trim()) warnings.push('REVIEWER_HASH missing. Reviewer login disabled.');
if (!String(process.env.CONTENT_EDITOR_HASH || '').trim()) warnings.push('CONTENT_EDITOR_HASH missing. Content editor login disabled.');
if (String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'false').toLowerCase() !== 'true') {
  warnings.push('HANA_SSL_VALIDATE_CERTIFICATE=false. TLS peer validation off.');
}
if (String(process.env.STARTUP_STRICT || 'false').toLowerCase() !== 'true') {
  warnings.push('STARTUP_STRICT=false. App may boot with config errors.');
}

if (missing.length) {
  console.error(`Missing required env: ${missing.join(', ')}`);
  process.exit(1);
}

console.log(JSON.stringify({
  ok: true,
  envFile: fs.existsSync(envPath),
  schema: process.env.HANA_SCHEMA,
  encrypt: String(process.env.HANA_ENCRYPT || 'true').toLowerCase() === 'true',
  sslValidateCertificate: String(process.env.HANA_SSL_VALIDATE_CERTIFICATE || 'false').toLowerCase() === 'true',
  startupStrict: String(process.env.STARTUP_STRICT || 'false').toLowerCase() === 'true',
  warnings
}, null, 2));
