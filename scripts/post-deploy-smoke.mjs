#!/usr/bin/env node
import { pathToFileURL } from 'node:url';
import path from 'node:path';

export function normalizeBaseUrl(value) {
  const raw = String(value || '').trim().replace(/\/+$/, '');
  if (!raw) throw new Error('SMOKE_BASE_URL is required');
  const url = new URL(raw);
  if (!['https:', 'http:'].includes(url.protocol)) throw new Error('SMOKE_BASE_URL must use http or https');
  if (url.protocol !== 'https:' && String(process.env.SMOKE_ALLOW_HTTP || '').toLowerCase() !== 'true') {
    throw new Error('SMOKE_BASE_URL must use https unless SMOKE_ALLOW_HTTP=true');
  }
  return url.toString().replace(/\/$/, '');
}

export function assertSecurityHeaders(headers) {
  const csp = String(headers.get('content-security-policy') || '');
  if (!csp.includes("frame-ancestors 'none'")) throw new Error('missing_strict_frame_ancestors');
  if (!csp.includes("object-src 'none'")) throw new Error('missing_strict_object_src');
  if (csp.includes("script-src 'self' 'unsafe-inline'")) throw new Error('broad_script_unsafe_inline_detected');
  const nosniff = String(headers.get('x-content-type-options') || '').toLowerCase();
  if (nosniff !== 'nosniff') throw new Error('missing_nosniff');
}

async function fetchChecked(fetchImpl, url, init = {}) {
  const res = await fetchImpl(url, { redirect: 'manual', ...init });
  if (!res.ok) throw new Error(`${init.method || 'GET'} ${url} returned HTTP ${res.status}`);
  return res;
}

export async function runSmoke({ baseUrl, accessCode = '', fetchImpl = globalThis.fetch } = {}) {
  if (typeof fetchImpl !== 'function') throw new Error('fetch_unavailable');
  const base = normalizeBaseUrl(baseUrl || process.env.SMOKE_BASE_URL);
  const results = [];

  const landing = await fetchChecked(fetchImpl, `${base}/`);
  const landingText = await landing.text();
  if (!/id=["']app["']/.test(landingText)) throw new Error('landing_missing_app_root');
  assertSecurityHeaders(landing.headers);
  if (!landing.headers.get('x-request-id')) throw new Error('landing_missing_request_id');
  results.push('landing');

  const status = await fetchChecked(fetchImpl, `${base}/api/status`, { headers: { Accept: 'application/json' } });
  const statusJson = await status.json();
  if (!statusJson || typeof statusJson !== 'object' || statusJson.error) throw new Error(`status_invalid:${statusJson?.error || 'unknown'}`);
  if (!Object.prototype.hasOwnProperty.call(statusJson, 'examActive')) throw new Error('status_missing_examActive');
  results.push('status');

  const moduleRes = await fetchChecked(fetchImpl, `${base}/client/main.js`);
  const moduleText = await moduleRes.text();
  if (!moduleText.includes('IE')) throw new Error('client_main_unexpected_content');
  results.push('client-module');

  const safeCode = String(accessCode || process.env.SMOKE_ACCESS_CODE || '').trim().toUpperCase();
  if (safeCode) {
    if (!/^[A-Z2-9]{6}$/.test(safeCode)) throw new Error('SMOKE_ACCESS_CODE must be a valid 6-character code');
    const validate = await fetchChecked(fetchImpl, `${base}/api/validate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ code: safeCode })
    });
    const validation = await validate.json();
    if (validation.valid !== true) throw new Error(`smoke_access_code_not_valid:${validation.reason || 'unknown'}`);
    results.push('access-code-validation');
  }

  return { ok: true, baseUrl: base, checks: results };
}

async function main() {
  const result = await runSmoke();
  console.log(JSON.stringify(result));
}

const invoked = process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (invoked) main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err.message }));
  process.exitCode = 1;
});
