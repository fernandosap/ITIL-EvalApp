'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { buildPolicy } = require('../../lib/core/csp.js');

const root = path.resolve(__dirname, '..', '..');
const port = Number(process.env.E2E_PORT || 4173);
const csp = buildPolicy(root);
const types = { '.js': 'application/javascript', '.html': 'text/html; charset=utf-8', '.svg': 'image/svg+xml' };
const questions = [
  { stem: 'Which framework is being assessed?', opts: ['ITIL 4', 'COBIT', 'TOGAF'], multi: false, note: '', requiredSelections: 1 },
  { stem: 'Select the two practices used in this fixture.', opts: ['Incident Management', 'Change Enablement', 'Payroll'], multi: true, note: 'Select exactly TWO answers.', requiredSelections: 2 },
  { stem: 'What is the purpose of the E2E flow?', opts: ['Validate the browser journey', 'Generate production data'], multi: false, note: '', requiredSelections: 1 }
];

function send(res, status, body, type = 'application/json', extraHeaders = {}) {
  const headers = { 'Content-Type': type, 'Cache-Control': 'no-store', ...extraHeaders };
  if (String(type).startsWith('text/html')) headers['Content-Security-Policy'] = csp;
  res.writeHead(status, headers);
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch (_e) { resolve({}); }
    });
  });
}

function invalidSelection(body, requireComplete) {
  const answers = Array.isArray(body?.answers) ? body.answers : [];
  for (let i = 0; i < questions.length; i += 1) {
    const selected = Array.isArray(answers[i]) ? [...new Set(answers[i])] : [];
    const required = questions[i].requiredSelections;
    if (selected.length > required || (requireComplete && selected.length !== required)) {
      return { question: i + 1, selectedCount: selected.length, requiredSelections: required };
    }
  }
  return null;
}

async function api(req, res, url) {
  if (url.pathname === '/api/status') {
    return send(res, 200, JSON.stringify({ examActive: true, examName: 'E2E Exam', total: 3, durationSecs: 2700, passPct: 80, passScore: 3, proctorEnabled: false }));
  }
  if (url.pathname === '/api/admin/auth-methods') return send(res, 200, JSON.stringify({ password: true, xsuaa: { enabled: false } }));
  if (url.pathname === '/api/admin/me') return send(res, 401, JSON.stringify({ ok: false, error: 'unauthorized' }));
  if (url.pathname === '/api/client-errors' && req.method === 'POST') return send(res, 200, JSON.stringify({ ok: true }));

  if (url.pathname === '/api/validate' && req.method === 'POST') {
    const body = await readBody(req);
    if (String(body.code || '').toUpperCase() !== 'ABC234') return send(res, 200, JSON.stringify({ valid: false, reason: 'not_found' }));
    return send(res, 200, JSON.stringify({
      valid: true, status: 'unused', durationSecs: 2700, passPct: 80, passScore: 3, total: 3,
      proctorEnabled: false, examMode: 'GRADED', isPractice: false, showCorrectAnswers: false
    }));
  }

  if (url.pathname === '/api/session/start' && req.method === 'POST') {
    await readBody(req);
    return send(res, 200, JSON.stringify({
      ok: true, examToken: 'e2e-exam-token', total: 3, durationSecs: 2700, passPct: 80, passScore: 3,
      proctorEnabled: false, examMode: 'GRADED', isPractice: false, showCorrectAnswers: false
    }));
  }

  const question = url.pathname.match(/^\/api\/question\/(\d+)$/);
  if (question && req.method === 'GET') {
    const index = Number(question[1]);
    if (!questions[index]) return send(res, 404, JSON.stringify({ error: 'question_not_found' }));
    return send(res, 200, JSON.stringify({ index, ...questions[index] }));
  }

  if (url.pathname === '/api/progress' && req.method === 'POST') {
    const body = await readBody(req);
    const invalid = invalidSelection(body, false);
    if (invalid) return send(res, 400, JSON.stringify({ error: 'too_many_selections', ...invalid }));
    return send(res, 200, JSON.stringify({ ok: true }));
  }

  if (url.pathname === '/api/submit' && req.method === 'POST') {
    const body = await readBody(req);
    const invalid = invalidSelection(body, body.autoSubmit !== true);
    if (invalid) return send(res, 400, JSON.stringify({ error: 'selection_count_incomplete', ...invalid }));
    return send(res, 200, JSON.stringify({
      ok: true,
      result: {
        code: 'ABC234', score: 3, total: 3, pct: 100, pass: true, passPct: 80, durationSecs: 95,
        submittedAt: new Date().toISOString(), tabSwitches: 0, incidentCount: 0, autoSubmit: false,
        isPractice: false, showCorrectAnswers: false, sectionResults: []
      }
    }));
  }

  return send(res, 404, JSON.stringify({ error: 'fixture_not_implemented', path: url.pathname }));
}

http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname.startsWith('/api/')) return api(req, res, url);

  let file;
  if (url.pathname === '/' || url.pathname === '') file = path.join(root, 'index.html');
  else if (/^\/client\/[A-Za-z0-9_-]+\.js$/.test(url.pathname)) file = path.join(root, url.pathname);
  else if (url.pathname === '/shared/constants.js') file = path.join(root, 'shared', 'constants.js');
  else if (url.pathname === '/favicon.svg') file = path.join(root, 'favicon.svg');
  else file = path.join(root, 'index.html');

  if (!file.startsWith(root) || !fs.existsSync(file)) return send(res, 404, 'Not found', 'text/plain');
  return send(res, 200, fs.readFileSync(file), types[path.extname(file)] || 'application/octet-stream');
}).listen(port, '127.0.0.1', () => console.log(`E2E fixture listening on ${port}`));
