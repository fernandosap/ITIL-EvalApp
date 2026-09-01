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

const questionSets = [
  { id: 1, name: 'New Default Exam', isActive: true, questionCount: 30, versionNumber: 4, lifecycleStatus: 'PUBLISHED', numQuestions: 30, durationMinutes: 45, passPct: 80, examMode: 'GRADED', proctorEnabled: true },
  { id: 2, name: 'Historic Exam', isActive: false, questionCount: 30, versionNumber: 2, lifecycleStatus: 'PUBLISHED', numQuestions: 30, durationMinutes: 45, passPct: 80, examMode: 'GRADED', proctorEnabled: true }
];

const adminCodes = [
  { code: 'ADM201', label: 'Seat 1', status: 'unused', questionSetId: null, questionSetName: '', examMode: '', archived: false, notes: '', score: null, pct: null, durationSecs: null, tabSwitches: 0, incidentCount: 0, submittedAt: null },
  { code: 'ADM202', label: 'Seat 10', status: 'completed', questionSetId: 2, questionSetName: 'Historic Exam', questionSetVersion: 2, examMode: 'GRADED', archived: false, notes: '', score: 25, pct: 83, durationSecs: 1200, tabSwitches: 1, incidentCount: 2, submittedAt: '2026-08-30T18:00:00.000Z' },
  { code: 'ADM203', label: 'Seat 20', status: 'completed', questionSetId: 1, questionSetName: 'New Default Exam', questionSetVersion: 4, examMode: 'GRADED', archived: false, notes: '', score: 29, pct: 97, durationSecs: 1050, tabSwitches: 0, incidentCount: 0, submittedAt: '2026-08-31T18:00:00.000Z' },
  { code: 'ADM204', label: 'Seat 30', status: 'completed', questionSetId: 2, questionSetName: 'Historic Exam', questionSetVersion: 2, examMode: 'GRADED', archived: true, archivedAt: '2026-08-31T19:00:00.000Z', notes: '', score: 27, pct: 90, durationSecs: 1100, tabSwitches: 0, incidentCount: 1, submittedAt: '2026-08-29T18:00:00.000Z' }
];

const notifications = [
  { level: 'high', message: 'Exam ADM202 has 12 flagged incident(s).', detail: '2026-08-31T20:00:00Z' },
  { level: 'high', message: 'Exam ABC111 has 9 flagged incident(s).', detail: '2026-08-31T19:00:00Z' },
  { level: 'high', message: 'Exam ABC112 has 7 flagged incident(s).', detail: '2026-08-31T18:00:00Z' },
  { level: 'medium', message: 'Exam ABC113 has 2 flagged incident(s).', detail: '2026-08-31T17:00:00Z' },
  { level: 'medium', message: 'Exam ABC114 has 2 flagged incident(s).', detail: '2026-08-31T16:00:00Z' }
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

function isAdmin(req) { return req.headers['x-admin-token'] === 'e2e-admin-token'; }

async function adminApi(req, res, url) {
  if (url.pathname === '/api/admin/auth-methods') return send(res, 200, JSON.stringify({ password: true, xsuaa: { enabled: false } }));
  if (url.pathname === '/api/admin/login' && req.method === 'POST') {
    await readBody(req);
    return send(res, 200, JSON.stringify({ ok: true, token: 'e2e-admin-token', role: 'admin' }));
  }
  if (url.pathname === '/api/admin/me') return send(res, 401, JSON.stringify({ ok: false, error: 'unauthorized' }));
  if (!isAdmin(req)) return send(res, 401, JSON.stringify({ error: 'unauthorized' }));

  if (url.pathname === '/api/admin/codes' && req.method === 'GET') {
    return send(res, 200, JSON.stringify({ ok: true, role: 'admin', codes: adminCodes, questionSets }));
  }
  if (url.pathname === '/api/admin/system-status' && req.method === 'GET') {
    return send(res, 200, JSON.stringify({
      ok: true, questionCount: 60, questionSetCount: 2, accessCodeCount: adminCodes.length,
      activeSessionCount: 0, resultCount: 3, appVersion: '1.0.0', appRevision: 'e2e',
      deployedAt: '2026-08-31T20:00:00.000Z', schema: 'ITIL_EXAM', activeQuestionSet: questionSets[0],
      examEnabled: true, notesEnabled: true, staleSessionCount: 0, staleSessions: [], auditEnabled: true,
      auditCount: 12, adminConfigured: true, managerConfigured: true, reviewerConfigured: true,
      contentEditorConfigured: true, adminSessionRevokedAt: null, warnings: []
    }));
  }
  if (url.pathname === '/api/admin/audit' && req.method === 'GET') {
    return send(res, 200, JSON.stringify({ ok: true, entries: [
      { createdAt: '2026-08-31T20:01:00.000Z', action: 'admin_note_saved', targetCode: 'ADM202', clientIp: '127.0.0.1', details: { noteLength: 4 } }
    ] }));
  }
  if (url.pathname === '/api/admin/notifications' && req.method === 'GET') return send(res, 200, JSON.stringify({ ok: true, notifications }));
  if (url.pathname === '/api/admin/analytics/overview' && req.method === 'GET') {
    return send(res, 200, JSON.stringify({ ok: true, summary: { attempts: 3, averagePct: 90, passRate: 100, averageDurationSecs: 1117 }, trend: [], byQuestionSet: [], weakestSections: [] }));
  }
  if ((url.pathname === '/api/admin/codes/archive' || url.pathname === '/api/admin/codes/unarchive') && req.method === 'POST') {
    const body = await readBody(req);
    const archived = url.pathname.endsWith('/archive');
    const codes = Array.isArray(body.codes) ? body.codes : [];
    for (const code of codes) {
      const row = adminCodes.find((item) => item.code === code);
      if (row) {
        row.archived = archived;
        row.archivedAt = archived ? new Date().toISOString() : null;
      }
    }
    return send(res, 200, JSON.stringify({ ok: true, archived, updatedCount: codes.length }));
  }
  if (url.pathname === '/api/admin/export.csv' && req.method === 'GET') {
    return send(res, 200, 'Code,Seat\nADM202,Seat 10', 'text/csv; charset=utf-8');
  }
  return send(res, 404, JSON.stringify({ error: 'admin_fixture_not_implemented', path: url.pathname }));
}

async function api(req, res, url) {
  if (url.pathname.startsWith('/api/admin/')) return adminApi(req, res, url);
  if (url.pathname === '/api/status') {
    return send(res, 200, JSON.stringify({ examActive: true, examName: 'E2E Exam', total: 3, durationSecs: 2700, passPct: 80, passScore: 3, proctorEnabled: false }));
  }
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
