import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import serverModule from '../server.js';

const { startServer, stopBackgroundJobs } = serverModule;
const root = process.cwd();
const envPath = path.join(root, '.env');

function loadDotEnv(filePath) {
  if (!fs.existsSync(filePath)) return;
  for (const rawLine of fs.readFileSync(filePath, 'utf8').split(/\r?\n/)) {
    const line = String(rawLine || '').trim();
    if (!line || line.startsWith('#')) continue;
    const eqIdx = line.indexOf('=');
    if (eqIdx <= 0) continue;
    const key = line.slice(0, eqIdx).trim();
    if (!key || process.env[key] != null) continue;
    let value = line.slice(eqIdx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) value = value.slice(1, -1);
    process.env[key] = value;
  }
}
loadDotEnv(envPath);

const adminHash = String(process.env.ADMIN_HASH || '').trim().toLowerCase();
if (!adminHash) {
  console.error('ADMIN_HASH missing. Smoke test needs hashed admin credential.');
  process.exit(1);
}

async function apiJson(baseUrl, url, options = {}) {
  const resp = await fetch(`${baseUrl}${url}`, options);
  const text = await resp.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (_err) { data = { raw: text }; }
  return { resp, data };
}

async function cleanupFixture(baseUrl, token, code, cloneId) {
  if (!baseUrl || !token) return;
  const headers = { 'Content-Type': 'application/json', 'X-Admin-Token': token };
  if (cloneId) {
    try { await apiJson(baseUrl, `/api/admin/question-sets/${cloneId}/archive`, { method: 'POST', headers, body: '{}' }); } catch (_e) {}
    try { await apiJson(baseUrl, `/api/admin/question-sets/${cloneId}`, { method: 'DELETE', headers }); } catch (_e) {}
  }
  if (code) {
    try { await apiJson(baseUrl, `/api/admin/codes/${encodeURIComponent(code)}`, { method: 'DELETE', headers }); } catch (_e) {}
  }
}

const server = startServer(0);
let baseUrl = '';
let token = '';
let createdCodeValue = '';
let cloneId = null;

try {
  const address = server.address();
  baseUrl = `http://127.0.0.1:${address.port}`;

  const health = await apiJson(baseUrl, '/api/health');
  assert.equal(health.resp.status, 200, `health failed: ${JSON.stringify(health.data)}`);
  assert.equal(health.data.ok, true);

  const login = await apiJson(baseUrl, '/api/admin/login', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ hash: adminHash })
  });
  assert.equal(login.resp.status, 200, `login failed: ${JSON.stringify(login.data)}`);
  token = login.data.token;
  assert.ok(token, 'missing admin token');

  const adminHeaders = { 'Content-Type': 'application/json', 'X-Admin-Token': token };
  const generate = await apiJson(baseUrl, '/api/admin/generate', { method: 'POST', headers: adminHeaders, body: JSON.stringify({ count: 1 }) });
  assert.equal(generate.resp.status, 200, `generate failed: ${JSON.stringify(generate.data)}`);
  assert.equal(generate.data.ok, true);

  const codes = await apiJson(baseUrl, '/api/admin/codes', { headers: { 'X-Admin-Token': token } });
  assert.equal(codes.resp.status, 200, `codes failed: ${JSON.stringify(codes.data)}`);
  const createdCode = (codes.data.codes || []).find((row) => row.status === 'unused');
  assert.ok(createdCode?.code, 'no generated code available');
  createdCodeValue = createdCode.code;

  const validate = await apiJson(baseUrl, '/api/validate', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code: createdCodeValue })
  });
  assert.equal(validate.resp.status, 200, `validate failed: ${JSON.stringify(validate.data)}`);
  assert.equal(validate.data.valid, true);

  const start = await apiJson(baseUrl, '/api/session/start', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code: createdCodeValue })
  });
  assert.equal(start.resp.status, 200, `start failed: ${JSON.stringify(start.data)}`);
  assert.ok(start.data.examToken, 'missing exam token');
  const examHeaders = { 'Content-Type': 'application/json', 'X-Exam-Token': start.data.examToken };

  const questions = [];
  for (let i = 0; i < start.data.total; i += 1) {
    const q = await apiJson(baseUrl, `/api/question/${i}`, { headers: examHeaders });
    assert.equal(q.resp.status, 200, `question ${i + 1} failed: ${JSON.stringify(q.data)}`);
    assert.ok(Array.isArray(q.data.opts) && q.data.opts.length >= 2, `question ${i + 1} options missing`);
    assert.ok(Number.isInteger(q.data.requiredSelections) && q.data.requiredSelections >= 1, `question ${i + 1} requiredSelections missing`);
    assert.ok(q.data.requiredSelections <= q.data.opts.length, `question ${i + 1} selection count exceeds options`);
    questions.push(q.data);
  }

  const progress = {
    code: createdCodeValue,
    answers: Array.from({ length: start.data.total }, () => []), visited: [0], currentQ: 0,
    incidents: [], tabSwitches: 0, elapsedMs: 5000
  };
  progress.answers[0] = Array.from({ length: questions[0].requiredSelections }, (_, i) => i);
  const save = await apiJson(baseUrl, '/api/progress', { method: 'POST', headers: examHeaders, body: JSON.stringify(progress) });
  assert.equal(save.resp.status, 200, `progress failed: ${JSON.stringify(save.data)}`);

  // Explicitly prove the server rejects an over-selection even if a modified
  // browser bypasses the client-side cap.
  const multiIdx = questions.findIndex((q) => q.requiredSelections < q.opts.length);
  if (multiIdx >= 0) {
    const invalid = Array.from({ length: start.data.total }, () => []);
    invalid[multiIdx] = Array.from({ length: questions[multiIdx].requiredSelections + 1 }, (_, i) => i);
    const rejected = await apiJson(baseUrl, '/api/progress', {
      method: 'POST', headers: examHeaders,
      body: JSON.stringify({ ...progress, answers: invalid, currentQ: multiIdx })
    });
    assert.equal(rejected.resp.status, 400, `over-selection was not rejected: ${JSON.stringify(rejected.data)}`);
    assert.equal(rejected.data.error, 'too_many_selections');
  }

  const resume = await apiJson(baseUrl, '/api/validate', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ code: createdCodeValue })
  });
  assert.equal(resume.resp.status, 200, `resume validate failed: ${JSON.stringify(resume.data)}`);
  assert.equal(resume.data.status, 'active');

  const finalAnswers = questions.map((q) => Array.from({ length: q.requiredSelections }, (_, i) => i));
  const submit = await apiJson(baseUrl, '/api/submit', {
    method: 'POST', headers: examHeaders,
    body: JSON.stringify({ code: createdCodeValue, answers: finalAnswers, durationSecs: 60, tabSwitches: 0, incidents: [], autoSubmit: false })
  });
  assert.equal(submit.resp.status, 200, `submit failed: ${JSON.stringify(submit.data)}`);
  assert.equal(submit.data.ok, true);
  assert.ok(submit.data.result?.submittedAt, 'result missing submittedAt');

  const review = await apiJson(baseUrl, `/api/admin/results/${encodeURIComponent(createdCodeValue)}/review`, { headers: { 'X-Admin-Token': token } });
  assert.equal(review.resp.status, 200, `review failed: ${JSON.stringify(review.data)}`);
  assert.equal(review.data.ok, true);

  const signedSummary = await apiJson(baseUrl, `/api/admin/results/${encodeURIComponent(createdCodeValue)}/signed-summary`, { headers: { 'X-Admin-Token': token } });
  assert.equal(signedSummary.resp.status, 200, `signed summary failed: ${JSON.stringify(signedSummary.data)}`);
  assert.equal(signedSummary.data.ok, true);
  assert.ok(signedSummary.data.signature, 'missing signed summary signature');

  const verifySummary = await apiJson(baseUrl, '/api/admin/results/verify-signature', {
    method: 'POST', headers: adminHeaders,
    body: JSON.stringify({ payload: signedSummary.data.payload, signature: signedSummary.data.signature })
  });
  assert.equal(verifySummary.resp.status, 200, `signature verify failed: ${JSON.stringify(verifySummary.data)}`);
  assert.equal(verifySummary.data.valid, true);

  for (const endpoint of ['/api/admin/notifications', '/api/admin/analytics/overview?days=30', '/api/admin/metrics', '/api/admin/audit/export.json']) {
    const response = await apiJson(baseUrl, endpoint, { headers: { 'X-Admin-Token': token } });
    assert.equal(response.resp.status, 200, `${endpoint} failed: ${JSON.stringify(response.data)}`);
    assert.equal(response.data.ok, true);
  }

  const questionSets = await apiJson(baseUrl, '/api/admin/question-sets', { headers: { 'X-Admin-Token': token } });
  assert.equal(questionSets.resp.status, 200, `question sets failed: ${JSON.stringify(questionSets.data)}`);
  const sourceSet = (questionSets.data.sets || []).find((row) => row.questionCount > 0) || (questionSets.data.sets || [])[0];
  assert.ok(sourceSet?.id, 'no question set available for smoke test');

  const questionSetExport = await apiJson(baseUrl, `/api/admin/question-sets/${sourceSet.id}/export.json`, { headers: { 'X-Admin-Token': token } });
  assert.equal(questionSetExport.resp.status, 200, `question set export failed: ${JSON.stringify(questionSetExport.data)}`);
  assert.equal(questionSetExport.data.ok, true);

  const uploadPreview = await apiJson(baseUrl, '/api/admin/question-sets/upload/preview', {
    method: 'POST', headers: adminHeaders,
    body: JSON.stringify({ questions: [{ qNum: 1, stem: `Smoke preview ${Date.now()}?`, opts: ['One', 'Two'], correctIndices: [0], multi: false }] })
  });
  assert.equal(uploadPreview.resp.status, 200, `upload preview failed: ${JSON.stringify(uploadPreview.data)}`);
  assert.equal(uploadPreview.data.ok, true);

  const clone = await apiJson(baseUrl, `/api/admin/question-sets/${sourceSet.id}/clone`, {
    method: 'POST', headers: adminHeaders,
    body: JSON.stringify({ name: `Smoke Clone ${Date.now()}`, description: 'Temporary smoke clone' })
  });
  assert.equal(clone.resp.status, 200, `clone failed: ${JSON.stringify(clone.data)}`);
  assert.equal(clone.data.ok, true);
  cloneId = clone.data.questionSet?.id;
  assert.ok(cloneId, 'missing cloned question set id');

  const archiveClone = await apiJson(baseUrl, `/api/admin/question-sets/${cloneId}/archive`, { method: 'POST', headers: adminHeaders, body: '{}' });
  assert.equal(archiveClone.resp.status, 200, `archive failed: ${JSON.stringify(archiveClone.data)}`);
  const deleteClone = await apiJson(baseUrl, `/api/admin/question-sets/${cloneId}`, { method: 'DELETE', headers: { 'X-Admin-Token': token } });
  assert.equal(deleteClone.resp.status, 200, `delete clone failed: ${JSON.stringify(deleteClone.data)}`);
  cloneId = null;

  console.log(JSON.stringify({ ok: true, baseUrl, code: createdCodeValue, examName: validate.data.examName, total: start.data.total, submittedPct: submit.data.result?.pct ?? null }, null, 2));
} finally {
  await cleanupFixture(baseUrl, token, createdCodeValue, cloneId);
  stopBackgroundJobs();
  await new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
}
