import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import serverModule from '../server.js';

const { startServer, stopBackgroundJobs } = serverModule;

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

const adminHash = String(process.env.ADMIN_HASH || '').trim().toLowerCase();
if (!adminHash) {
  console.error('ADMIN_HASH missing. Smoke test needs hashed admin credential.');
  process.exit(1);
}

async function apiJson(baseUrl, url, options = {}) {
  const resp = await fetch(`${baseUrl}${url}`, options);
  const text = await resp.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_err) {
    data = { raw: text };
  }
  return { resp, data };
}

const server = startServer(0);

try {
  const address = server.address();
  const baseUrl = `http://127.0.0.1:${address.port}`;

  const health = await apiJson(baseUrl, '/api/health');
  assert.equal(health.resp.status, 200, `health failed: ${JSON.stringify(health.data)}`);
  assert.equal(health.data.ok, true);

  const login = await apiJson(baseUrl, '/api/admin/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ hash: adminHash })
  });
  assert.equal(login.resp.status, 200, `login failed: ${JSON.stringify(login.data)}`);
  const token = login.data.token;
  assert.ok(token, 'missing admin token');

  const generate = await apiJson(baseUrl, '/api/admin/generate', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Token': token
    },
    body: JSON.stringify({ count: 1 })
  });
  assert.equal(generate.resp.status, 200, `generate failed: ${JSON.stringify(generate.data)}`);
  assert.equal(generate.data.ok, true);

  const codes = await apiJson(baseUrl, '/api/admin/codes', {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(codes.resp.status, 200, `codes failed: ${JSON.stringify(codes.data)}`);
  const createdCode = (codes.data.codes || []).find((row) => row.status === 'unused');
  assert.ok(createdCode?.code, 'no generated code available');

  const validate = await apiJson(baseUrl, '/api/validate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: createdCode.code })
  });
  assert.equal(validate.resp.status, 200, `validate failed: ${JSON.stringify(validate.data)}`);
  assert.equal(validate.data.valid, true);

  const start = await apiJson(baseUrl, '/api/session/start', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: createdCode.code })
  });
  assert.equal(start.resp.status, 200, `start failed: ${JSON.stringify(start.data)}`);
  assert.ok(start.data.examToken, 'missing exam token');

  const examHeaders = {
    'Content-Type': 'application/json',
    'X-Exam-Token': start.data.examToken
  };

  const q0 = await apiJson(baseUrl, '/api/question/0', { headers: examHeaders });
  assert.equal(q0.resp.status, 200, `question failed: ${JSON.stringify(q0.data)}`);
  assert.ok(Array.isArray(q0.data.opts) && q0.data.opts.length >= 2, 'question options missing');

  const progress = {
    code: createdCode.code,
    answers: Array.from({ length: start.data.total }, () => []),
    visited: [0],
    currentQ: 0,
    incidents: [],
    tabSwitches: 0,
    elapsedMs: 5000
  };
  progress.answers[0] = [0];

  const save = await apiJson(baseUrl, '/api/progress', {
    method: 'POST',
    headers: examHeaders,
    body: JSON.stringify(progress)
  });
  assert.equal(save.resp.status, 200, `progress failed: ${JSON.stringify(save.data)}`);

  const resume = await apiJson(baseUrl, '/api/validate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: createdCode.code })
  });
  assert.equal(resume.resp.status, 200, `resume validate failed: ${JSON.stringify(resume.data)}`);
  assert.equal(resume.data.status, 'active');

  const finalAnswers = Array.from({ length: start.data.total }, () => [0]);
  const submit = await apiJson(baseUrl, '/api/submit', {
    method: 'POST',
    headers: examHeaders,
    body: JSON.stringify({
      code: createdCode.code,
      answers: finalAnswers,
      durationSecs: 60,
      tabSwitches: 0,
      incidents: [],
      autoSubmit: false
    })
  });
  assert.equal(submit.resp.status, 200, `submit failed: ${JSON.stringify(submit.data)}`);
  assert.equal(submit.data.ok, true);
  assert.ok(submit.data.result?.submittedAt, 'result missing submittedAt');

  const review = await apiJson(baseUrl, `/api/admin/results/${encodeURIComponent(createdCode.code)}/review`, {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(review.resp.status, 200, `review failed: ${JSON.stringify(review.data)}`);
  assert.equal(review.data.ok, true);

  const signedSummary = await apiJson(baseUrl, `/api/admin/results/${encodeURIComponent(createdCode.code)}/signed-summary`, {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(signedSummary.resp.status, 200, `signed summary failed: ${JSON.stringify(signedSummary.data)}`);
  assert.equal(signedSummary.data.ok, true);
  assert.ok(signedSummary.data.signature, 'missing signed summary signature');

  const verifySummary = await apiJson(baseUrl, '/api/admin/results/verify-signature', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Token': token
    },
    body: JSON.stringify({
      payload: signedSummary.data.payload,
      signature: signedSummary.data.signature
    })
  });
  assert.equal(verifySummary.resp.status, 200, `signature verify failed: ${JSON.stringify(verifySummary.data)}`);
  assert.equal(verifySummary.data.valid, true);

  const notifications = await apiJson(baseUrl, '/api/admin/notifications', {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(notifications.resp.status, 200, `notifications failed: ${JSON.stringify(notifications.data)}`);
  assert.equal(notifications.data.ok, true);

  const analytics = await apiJson(baseUrl, '/api/admin/analytics/overview?days=30', {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(analytics.resp.status, 200, `analytics failed: ${JSON.stringify(analytics.data)}`);
  assert.equal(analytics.data.ok, true);

  const metrics = await apiJson(baseUrl, '/api/admin/metrics', {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(metrics.resp.status, 200, `metrics failed: ${JSON.stringify(metrics.data)}`);
  assert.equal(metrics.data.ok, true);

  const auditExport = await apiJson(baseUrl, '/api/admin/audit/export.json', {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(auditExport.resp.status, 200, `audit export failed: ${JSON.stringify(auditExport.data)}`);
  assert.equal(auditExport.data.ok, true);
  assert.ok(auditExport.data.signature, 'missing audit export signature');

  const questionSets = await apiJson(baseUrl, '/api/admin/question-sets', {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(questionSets.resp.status, 200, `question sets failed: ${JSON.stringify(questionSets.data)}`);
  const sourceSet = (questionSets.data.sets || []).find((row) => row.questionCount > 0) || (questionSets.data.sets || [])[0];
  assert.ok(sourceSet?.id, 'no question set available for smoke test');

  const questionSetExport = await apiJson(baseUrl, `/api/admin/question-sets/${sourceSet.id}/export.json`, {
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(questionSetExport.resp.status, 200, `question set export failed: ${JSON.stringify(questionSetExport.data)}`);
  assert.equal(questionSetExport.data.ok, true);

  const uploadPreview = await apiJson(baseUrl, '/api/admin/question-sets/upload/preview', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Token': token
    },
    body: JSON.stringify({
      questions: [
        {
          qNum: 1,
          stem: `Smoke preview ${Date.now()}?`,
          opts: ['One', 'Two'],
          correctIndices: [0],
          multi: false
        }
      ]
    })
  });
  assert.equal(uploadPreview.resp.status, 200, `upload preview failed: ${JSON.stringify(uploadPreview.data)}`);
  assert.equal(uploadPreview.data.ok, true);
  assert.equal(uploadPreview.data.count, 1);

  const clone = await apiJson(baseUrl, `/api/admin/question-sets/${sourceSet.id}/clone`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Token': token
    },
    body: JSON.stringify({
      name: `Smoke Clone ${Date.now()}`,
      description: 'Temporary smoke clone'
    })
  });
  assert.equal(clone.resp.status, 200, `clone failed: ${JSON.stringify(clone.data)}`);
  assert.equal(clone.data.ok, true);
  assert.ok(clone.data.questionSet?.id, 'missing cloned question set id');

  const archiveClone = await apiJson(baseUrl, `/api/admin/question-sets/${clone.data.questionSet.id}/archive`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Token': token
    },
    body: JSON.stringify({})
  });
  assert.equal(archiveClone.resp.status, 200, `archive failed: ${JSON.stringify(archiveClone.data)}`);
  assert.equal(archiveClone.data.ok, true);

  const deleteClone = await apiJson(baseUrl, `/api/admin/question-sets/${clone.data.questionSet.id}`, {
    method: 'DELETE',
    headers: { 'X-Admin-Token': token }
  });
  assert.equal(deleteClone.resp.status, 200, `delete clone failed: ${JSON.stringify(deleteClone.data)}`);
  assert.equal(deleteClone.data.ok, true);

  const reset = await apiJson(baseUrl, '/api/admin/reset', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Token': token
    },
    body: JSON.stringify({ code: createdCode.code })
  });
  assert.equal(reset.resp.status, 200, `reset failed: ${JSON.stringify(reset.data)}`);

  console.log(JSON.stringify({
    ok: true,
    baseUrl,
    code: createdCode.code,
    examName: validate.data.examName,
    total: start.data.total,
    submittedPct: submit.data.result?.pct ?? null,
    clonedQuestionSetId: clone.data.questionSet.id
  }, null, 2));
} finally {
  stopBackgroundJobs();
  await new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
}
