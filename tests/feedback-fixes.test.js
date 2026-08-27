'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const policy = require('../client/selection-policy.js');
const feedback = require('../client/feedback-fixes.js');
const examRoutes = require('../lib/routes/exam.js');
const adminRoutes = require('../lib/routes/admin.js');

function makeRes() {
  return {
    statusCode: 200,
    body: null,
    headers: {},
    setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; },
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; }
  };
}

function sessionWith(question) {
  return { qOrder: [0], total: 1, questions: [question] };
}

test('selection policy derives exact two/three counts without exposing answers', () => {
  assert.equal(policy.requiredSelections({ multi: true, correctIndices: [0, 2], note: 'Select exactly TWO answers.' }), 2);
  assert.equal(policy.requiredSelections({ multi: true, correctIndices: [0, 2, 4], note: 'Select exactly THREE answers.' }), 3);
  assert.equal(policy.noteCount('You must identify both answers.'), null);
  assert.equal(policy.noteCount('Select exactly THREE answers.'), 3);
});

test('multi-select is incomplete below exact count and rejects over-selection', () => {
  const q2 = { multi: true, requiredSelections: 2, opts: ['A', 'B', 'C', 'D'] };
  assert.equal(policy.isComplete([0], q2), false);
  assert.equal(policy.isComplete([0, 2], q2), true);
  assert.equal(policy.validateAnswer([0, 1, 2], q2).error, 'too_many_selections');
  assert.equal(policy.validateAnswer([0], q2, { requireComplete: true }).error, 'selection_count_incomplete');

  const q3 = { multi: true, requiredSelections: 3, opts: ['A', 'B', 'C', 'D'] };
  assert.equal(policy.isComplete([0, 1], q3), false);
  assert.equal(policy.isComplete([0, 1, 2], q3), true);
  assert.equal(policy.selectionLabel([0, 1], q3), '2/3 selected');
});

test('server selection guard rejects manual under/over selection but allows incomplete auto-submit', () => {
  const question = { multi: true, correctIndices: [0, 1], opts: ['A', 'B', 'C'] };
  const guard = examRoutes.selectionGuard({ requireComplete: true });

  const underRes = makeRes();
  guard({ examSession: sessionWith(question), body: { answers: [[0]], autoSubmit: false } }, underRes, () => assert.fail('under-selection passed'));
  assert.equal(underRes.statusCode, 400);
  assert.equal(underRes.body.error, 'selection_count_incomplete');
  assert.equal(underRes.body.requiredSelections, 2);

  const overRes = makeRes();
  guard({ examSession: sessionWith(question), body: { answers: [[0, 1, 2]], autoSubmit: false } }, overRes, () => assert.fail('over-selection passed'));
  assert.equal(overRes.statusCode, 400);
  assert.equal(overRes.body.error, 'too_many_selections');

  const autoRes = makeRes();
  let autoPassed = false;
  guard({ examSession: sessionWith(question), body: { answers: [[0]], autoSubmit: true } }, autoRes, () => { autoPassed = true; });
  assert.equal(autoPassed, true);
});

test('question response exposes only required selection count', () => {
  const req = { params: { displayIdx: '0' }, examSession: sessionWith({ multi: true, correctIndices: [1, 2], opts: ['A', 'B', 'C'] }) };
  const res = makeRes();
  let passed = false;
  examRoutes.exposeRequiredSelections(req, res, () => { passed = true; });
  assert.equal(passed, true);
  res.json({ displayIdx: 0, multi: true, opts: ['A', 'B', 'C'] });
  assert.equal(res.body.requiredSelections, 2);
  assert.equal(Object.hasOwn(res.body, 'correctIndices'), false);
});

test('legacy login warnings explicitly distinguish SAP SSO/RBAC', () => {
  const res = makeRes();
  adminRoutes.clarifyLegacyLoginWarnings({}, res, () => {});
  res.json({ warnings: [
    'REVIEWER_HASH is not configured. Reviewer login is disabled.',
    'CONTENT_EDITOR_HASH is not configured. Content editor login is disabled.'
  ] });
  assert.match(res.body.warnings[0], /legacy password/i);
  assert.match(res.body.warnings[0], /SAP SSO\/RBAC access is unaffected/i);
  assert.match(res.body.warnings[1], /SAP SSO\/RBAC access is unaffected/i);
});

test('candidate palette meets WCAG AA contrast targets for normal text', () => {
  assert.ok(feedback.contrastRatio('#172033', '#ffffff') >= 4.5, 'question text contrast below AA');
  assert.ok(feedback.contrastRatio('#24344d', '#ffffff') >= 4.5, 'option text contrast below AA');
  assert.ok(feedback.contrastRatio('#704300', '#fff8e6') >= 4.5, 'multi-select instruction contrast below AA');
  assert.ok(feedback.contrastRatio('#56627a', '#ffffff') >= 4.5, 'navigation help contrast below AA');
});

test('status chip fallback is total and does not throw for partial admin module loads', () => {
  assert.match(feedback.fallbackStatusChip({ status: 'completed', pass: true }), /PASS/);
  assert.match(feedback.fallbackStatusChip({ status: 'completed', pass: false }), /FAIL/);
  assert.match(feedback.fallbackStatusChip({ status: 'active' }), /ACTIVE/);
  assert.match(feedback.fallbackStatusChip({ status: 'unused' }), /UNUSED/);
  assert.doesNotThrow(() => feedback.fallbackStatusChip({}));
});

test('smoke test always cleans temporary clone and access code in finally', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'scripts', 'smoke-test.mjs'), 'utf8');
  assert.match(source, /async function cleanupFixture/);
  assert.match(source, /finally\s*\{[\s\S]*cleanupFixture\(baseUrl, token, createdCodeValue, cloneId\)/);
  assert.match(source, /Smoke Clone \$\{Date\.now\(\)\}/);
  assert.match(source, /\/api\/admin\/codes\/\$\{encodeURIComponent\(code\)\}/);
});
