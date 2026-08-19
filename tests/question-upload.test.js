'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

// validateQuestionUploadEntries is exported from server.js. We require
// the server module here; it does NOT start the server (we never call
// startServer()), it just loads the module and its helpers.
const { validateQuestionUploadEntries } = require('../server.js');

function makeValidQuestion(overrides = {}) {
  return {
    qNum: 1,
    stem: 'What is 2+2?',
    note: '',
    multi: false,
    opts: ['3', '4', '5', '6'],
    correctIndices: [1],
    ...overrides
  };
}

test('validateQuestionUploadEntries: single valid question → ok', () => {
  const result = validateQuestionUploadEntries([makeValidQuestion()]);
  assert.equal(result.ok, true);
  assert.deepEqual(result.errors, []);
  assert.equal(result.normalized.length, 1);
});

test('validateQuestionUploadEntries: rejects non-array input', () => {
  const result = validateQuestionUploadEntries(null);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /at least one question/i.test(e)));
});

test('validateQuestionUploadEntries: empty array → "at least one" error', () => {
  const result = validateQuestionUploadEntries([]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /at least one/i.test(e)));
});

test('validateQuestionUploadEntries: rejects more than 500 questions', () => {
  const tooMany = [];
  for (let i = 1; i <= 501; i += 1) {
    tooMany.push(makeValidQuestion({ qNum: i }));
  }
  const result = validateQuestionUploadEntries(tooMany);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /500 questions/i.test(e)));
});

test('validateQuestionUploadEntries: rejects missing/invalid qNum', () => {
  const result = validateQuestionUploadEntries([makeValidQuestion({ qNum: 0 })]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /Row 2.*q_num/i.test(e)));
});

test('validateQuestionUploadEntries: rejects missing stem', () => {
  const result = validateQuestionUploadEntries([makeValidQuestion({ stem: '' })]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /Row 2.*stem is required/i.test(e)));
});

test('validateQuestionUploadEntries: rejects stem > 2000 chars', () => {
  const longStem = 'a'.repeat(2001);
  const result = validateQuestionUploadEntries([makeValidQuestion({ stem: longStem })]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /exceeds 2000 characters/i.test(e)));
});

test('validateQuestionUploadEntries: warns on note > 1000 chars', () => {
  const result = validateQuestionUploadEntries([makeValidQuestion({ note: 'n'.repeat(1001) })]);
  assert.equal(result.ok, true);
  assert.ok(result.warnings.some((w) => /note is longer than 1000/i.test(w)));
});

test('validateQuestionUploadEntries: rejects fewer than 2 options', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ opts: ['only one'], correctIndices: [0] })
  ]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /at least two options/i.test(e)));
});

test('validateQuestionUploadEntries: rejects more than 6 options', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ opts: ['a', 'b', 'c', 'd', 'e', 'f', 'g'], correctIndices: [0] })
  ]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /maximum six options/i.test(e)));
});

test('validateQuestionUploadEntries: rejects empty correctIndices', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ correctIndices: [] })
  ]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /correct_indices is required/i.test(e)));
});

test('validateQuestionUploadEntries: single-select must have exactly one correct', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ multi: false, correctIndices: [0, 1] })
  ]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /single-select.*exactly one/i.test(e)));
});

test('validateQuestionUploadEntries: multi-select allows multiple correct', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ multi: true, correctIndices: [0, 1] })
  ]);
  assert.equal(result.ok, true);
  assert.deepEqual(result.errors, []);
});

test('validateQuestionUploadEntries: rejects correct index out of range', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ correctIndices: [4] })  // 4 is out of range for 4 opts (0..3)
  ]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /out of range/i.test(e)));
});

test('validateQuestionUploadEntries: warns on duplicate option text (case-insensitive)', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ opts: ['Yes', 'yes', 'No'] })
  ]);
  assert.equal(result.ok, true);  // warning, not error
  assert.ok(result.warnings.some((w) => /duplicate option text/i.test(w)));
});

test('validateQuestionUploadEntries: rejects duplicate qNum', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ qNum: 1 }),
    makeValidQuestion({ qNum: 1, stem: 'A different question' })
  ]);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((e) => /Row 3.*q_num 1 duplicates/i.test(e)));
});

test('validateQuestionUploadEntries: warns on duplicate stem (case-insensitive)', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ qNum: 1, stem: 'Same question' }),
    makeValidQuestion({ qNum: 2, stem: 'same QUESTION' })
  ]);
  assert.equal(result.ok, true);  // warning
  assert.ok(result.warnings.some((w) => /Row 3.*duplicate question stem/i.test(w)));
});

test('validateQuestionUploadEntries: warns on identical option sets across rows', () => {
  const result = validateQuestionUploadEntries([
    makeValidQuestion({ qNum: 1, stem: 'Q1', opts: ['A', 'B', 'C'], correctIndices: [0] }),
    makeValidQuestion({ qNum: 2, stem: 'Q2', opts: ['A', 'B', 'C'], correctIndices: [0] })
  ]);
  assert.equal(result.ok, true);
  assert.ok(result.warnings.some((w) => /same option set appears in another question/i.test(w)));
});

test('validateQuestionUploadEntries: multiple errors are reported at once', () => {
  const result = validateQuestionUploadEntries([
    { qNum: 0, stem: '', note: '', multi: false, opts: ['only'], correctIndices: [] }
  ]);
  assert.equal(result.ok, false);
  // qNum + stem + opts (1) + correctIndices (empty) → at least 4 errors
  assert.ok(result.errors.length >= 4);
});

test('validateQuestionUploadEntries: normalizes input (trims, coerces)', () => {
  const result = validateQuestionUploadEntries([
    {
      qNum: '7',  // string instead of number
      stem: '  trimmed  ',
      note: '  note  ',
      multi: 'true',  // truthy
      opts: ['  a  ', '', 'b', 'c'],  // empty string filtered
      correctIndices: ['0', 'abc', '1', '-1']  // non-integer filtered
    }
  ]);
  assert.equal(result.ok, true);
  assert.equal(result.normalized[0].qNum, 7);
  assert.equal(result.normalized[0].stem, 'trimmed');
  assert.equal(result.normalized[0].note, 'note');
  assert.equal(result.normalized[0].multi, true);
  assert.deepEqual(result.normalized[0].opts, ['a', 'b', 'c']);
  assert.deepEqual(result.normalized[0].correctIndices, [0, 1]);
});

test('validateQuestionUploadEntries: 500 questions exactly → ok', () => {
  const at = [];
  for (let i = 1; i <= 500; i += 1) at.push(makeValidQuestion({ qNum: i }));
  const result = validateQuestionUploadEntries(at);
  assert.equal(result.ok, true);
  assert.equal(result.normalized.length, 500);
});
