'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  makePRNG,
  seededShuffle,
  buildOrdering,
  pickQuestionsForSession,
  gradeExamFromSession
} = require('../shared/scoring.js');

/* ----------------------------------------------------------------------------
 * Helpers for fixture building
 * -------------------------------------------------------------------------- */

function makeQuestion(index, opts, answer, extras = {}) {
  return {
    questionId: 1000 + index,
    questionIndex: index,
    stem: `Question ${index} stem`,
    note: null,
    opts: opts.slice(),
    answer: answer.slice(),
    multi: answer.length > 1,
    sectionId: extras.sectionId != null ? extras.sectionId : null,
    sectionName: extras.sectionName || '',
    sectionOrder: extras.sectionOrder || 0,
    sectionDrawCount: extras.sectionDrawCount != null ? extras.sectionDrawCount : null
  };
}

function makeSession(questions, code) {
  const ordering = buildOrdering(questions, code);
  const answerKey = questions.map((q) => q.answer.slice());
  return {
    code,
    qOrder: ordering.qOrder,
    optOrders: ordering.optOrders,
    answerKey,
    questions: questions.slice(),
    passPct: 80,
    total: questions.length
  };
}

/* ----------------------------------------------------------------------------
 * makePRNG and seededShuffle
 * -------------------------------------------------------------------------- */

test('makePRNG: deterministic for the same seed', () => {
  const a = makePRNG('ABC123');
  const b = makePRNG('ABC123');
  for (let i = 0; i < 10; i += 1) {
    assert.equal(a(), b());
  }
});

test('makePRNG: different seeds produce different streams', () => {
  const a = makePRNG('CODE1');
  const b = makePRNG('CODE2');
  // At least one of the first 10 values must differ.
  let diff = false;
  for (let i = 0; i < 10; i += 1) {
    if (a() !== b()) { diff = true; break; }
  }
  assert.equal(diff, true);
});

test('seededShuffle: does not mutate the input array', () => {
  const arr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const snapshot = arr.slice();
  const rng = makePRNG('seed');
  seededShuffle(arr, rng);
  assert.deepEqual(arr, snapshot);
});

test('seededShuffle: result is a permutation of the input', () => {
  const arr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  const rng = makePRNG('seed');
  const shuffled = seededShuffle(arr, rng);
  assert.equal(shuffled.length, arr.length);
  assert.equal(new Set(shuffled).size, arr.length);
  for (const v of arr) assert.ok(shuffled.includes(v));
});

/* ----------------------------------------------------------------------------
 * buildOrdering
 * -------------------------------------------------------------------------- */

test('buildOrdering: same code + same questions → same qOrder', () => {
  const qs = [
    makeQuestion(0, ['A', 'B', 'C'], [0]),
    makeQuestion(1, ['A', 'B', 'C'], [1]),
    makeQuestion(2, ['A', 'B', 'C'], [2])
  ];
  const a = buildOrdering(qs, 'CODE1');
  const b = buildOrdering(qs, 'CODE1');
  assert.deepEqual(a.qOrder, b.qOrder);
  assert.deepEqual(a.optOrders, b.optOrders);
});

test('buildOrdering: optOrders are permutations of [0..n-1]', () => {
  const qs = [
    makeQuestion(0, ['A', 'B', 'C', 'D'], [0]),
    makeQuestion(1, ['A', 'B'], [1])
  ];
  const { optOrders } = buildOrdering(qs, 'CODE1');
  for (let i = 0; i < optOrders.length; i += 1) {
    const order = optOrders[i];
    assert.equal(order.length, qs[i].opts.length);
    assert.equal(new Set(order).size, qs[i].opts.length);
  }
});

test('buildOrdering: different codes produce different qOrder (probabilistically)', () => {
  const qs = Array.from({ length: 10 }, (_, i) => makeQuestion(i, ['A', 'B', 'C'], [0]));
  const a = buildOrdering(qs, 'AAA');
  const b = buildOrdering(qs, 'ZZZ');
  // With 10 questions, the chance of a collision is ~1/10!; just assert they differ
  // somewhere. If they happen to match, the test is fragile but functionally still
  // safe (the resume logic would still work because the second call uses fresh code).
  assert.notDeepEqual(a.qOrder, b.qOrder);
});

/* ----------------------------------------------------------------------------
 * pickQuestionsForSession
 * -------------------------------------------------------------------------- */

test('pickQuestionsForSession: returns all when numQuestions is null', () => {
  const qs = Array.from({ length: 5 }, (_, i) => makeQuestion(i, ['A', 'B'], [0]));
  const result = pickQuestionsForSession({ id: 1, numQuestions: null, questions: qs }, 'CODE');
  assert.equal(result.length, 5);
  // Sorted by questionIndex
  for (let i = 0; i < result.length - 1; i += 1) {
    assert.ok(result[i].questionIndex <= result[i + 1].questionIndex);
  }
});

test('pickQuestionsForSession: returns numQuestions when set and smaller than total', () => {
  const qs = Array.from({ length: 10 }, (_, i) => makeQuestion(i, ['A', 'B'], [0]));
  const result = pickQuestionsForSession({ id: 1, numQuestions: 3, questions: qs }, 'CODE');
  assert.equal(result.length, 3);
});

test('pickQuestionsForSession: deterministic for the same code', () => {
  const qs = Array.from({ length: 20 }, (_, i) => makeQuestion(i, ['A', 'B'], [0]));
  const set = { id: 7, numQuestions: 5, questions: qs };
  const a = pickQuestionsForSession(set, 'SEED-CODE');
  const b = pickQuestionsForSession(set, 'SEED-CODE');
  assert.deepEqual(a.map((q) => q.questionId), b.map((q) => q.questionId));
});

test('pickQuestionsForSession: throws on empty question set', () => {
  assert.throws(
    () => pickQuestionsForSession({ id: 1, numQuestions: 5, questions: [] }, 'CODE'),
    /empty/i
  );
});

test('pickQuestionsForSession: respects per-section DRAW_COUNT quotas as floors', () => {
  // Quotas are minimums: at least 1 from section A, at least 2 from section B.
  // If the quotas alone don't reach the target, the function fills from the
  // remainder (any section), so totals are: A in [1, 2], B in [2, 3], sum == 5.
  const qs = [
    makeQuestion(0, ['A', 'B'], [0], { sectionId: 10, sectionName: 'A', sectionOrder: 0, sectionDrawCount: 1 }),
    makeQuestion(1, ['A', 'B'], [0], { sectionId: 10, sectionName: 'A', sectionOrder: 0, sectionDrawCount: 1 }),
    makeQuestion(2, ['A', 'B'], [0], { sectionId: 20, sectionName: 'B', sectionOrder: 1, sectionDrawCount: 2 }),
    makeQuestion(3, ['A', 'B'], [0], { sectionId: 20, sectionName: 'B', sectionOrder: 1, sectionDrawCount: 2 }),
    makeQuestion(4, ['A', 'B'], [0], { sectionId: 20, sectionName: 'B', sectionOrder: 1, sectionDrawCount: 2 })
  ];
  const set = { id: 1, numQuestions: 5, questions: qs };
  const result = pickQuestionsForSession(set, 'CODE');
  const fromA = result.filter((q) => q.sectionId === 10).length;
  const fromB = result.filter((q) => q.sectionId === 20).length;
  assert.equal(result.length, 5);
  // Floor: each section contributes at least its quota
  assert.ok(fromA >= 1, `expected at least 1 from A, got ${fromA}`);
  assert.ok(fromB >= 2, `expected at least 2 from B, got ${fromB}`);
  // Ceiling: cannot exceed the section's total question count
  assert.ok(fromA <= 2, `expected at most 2 from A, got ${fromA}`);
  assert.ok(fromB <= 3, `expected at most 3 from B, got ${fromB}`);
  // Sum check
  assert.equal(fromA + fromB, 5);
});

test('pickQuestionsForSession: when quotas sum to target exactly, result equals quotas', () => {
  const qs = [
    makeQuestion(0, ['A', 'B'], [0], { sectionId: 10, sectionName: 'A', sectionOrder: 0, sectionDrawCount: 1 }),
    makeQuestion(1, ['A', 'B'], [0], { sectionId: 10, sectionName: 'A', sectionOrder: 0, sectionDrawCount: 1 }),
    makeQuestion(2, ['A', 'B'], [0], { sectionId: 20, sectionName: 'B', sectionOrder: 1, sectionDrawCount: 2 }),
    makeQuestion(3, ['A', 'B'], [0], { sectionId: 20, sectionName: 'B', sectionOrder: 1, sectionDrawCount: 2 }),
    makeQuestion(4, ['A', 'B'], [0], { sectionId: 20, sectionName: 'B', sectionOrder: 1, sectionDrawCount: 2 })
  ];
  const set = { id: 1, numQuestions: 3, questions: qs };
  // Target 3 = 1 (A) + 2 (B) — exactly the quotas
  const result = pickQuestionsForSession(set, 'CODE');
  assert.equal(result.length, 3);
  assert.equal(result.filter((q) => q.sectionId === 10).length, 1);
  assert.equal(result.filter((q) => q.sectionId === 20).length, 2);
});

/* ----------------------------------------------------------------------------
 * gradeExamFromSession: correctness and scoring
 * -------------------------------------------------------------------------- */

function sessionFor(questions, code, passPct = 80) {
  const s = makeSession(questions, code);
  s.passPct = passPct;
  return s;
}

test('gradeExamFromSession: all correct → score=total, pass=true', () => {
  const qs = [
    makeQuestion(0, ['A', 'B', 'C'], [0]),
    makeQuestion(1, ['A', 'B', 'C'], [1]),
    makeQuestion(2, ['A', 'B', 'C'], [2])
  ];
  const session = sessionFor(qs, 'CODE');
  // All correct: for each displayIdx, the display index that maps to the answer
  const answers = session.qOrder.map((qIdx, displayIdx) => {
    const optOrder = session.optOrders[displayIdx];
    // The display index pointing to the answer index in optOrder
    return [optOrder.indexOf(qs[qIdx].answer[0])];
  });
  const result = gradeExamFromSession(session, answers);
  assert.equal(result.score, 3);
  assert.equal(result.total, 3);
  assert.equal(result.pct, 100);
  assert.equal(result.pass, true);
  assert.equal(result.questionResults.length, 3);
  assert.ok(result.questionResults.every((q) => q.correct));
});

test('gradeExamFromSession: all wrong → score=0, pass=false', () => {
  const qs = [
    makeQuestion(0, ['A', 'B', 'C'], [0]),
    makeQuestion(1, ['A', 'B', 'C'], [1])
  ];
  const session = sessionFor(qs, 'CODE');
  // All wrong: pick the first display index (which never matches the answer unless answer=0 maps to display 0)
  // Safer: pick a display index that we know is wrong by computing it.
  const answers = session.qOrder.map((qIdx, displayIdx) => {
    const optOrder = session.optOrders[displayIdx];
    const correctDisplay = optOrder.indexOf(qs[qIdx].answer[0]);
    // pick a different one
    return [correctDisplay === 0 ? 1 : 0];
  });
  const result = gradeExamFromSession(session, answers);
  assert.equal(result.score, 0);
  assert.equal(result.pct, 0);
  assert.equal(result.pass, false);
});

test('gradeExamFromSession: mixed → exact count', () => {
  const qs = [
    makeQuestion(0, ['A', 'B', 'C'], [0]),
    makeQuestion(1, ['A', 'B', 'C'], [1]),
    makeQuestion(2, ['A', 'B', 'C'], [2]),
    makeQuestion(3, ['A', 'B', 'C'], [1])
  ];
  const session = sessionFor(qs, 'CODE');
  // Get correct for the first 2, wrong for the last 2
  const correctFirst = session.qOrder.map((qIdx, displayIdx) => {
    const optOrder = session.optOrders[displayIdx];
    return [optOrder.indexOf(qs[qIdx].answer[0])];
  });
  const wrongLast = session.qOrder.map((qIdx, displayIdx) => {
    const optOrder = session.optOrders[displayIdx];
    const correctDisplay = optOrder.indexOf(qs[qIdx].answer[0]);
    return [correctDisplay === 0 ? 1 : 0];
  });
  // 2 correct + 2 wrong
  const answers = [correctFirst[0], correctFirst[1], wrongLast[2], wrongLast[3]];
  const result = gradeExamFromSession(session, answers);
  assert.equal(result.score, 2);
  assert.equal(result.pct, 50);
  assert.equal(result.pass, false);
});

test('gradeExamFromSession: multi-select is exact-match (no partial credit)', () => {
  const qs = [
    makeQuestion(0, ['A', 'B', 'C', 'D'], [0, 2])  // multi: A and C
  ];
  const session = sessionFor(qs, 'CODE');
  // For display, find display indexes of original 0 and 2
  const optOrder = session.optOrders[0];
  const displayA = optOrder.indexOf(0);
  const displayC = optOrder.indexOf(2);
  // 1) Selecting only one of the two — should be WRONG
  let result = gradeExamFromSession(session, [[displayA]]);
  assert.equal(result.score, 0);
  assert.equal(result.questionResults[0].correct, false);
  // 2) Selecting both — should be CORRECT
  result = gradeExamFromSession(session, [[displayA, displayC]]);
  assert.equal(result.score, 1);
  assert.equal(result.questionResults[0].correct, true);
  // 3) Selecting both plus an extra — should be WRONG (must not select any incorrect option)
  result = gradeExamFromSession(session, [[displayA, displayC, optOrder.indexOf(1)]]);
  assert.equal(result.score, 0);
});

test('gradeExamFromSession: out-of-range display indexes are filtered out', () => {
  const qs = [makeQuestion(0, ['A', 'B'], [0])];
  const session = sessionFor(qs, 'CODE');
  // Submit 99 (out of range) and the correct one
  const optOrder = session.optOrders[0];
  const correctDisplay = optOrder.indexOf(qs[0].answer[0]);
  const result = gradeExamFromSession(session, [[99, correctDisplay, -1]]);
  // Only the correct one should count; 99 and -1 are filtered
  assert.equal(result.score, 1);
});

test('gradeExamFromSession: passPct threshold', () => {
  const qs = Array.from({ length: 10 }, (_, i) => makeQuestion(i, ['A', 'B'], [0]));
  const session = sessionFor(qs, 'CODE', 80);
  // 8/10 = 80% → pass; 7/10 = 70% → fail
  const correctAnswers = session.qOrder.map((qIdx, displayIdx) => {
    const optOrder = session.optOrders[displayIdx];
    return [optOrder.indexOf(qs[qIdx].answer[0])];
  });
  const wrongAnswers = session.qOrder.map((qIdx, displayIdx) => {
    const optOrder = session.optOrders[displayIdx];
    const correctDisplay = optOrder.indexOf(qs[qIdx].answer[0]);
    return [correctDisplay === 0 ? 1 : 0];
  });
  // 8 correct (first 8) + 2 wrong (last 2) = 80%
  let answers = correctAnswers.slice(0, 8).concat(wrongAnswers.slice(8));
  let result = gradeExamFromSession(session, answers);
  assert.equal(result.score, 8);
  assert.equal(result.pct, 80);
  assert.equal(result.pass, true);

  // 7 correct + 3 wrong = 70%
  answers = correctAnswers.slice(0, 7).concat(wrongAnswers.slice(7));
  result = gradeExamFromSession(session, answers);
  assert.equal(result.pct, 70);
  assert.equal(result.pass, false);
});

test('gradeExamFromSession: sectionResults aggregate correctly', () => {
  const qs = [
    makeQuestion(0, ['A', 'B'], [0], { sectionId: 1, sectionName: 'A', sectionOrder: 0 }),
    makeQuestion(1, ['A', 'B'], [0], { sectionId: 1, sectionName: 'A', sectionOrder: 0 }),
    makeQuestion(2, ['A', 'B'], [0], { sectionId: 2, sectionName: 'B', sectionOrder: 1 })
  ];
  const session = sessionFor(qs, 'CODE');
  // First two correct, last one wrong
  const optOrder0 = session.optOrders[session.qOrder.indexOf(0)];
  const optOrder1 = session.optOrders[session.qOrder.indexOf(1)];
  const optOrder2 = session.optOrders[session.qOrder.indexOf(2)];
  // For each display index, provide the correct answer
  const answers = session.qOrder.map((qIdx, displayIdx) => {
    const optOrder = session.optOrders[displayIdx];
    return [optOrder.indexOf(qs[qIdx].answer[0])];
  });
  // Make the third wrong
  const lastDisplayIdx = session.qOrder.length - 1;
  // find which display index corresponds to question 2 (section B)
  // by mapping back: answers[displayIdx] is what we submitted for question session.qOrder[displayIdx]
  for (let d = 0; d < session.qOrder.length; d += 1) {
    if (session.qOrder[d] === 2) {
      const optOrder = session.optOrders[d];
      const correctDisplay = optOrder.indexOf(qs[2].answer[0]);
      answers[d] = [correctDisplay === 0 ? 1 : 0];
    }
  }
  const result = gradeExamFromSession(session, answers);
  assert.equal(result.sectionResults.length, 2);
  // Section A: 2/2 = 100%
  const secA = result.sectionResults.find((s) => s.sectionId === 1);
  assert.equal(secA.correct, 2);
  assert.equal(secA.total, 2);
  assert.equal(secA.pct, 100);
  // Section B: 0/1 = 0%
  const secB = result.sectionResults.find((s) => s.sectionId === 2);
  assert.equal(secB.correct, 0);
  assert.equal(secB.total, 1);
  assert.equal(secB.pct, 0);
});

test('gradeExamFromSession: questionResults expose display-space indexes for review UI', () => {
  const qs = [makeQuestion(0, ['A', 'B', 'C'], [1])];
  const session = sessionFor(qs, 'CODE');
  const optOrder = session.optOrders[0];
  const correctDisplay = optOrder.indexOf(qs[0].answer[0]);
  const result = gradeExamFromSession(session, [[correctDisplay]]);
  const qr = result.questionResults[0];
  // givenDisplay and expectedDisplay are in display-space indexes
  assert.deepEqual(qr.givenDisplay, qr.expectedDisplay);
  assert.equal(qr.expectedDisplay.length, 1);
  // displayOptions is in display order
  assert.equal(qr.displayOptions.length, 3);
  assert.equal(qr.displayOptions[correctDisplay], 'B');
});

test('gradeExamFromSession: empty answers array handled gracefully', () => {
  const qs = [makeQuestion(0, ['A', 'B'], [0])];
  const session = sessionFor(qs, 'CODE');
  const result = gradeExamFromSession(session, []);
  assert.equal(result.score, 0);
  assert.equal(result.pct, 0);
  assert.equal(result.pass, false);
  assert.equal(result.questionResults.length, 1);
  assert.equal(result.questionResults[0].correct, false);
});

test('gradeExamFromSession: null answers handled gracefully', () => {
  const qs = [makeQuestion(0, ['A', 'B'], [0])];
  const session = sessionFor(qs, 'CODE');
  const result = gradeExamFromSession(session, [null, undefined]);
  assert.equal(result.score, 0);
  assert.equal(result.pct, 0);
});

test('gradeExamFromSession: questions without sectionId do not produce section entries', () => {
  const qs = [makeQuestion(0, ['A', 'B'], [0])];
  const session = sessionFor(qs, 'CODE');
  const optOrder = session.optOrders[0];
  const correctDisplay = optOrder.indexOf(qs[0].answer[0]);
  const result = gradeExamFromSession(session, [[correctDisplay]]);
  assert.equal(result.sectionResults.length, 0);
});
