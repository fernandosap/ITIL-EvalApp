// Pure scoring and ordering helpers, shared between server.js and tests.
// No I/O, no HANA, no Express. Safe to require from anywhere.
'use strict';

// Deterministic 32-bit PRNG (LCG, params from Numerical Recipes).
// Used so that ordering decisions (qOrder, optOrders) are reproducible
// per access code — the same candidate resumes the same shuffled exam.
function makePRNG(seed) {
  let s = 0;
  const str = String(seed == null ? '' : seed);
  for (let i = 0; i < str.length; i += 1) {
    s = (Math.imul(s, 31) + str.charCodeAt(i)) >>> 0;
  }
  return function prng() {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 4294967296;
  };
}

// Fisher-Yates with a supplied PRNG. Does not mutate the input.
function seededShuffle(arr, rng) {
  const out = [...arr];
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rng() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

// Produce the per-session question ordering and per-question option
// ordering, fully determined by the access code. Two candidates with the
// same code and same question set get the same ordering (so resumes work).
function buildOrdering(questions, code) {
  const rng = makePRNG(code);
  const qOrder = seededShuffle(questions.map((_, idx) => idx), rng);
  const optOrders = qOrder.map((qIdx) => seededShuffle(questions[qIdx].opts.map((_, idx) => idx), rng));
  return { qOrder, optOrders };
}

// Pick the questions for a session. Honors per-section DRAW_COUNT quotas
// when present; otherwise just shuffles and slices to the requested count.
function pickQuestionsForSession(questionSet, code) {
  const allQuestions = Array.isArray(questionSet && questionSet.questions) ? [...questionSet.questions] : [];
  if (!allQuestions.length) throw new Error('Question set is empty.');

  const requested = questionSet.numQuestions == null ? allQuestions.length : Number(questionSet.numQuestions);
  const targetCount = Math.max(1, Math.min(Number.isFinite(requested) ? requested : allQuestions.length, allQuestions.length));
  if (targetCount >= allQuestions.length) {
    return allQuestions.sort((a, b) => a.questionIndex - b.questionIndex);
  }

  const rng = makePRNG(`${code}:${questionSet.id}`);
  const hasSectionQuotas = allQuestions.some((q) => q.sectionId != null && q.sectionDrawCount != null);
  if (!hasSectionQuotas) {
    return seededShuffle(allQuestions, rng)
      .slice(0, targetCount)
      .sort((a, b) => a.questionIndex - b.questionIndex);
  }

  const chosen = [];
  const used = new Set();
  const bySection = new Map();
  for (const question of allQuestions) {
    const key = question.sectionId == null ? '__unsectioned__' : String(question.sectionId);
    if (!bySection.has(key)) bySection.set(key, []);
    bySection.get(key).push(question);
  }
  for (const [key, items] of bySection.entries()) {
    const quota = key === '__unsectioned__' ? null : items[0].sectionDrawCount;
    if (quota == null) continue;
    const pickCount = Math.max(0, Math.min(Number(quota) || 0, items.length, targetCount - chosen.length));
    const sample = seededShuffle(items, rng).slice(0, pickCount);
    for (const q of sample) {
      chosen.push(q);
      used.add(q.questionId);
    }
    if (chosen.length >= targetCount) break;
  }
  if (chosen.length < targetCount) {
    const remaining = allQuestions.filter((q) => !used.has(q.questionId));
    const fill = seededShuffle(remaining, rng).slice(0, targetCount - chosen.length);
    chosen.push(...fill);
  }
  return chosen
    .slice(0, targetCount)
    .sort((a, b) => a.questionIndex - b.questionIndex);
}

// Grade a completed exam session. The session is the immutable, per-
// candidate state captured at session start (qOrder, optOrders, answerKey,
// questions, passPct, total). `answers` is the candidate's selections in
// display order, where answers[displayIdx] is an array of selected
// option indexes (display-space, not original).
//
// Returns: { score, total, pct, pass, questionResults, sectionResults }.
// Multi-select grading is exact-match: a question is correct only if
// the candidate selected every correct option AND no incorrect option.
function gradeExamFromSession(session, answers) {
  const answerKey = Array.isArray(session && session.answerKey) ? session.answerKey : [];
  let score = 0;
  const questionResults = [];
  const sectionMap = new Map();

  session.qOrder.forEach((questionIdx, displayIdx) => {
    const displaySelection = Array.isArray(answers && answers[displayIdx]) ? answers[displayIdx].map(Number) : [];
    const optionOrder = session.optOrders[displayIdx];
    const originalSelection = displaySelection
      .filter((idx) => Number.isInteger(idx) && idx >= 0 && idx < optionOrder.length)
      .map((idx) => optionOrder[idx])
      .sort((a, b) => a - b);
    const expected = (answerKey[questionIdx] || []).slice().sort((a, b) => a - b);
    const correct = originalSelection.join(',') === expected.join(',');
    if (correct) score += 1;
    const question = session.questions && session.questions[questionIdx];
    const displayOptions = Array.isArray(optionOrder)
      ? optionOrder.map((idx) => String((question && question.opts && question.opts[idx]) || ''))
      : ((question && question.opts) || []).map((opt) => String(opt));
    const toDisplayIndexes = (originalIndexes) => originalIndexes
      .map((originalIdx) => optionOrder.findIndex((idx) => idx === originalIdx))
      .filter((idx) => idx >= 0)
      .sort((a, b) => a - b);
    questionResults.push({
      displayIdx,
      questionIndex: question ? question.questionIndex : questionIdx,
      questionId: question ? question.questionId : null,
      correct,
      given: originalSelection,
      expected,
      givenDisplay: toDisplayIndexes(originalSelection),
      expectedDisplay: toDisplayIndexes(expected),
      stem: question ? question.stem : '',
      note: question ? question.note : null,
      opts: question && Array.isArray(question.opts) ? question.opts.map((opt) => String(opt)) : [],
      displayOptions,
      multi: question ? Boolean(question.multi) : false,
      sectionId: question ? question.sectionId : null,
      sectionName: question ? question.sectionName : ''
    });
    if (question && question.sectionId != null) {
      const key = String(question.sectionId);
      if (!sectionMap.has(key)) {
        sectionMap.set(key, {
          sectionId: question.sectionId,
          name: question.sectionName || 'Section',
          displayOrder: Number(question.sectionOrder || 0),
          correct: 0,
          total: 0
        });
      }
      const section = sectionMap.get(key);
      section.total += 1;
      if (correct) section.correct += 1;
    }
  });

  const total = Number((session && session.total) || ((session && session.questions && session.questions.length) || 0));
  const pct = total > 0 ? Math.round((score / total) * 100) : 0;
  const pass = pct >= Number((session && session.passPct) || 80);
  const sectionResults = Array.from(sectionMap.values())
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .map((section) => ({
      sectionId: section.sectionId,
      name: section.name,
      correct: section.correct,
      total: section.total,
      pct: section.total ? Math.round((section.correct / section.total) * 100) : 0
    }));
  return { score, total, pct, pass, questionResults, sectionResults };
}

module.exports = {
  makePRNG,
  seededShuffle,
  buildOrdering,
  pickQuestionsForSession,
  gradeExamFromSession
};
