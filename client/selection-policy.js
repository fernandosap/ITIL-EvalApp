/* eslint-disable no-console */
(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.IE = root.IE || {};
  root.IE.selectionPolicy = api;
})(typeof window !== 'undefined' ? window : globalThis, function () {
  const WORDS = { one: 1, two: 2, three: 3, four: 4, five: 5, six: 6 };

  function noteCount(note) {
    const text = String(note || '').toLowerCase();
    const match = text.match(/(?:select|choose|identify)\s+(?:exactly\s+)?(one|two|three|four|five|six|\d+)\b/i);
    if (!match) return null;
    const raw = String(match[1]).toLowerCase();
    const value = WORDS[raw] || Number(raw);
    return Number.isInteger(value) && value > 0 ? value : null;
  }

  function requiredSelections(question) {
    if (!question || typeof question !== 'object') return 1;
    const explicit = Number(question.requiredSelections);
    if (Number.isInteger(explicit) && explicit > 0) return explicit;
    if (!question.multi) return 1;
    // Persisted server questions expose their answer key internally as
    // `answer`; upload/validation helpers use `correctIndices`. Either one is
    // authoritative for cardinality, but neither is ever sent to the browser.
    if (Array.isArray(question.answer) && question.answer.length > 0) return question.answer.length;
    if (Array.isArray(question.correctIndices) && question.correctIndices.length > 0) return question.correctIndices.length;
    return noteCount(question.note) || 2;
  }

  function uniqueSelection(answer) {
    if (!Array.isArray(answer)) return [];
    const seen = new Set();
    const out = [];
    for (const value of answer) {
      const idx = Number(value);
      if (!Number.isInteger(idx) || idx < 0 || seen.has(idx)) continue;
      seen.add(idx);
      out.push(idx);
    }
    return out.sort((a, b) => a - b);
  }

  function isComplete(answer, question) {
    return uniqueSelection(answer).length === requiredSelections(question);
  }

  function validateAnswer(answer, question, options = {}) {
    const raw = Array.isArray(answer) ? answer : [];
    const normalized = uniqueSelection(raw);
    const required = requiredSelections(question);
    const optionCount = Array.isArray(question?.opts) ? question.opts.length : Number(question?.optionCount || 0);
    const hasDuplicateOrInvalid = normalized.length !== raw.length;
    const outOfRange = optionCount > 0 && normalized.some((idx) => idx >= optionCount);
    if (hasDuplicateOrInvalid || outOfRange) {
      return { ok: false, error: 'invalid_selection', required, selected: normalized.length };
    }
    if (normalized.length > required) {
      return { ok: false, error: 'too_many_selections', required, selected: normalized.length };
    }
    if (options.requireComplete && normalized.length !== required) {
      return { ok: false, error: 'selection_count_incomplete', required, selected: normalized.length };
    }
    return { ok: true, required, selected: normalized.length, complete: normalized.length === required };
  }

  function selectionLabel(answer, question) {
    const selected = uniqueSelection(answer).length;
    const required = requiredSelections(question);
    if (required <= 1) return selected ? '✓ Answered' : 'No answer';
    return `${selected}/${required} selected`;
  }

  return { noteCount, requiredSelections, uniqueSelection, isComplete, validateAnswer, selectionLabel };
});
