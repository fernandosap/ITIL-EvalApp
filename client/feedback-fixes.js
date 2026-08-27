/* eslint-disable no-console */
(function (root, factory) {
  const api = factory(root);
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.IE = root.IE || {};
  root.IE.feedbackFixes = api;
})(typeof window !== 'undefined' ? window : globalThis, function (root) {
  let installed = false;
  let observer = null;

  function fallbackStatusChip(row = {}) {
    if (row.status === 'completed' && row.isPractice) return '<span class="chip chip-pass">PRACTICE</span>';
    if (row.status === 'completed') return `<span class="chip ${row.pass ? 'chip-pass' : 'chip-fail'}">${row.pass ? 'PASS' : 'FAIL'}</span>`;
    if (row.status === 'active') return '<span class="chip chip-active">ACTIVE</span>';
    return '<span class="chip chip-unused">UNUSED</span>';
  }

  function luminance(hex) {
    const value = String(hex || '').replace('#', '');
    if (!/^[0-9a-f]{6}$/i.test(value)) return 0;
    const parts = [0, 2, 4].map((i) => parseInt(value.slice(i, i + 2), 16) / 255)
      .map((v) => (v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4)));
    return 0.2126 * parts[0] + 0.7152 * parts[1] + 0.0722 * parts[2];
  }

  function contrastRatio(a, b) {
    const l1 = luminance(a);
    const l2 = luminance(b);
    return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
  }

  function applyAccessibleContrast(doc = root.document) {
    if (!doc?.querySelectorAll) return;
    doc.querySelectorAll('.q-stem').forEach((el) => { el.style.color = '#172033'; });
    doc.querySelectorAll('.option').forEach((el) => {
      el.style.color = '#24344d';
      el.style.backgroundColor = '#ffffff';
      el.style.borderColor = '#7889a8';
      el.style.opacity = '1';
      if (el.classList.contains('selected')) {
        el.style.backgroundColor = '#e8f1ff';
        el.style.borderColor = '#1f4f8a';
        el.style.boxShadow = '0 0 0 2px rgba(31,79,138,.14)';
      }
    });
    doc.querySelectorAll('.multi-note').forEach((el) => {
      el.style.color = '#704300';
      el.style.backgroundColor = '#fff8e6';
      el.style.borderColor = '#d59a2d';
    });
    doc.querySelectorAll('.nav-help,.sel-count').forEach((el) => { el.style.color = '#56627a'; });
  }

  function rememberRequirement() {
    const S = root.S;
    const policy = root.IE?.selectionPolicy;
    if (!S || !policy || !S.currentQCache) return;
    S.questionRequirements = S.questionRequirements || {};
    S.questionRequirements[S.currentQ] = policy.requiredSelections(S.currentQCache);
  }

  function updateSelectionUi() {
    const S = root.S;
    const policy = root.IE?.selectionPolicy;
    const doc = root.document;
    if (!S || !policy || !doc || S.screen !== 'exam') return;
    rememberRequirement();
    const q = S.currentQCache;
    if (!q) return;
    const answer = S.answers?.[S.currentQ] || [];
    const label = policy.selectionLabel(answer, q);
    const selectedCount = policy.uniqueSelection(answer).length;
    const required = policy.requiredSelections(q);
    doc.querySelectorAll('.sel-count').forEach((el) => {
      // MutationObserver watches child-list changes. Rewriting the same text on
      // every callback creates a self-sustaining mutation loop and can freeze
      // the browser main thread. Only mutate DOM when the value truly changed.
      if (el.textContent !== label) el.textContent = label;
      el.style.fontWeight = '700';
      el.style.color = selectedCount === required ? '#1f5f2c' : '#8a5b00';
    });
    const meta = doc.querySelector('.q-meta');
    if (meta && q.multi && !meta.querySelector('[data-selection-requirement]')) {
      const chip = doc.createElement('span');
      chip.dataset.selectionRequirement = '1';
      chip.textContent = `Select exactly ${required}`;
      chip.style.fontSize = '12px';
      chip.style.fontWeight = '800';
      chip.style.color = '#704300';
      chip.style.background = '#fff8e6';
      chip.style.border = '1px solid #d59a2d';
      chip.style.borderRadius = '999px';
      chip.style.padding = '4px 8px';
      meta.appendChild(chip);
    }
    applyAccessibleContrast(doc);
  }

  function patchExamActions() {
    const exam = root.IE?.exam;
    const policy = root.IE?.selectionPolicy;
    if (!exam || !policy || exam.__exactSelectionPatched) return;
    const originalPick = exam.pick;
    const originalTrySubmit = exam.trySubmit;

    exam.pick = function pickWithLimit(displayOptIdx) {
      const S = root.S;
      const q = S?.currentQCache;
      if (q?.multi) {
        const answer = Array.isArray(S.answers?.[S.currentQ]) ? S.answers[S.currentQ] : [];
        const normalized = policy.uniqueSelection(answer);
        const required = policy.requiredSelections(q);
        const idx = Number(displayOptIdx);
        if (!normalized.includes(idx) && normalized.length >= required) {
          root.IE?.util?.modal?.('ℹ️', 'Selection limit reached', `This question requires exactly ${required} answers. Deselect one before choosing another option.`, [{ label: 'OK', cls: 'btn-primary' }]);
          return;
        }
      }
      return originalPick(displayOptIdx);
    };

    exam.trySubmit = function trySubmitExact() {
      const S = root.S;
      const requirements = S?.questionRequirements || {};
      const incomplete = [];
      for (let i = 0; i < Number(S?.total || 0); i += 1) {
        const answer = Array.isArray(S.answers?.[i]) ? S.answers[i] : [];
        const knownRequired = requirements[i] || (i === S.currentQ && S.currentQCache ? policy.requiredSelections(S.currentQCache) : null);
        const required = Number(knownRequired || (answer.length > 0 ? policy.uniqueSelection(answer).length : 1));
        if (policy.uniqueSelection(answer).length !== required) incomplete.push(i + 1);
      }
      if (incomplete.length) {
        root.IE?.util?.modal?.('⚠️', 'Incomplete answers', `${incomplete.length} question${incomplete.length === 1 ? '' : 's'} do not have the required number of selections. Please review question${incomplete.length === 1 ? '' : 's'} ${incomplete.slice(0, 8).join(', ')}${incomplete.length > 8 ? '…' : ''}.`, [{ label: 'Go Back', cls: 'btn-primary' }]);
        return;
      }
      return originalTrySubmit();
    };
    exam.__exactSelectionPatched = true;
  }

  function loadScript(src) {
    if (!root.document) return Promise.resolve(false);
    return new Promise((resolve) => {
      const script = root.document.createElement('script');
      script.src = src;
      script.async = false;
      script.onload = () => resolve(true);
      script.onerror = () => resolve(false);
      root.document.head.appendChild(script);
    });
  }

  async function ensureAdminStatusChip() {
    root.IE = root.IE || {};
    root.IE.exam = root.IE.exam || {};
    if (typeof root.IE.exam.statusChip === 'function') return true;
    root.IE.exam.statusChip = fallbackStatusChip;
    await loadScript(`/client/admin-codes.js?dependency-repair=${Date.now()}`);
    return typeof root.IE?.admin?.showAdmin === 'function';
  }

  function install() {
    if (installed) return;
    installed = true;
    patchExamActions();
    if (root.document?.documentElement && typeof MutationObserver !== 'undefined') {
      observer = new MutationObserver(() => {
        patchExamActions();
        updateSelectionUi();
      });
      observer.observe(root.document.documentElement, { childList: true, subtree: true });
    }
    updateSelectionUi();
  }

  function stop() {
    if (observer) observer.disconnect();
    observer = null;
    installed = false;
  }

  return { fallbackStatusChip, luminance, contrastRatio, applyAccessibleContrast, ensureAdminStatusChip, install, stop };
});
