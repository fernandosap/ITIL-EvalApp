/* eslint-disable no-console */
// client/exam.js — the in-exam flow: question rendering, navigation,
// answer picking, timer, submission, results page.
//
// Public surface (attached to window.IE.exam):
//   renderQ, goToQ, prevQ, nextQ, pick, trySubmit, startTimer,
//   getRemainingMs, updateTimer, submitExam, showResultsFromRecord,
//   downloadResultSummary, showPendingSubmission, retryPendingSubmission,
//   statusChip
//
// Security event listeners (keydown/contextmenu/selectstart) are wired
// here. They guard against screenshot shortcuts, right-click, and text
// selection during the exam screen.

(function (root) {
  const { $, _esc, render, modal, fmt, setSavePill, apiJson, durationLabel, connectivityBanner } = root.IE.util;
  const S = root.S;
  const { logIncident, isOnline, queueProgressSave, buildSubmitPayload,
          queuePendingAction, clearPendingState, replayPendingActions, proctorEnabled } = root.IE.state;

  // ---- Question rendering ----
  async function renderQ() {
    S.screen = 'exam';
    document.body.classList.add('exam-bg');
    S.visited.add(S.currentQ);

    let q;
    try {
      q = await apiJson(`/api/question/${S.currentQ}`);
    } catch (_e) {
      q = null;
    }
    if (!q || q.error) {
      modal('❌', 'Question Load Error', 'Could not load question. Please check your connection.', [{ label: 'Retry', cls: 'btn-primary', action: renderQ }]);
      return;
    }
    S.currentQCache = q;

    // ---- Render error boundary ----
    // The big template string below is built from server
    // data (q.stem, q.opts, q.note). A malformed response,
    // a future schema change, or a bug in the template
    // could throw mid-render. Without this try/catch, a
    // throw leaves the exam screen blank, the candidate
    // stuck, and only the global window.onerror recovery
    // screen helps (which forces a full Reload).
    //
    // The boundary renders a degraded "this question
    // failed to render" view with a Next button so the
    // candidate can skip and continue the exam. The
    // incident is reported to /api/client-errors (with
    // type='error' from window.onerror) and shown in
    // the admin client-errors dashboard.
    try {
      await _renderQBody(q);
    } catch (renderErr) {
      // Explicit telemetry report. The local catch here
      // would otherwise prevent window.onerror from
      // firing, which means without this call the admin
      // dashboard would never know a render error happened.
      // Fire-and-forget — we never want the failure path
      // to block the in-place recovery screen.
      try {
        if (root.IE && root.IE.util && typeof root.IE.util.reportClientError === 'function') {
          root.IE.util.reportClientError('error', {
            message: (renderErr && renderErr.message) ? String(renderErr.message) : 'render_failed',
            filename: 'client/exam.js',
            line: 0,
            col: 0,
            stack: (renderErr && renderErr.stack) ? String(renderErr.stack) : ''
          });
        }
      } catch (_e) { /* never let the reporter block recovery */ }
      _renderQuestionErrorFallback(renderErr);
    }
  }

  // The actual render body, separated so a render error
  // can be caught without losing the API call result.
  async function _renderQBody(q) {
    const sel = S.answers[S.currentQ] || [];
    const answered = S.answers.filter((a) => a && a.length > 0).length;
    const pct = (answered / S.total) * 100;
    const isLast = S.currentQ === S.total - 1;
    const unanswered = S.answers.filter((a) => !a || a.length === 0).length;

    const opts = q.opts.map((text, displayIdx) => {
      const isSel = sel.includes(displayIdx);
      return `<button class="option${isSel ? ' selected' : ''}" data-action="pick" data-args="${displayIdx}">
        <span class="opt-letter">${'ABCDEF'[displayIdx]}</span>
        <span class="opt-text">${_esc(text)}</span>
      </button>`;
    }).join('');

    const dots = Array.from({ length: S.total }, (_, i) => {
      const a = S.answers[i] || [];
      const vis = S.visited.has(i);
      const isCurrent = i === S.currentQ;
      const isAnswered = a.length > 0;
      const isSkipped = vis && !isAnswered;
      let cls = 'nav-dot';
      if (isCurrent) cls += ' current';
      else if (isAnswered) cls += ' answered';
      else if (isSkipped) cls += ' skipped';
      // Status glyph shown in the top-right corner of each dot.
      // ✓ = answered, · = skipped/visited, empty = unanswered.
      // The current question hides the glyph and relies on its
      // background fill to convey "you are here".
      const statusGlyph = isCurrent ? '' : (isAnswered ? '✓' : (isSkipped ? '·' : ''));
      const statusLabel = isCurrent ? 'Current' : (isAnswered ? 'Answered' : (isSkipped ? 'Skipped' : 'Unanswered'));
      return `<button type="button" class="${cls}" data-action="goToQ" data-args="${i}" title="Q${i + 1}: ${statusLabel}" aria-label="Go to question ${i + 1}: ${statusLabel}" ${isCurrent ? 'aria-current="step"' : ''}><span class="nav-dot-num">${i + 1}</span><span class="nav-dot-status" aria-hidden="true">${statusGlyph}</span></button>`;
    }).join('');

    render(`<div class="no-select exam-shell" style="min-height:100vh;display:flex;flex-direction:column">
      <div class="exam-header">
        <div class="header-brand">
          <div class="brand-chip">
            <span class="brand-chip-mark" aria-hidden="true"><i></i></span>
            <span class="brand-chip-text">
              <span class="brand-chip-name">Academy Exam App</span>
              <span class="brand-chip-sub">${S.isPractice ? 'Practice Knowledge Check' : 'Secure Exam Session'}</span>
            </span>
          </div>
        </div>
        <div class="header-info">
          <span class="header-title">Academy assessment flow</span>
          <span class="header-code">CODE: ${_esc(S.code)}</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px">
          <div id="save-pill" class="${root.SAVE_UI.cls}" role="status" aria-live="polite">${_esc(root.SAVE_UI.text)}</div>
          <div id="timer" class="timer" role="status" aria-live="polite" aria-label="Time remaining">--:--</div>
        </div>
      </div>
      ${connectivityBanner()}
      <div style="background:white;padding:8px 20px;border-bottom:1px solid #e0e6f0">
        <div style="display:flex;justify-content:space-between;font-size:12px;color:#777;margin-bottom:5px">
          <span><strong>Q${S.currentQ + 1}</strong> of ${S.total} · <span style="color:${answered === S.total ? '#1a5c1a' : '#c55a11'}">${answered}/${S.total} answered</span></span>
          <span>${sel.length ? (q.multi ? `${sel.length} selected` : '✓ Answered') : '<span style="color:#c55a11">⚠ Unanswered</span>'}</span>
        </div>
        <div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div>
        <div class="nav-dots">${dots}</div>
        <div class="nav-help">Tap a number to jump &middot; <code>✓</code> answered &middot; <code>&middot;</code> skipped &middot; current = highlighted &middot; arrow keys move &middot; 1&ndash;9 jump</div>
      </div>
      <div style="flex:1;overflow-y:auto;padding:18px 16px 80px">
        <div style="max-width:720px;margin:0 auto">
          <div class="q-meta">
            <span class="q-num">Q${S.currentQ + 1}</span>
            ${q.multi ? '<span class="multi-badge">★ MULTI-SELECT</span>' : ''}
          </div>
          <div class="q-stem">${_esc(q.stem)}</div>
          ${q.multi && q.note ? `<div class="multi-note">${_esc(q.note)}</div>` : ''}
          <div class="options">${opts}</div>
          <div class="exam-nav">
            <button class="btn btn-secondary" data-action="prevQ" ${S.currentQ === 0 ? 'disabled' : ''}>← Back</button>
            <span class="sel-count">${sel.length ? (q.multi ? `${sel.length} selected` : '✓ Answered') : 'No answer'}</span>
            ${isLast
              ? `<button class="btn btn-primary" data-action="trySubmit" ${unanswered > 0 ? 'disabled' : ''}>Submit Exam</button>`
              : `<button class="btn btn-primary" data-action="nextQ">Next →</button>`}
          </div>
          ${S.isPractice ? '<p style="text-align:center;font-size:12px;color:#1a5c1a;margin-top:8px;font-weight:700">Practice mode: you will see right/wrong feedback after submission.</p>' : ''}
          ${isLast && unanswered > 0 ? `<p style="text-align:center;font-size:12px;color:#c55a11;margin-top:8px">⚠ ${unanswered} unanswered — use the dots above to go back.</p>` : ''}
        </div>
      </div>
    </div>
    ${proctorEnabled() ? `<div class="webcam-corner"><video id="exam-cam" autoplay muted playsinline></video><div class="webcam-label">PROCTORED</div></div>` : ''}`);

    const ec = $('exam-cam');
    if (ec && S.webcamStream && proctorEnabled()) ec.srcObject = S.webcamStream;
    setSavePill(root.SAVE_UI.cls, root.SAVE_UI.text);
    updateTimer();
    queueProgressSave();
  }

  // In-place fallback when _renderQBody throws. Renders a
  // minimal "this question failed to render" view with
  // navigation buttons. The candidate can skip to the next
  // question instead of losing the entire exam session to
  // the global recovery screen.
  function _renderQuestionErrorFallback(renderErr) {
    const isLast = S.currentQ === S.total - 1;
    const errMsg = (renderErr && renderErr.message) ? String(renderErr.message).slice(0, 200) : 'unknown error';
    render(`<div class="no-select exam-shell" style="min-height:100vh;display:flex;flex-direction:column">
      <div class="exam-header">
        <div class="header-brand">
          <div class="brand-chip">
            <span class="brand-chip-mark" aria-hidden="true"><i></i></span>
            <span class="brand-chip-text">
              <span class="brand-chip-name">Academy Exam App</span>
              <span class="brand-chip-sub">${S.isPractice ? 'Practice Knowledge Check' : 'Secure Exam Session'}</span>
            </span>
          </div>
        </div>
        <div class="header-info">
          <span class="header-title">Academy assessment flow</span>
          <span class="header-code">CODE: ${_esc(S.code)}</span>
        </div>
        <div style="display:flex;align-items:center;gap:8px">
          <div id="save-pill" class="${root.SAVE_UI.cls}" role="status" aria-live="polite">${_esc(root.SAVE_UI.text)}</div>
          <div id="timer" class="timer" role="status" aria-live="polite" aria-label="Time remaining">--:--</div>
        </div>
      </div>
      ${connectivityBanner()}
      <div style="flex:1;display:flex;align-items:center;justify-content:center;padding:24px">
        <div class="card" style="max-width:520px;text-align:center">
          <div style="font-size:38px;margin-bottom:8px">⚠️</div>
          <h2 style="margin-bottom:8px">Question ${S.currentQ + 1} failed to render</h2>
          <p style="color:#666;margin-bottom:18px">A rendering error prevented this question from displaying. Your previous answers are saved. Skip to the next question to continue.</p>
          <p style="color:#999;font-size:12px;margin-bottom:18px;font-family:monospace">${_esc(errMsg)}</p>
          <div style="display:flex;gap:10px;justify-content:center;flex-wrap:wrap">
            <button class="btn btn-secondary" data-action="prevQ" ${S.currentQ === 0 ? 'disabled' : ''}>← Back</button>
            <button class="btn btn-primary" data-action="nextQ" ${isLast ? 'disabled' : ''}>Skip to Next →</button>
            ${isLast ? `<button class="btn btn-success" data-action="trySubmit">Submit Exam</button>` : ''}
          </div>
        </div>
      </div>
    </div>`);
    // Still keep the timer + save pill alive so the candidate
    // can finish the exam normally.
    setSavePill(root.SAVE_UI.cls, root.SAVE_UI.text);
    updateTimer();
    queueProgressSave();
  }

  function goToQ(i) {
    if (S.submitted || S.proctorRecoveryRequired) return;
    S.currentQ = i;
    renderQ();
  }
  function prevQ() {
    if (S.submitted || S.proctorRecoveryRequired || S.currentQ <= 0) return;
    S.currentQ -= 1;
    renderQ();
  }
  function nextQ() {
    if (S.submitted || S.proctorRecoveryRequired) return;
    S.currentQ = Math.min(S.total - 1, S.currentQ + 1);
    renderQ();
  }
  function pick(displayOptIdx) {
    if (S.submitted || S.proctorRecoveryRequired) return;
    const q = S.currentQCache;
    let a = [...(S.answers[S.currentQ] || [])];
    if (q.multi) a = a.includes(displayOptIdx) ? a.filter((x) => x !== displayOptIdx) : [...a, displayOptIdx].sort((x, y) => x - y);
    else a = [displayOptIdx];
    S.answers[S.currentQ] = a;
    queueProgressSave();
    renderQ();
  }

  function trySubmit() {
    if (S.proctorRecoveryRequired) return;
    const unanswered = S.answers.filter((a) => !a || a.length === 0).length;
    if (unanswered > 0) {
      modal('⚠️', 'Unanswered Questions', `${unanswered} question${unanswered !== 1 ? 's are' : ' is'} still unanswered. Please answer all questions before submitting.`, [{ label: 'Go Back', cls: 'btn-primary' }]);
      return;
    }
    modal('📝', 'Submit Exam', 'Are you sure you want to submit? You will not be able to make changes after submission.', [
      { label: 'Submit Now', cls: 'btn-primary', action: () => submitExam(false) },
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  // ---- Timer ----
  function startTimer() {
    clearInterval(S.timerInterval);
    S.timerInterval = setInterval(updateTimer, 500);
  }
  function getRemainingMs() {
    return (S.durationSecs * 1000) - (S.elapsed + (S.startTime ? Date.now() - S.startTime : 0));
  }
  function updateTimer() {
    const rem = Math.ceil(getRemainingMs() / 1000);
    const el = $('timer');
    if (!el) return;
    if (rem <= 0) {
      el.textContent = '00:00';
      clearInterval(S.timerInterval);
      if (!S.submitted) submitExam(true);
      return;
    }
    el.textContent = fmt(rem);
    el.className = 'timer' + (rem <= 60 ? ' danger' : rem <= 300 ? ' warning' : '');
  }

  // ---- Submission ----
  async function submitExam(autoSubmit) {
    if (S.submitted || (!autoSubmit && S.proctorRecoveryRequired)) return;
    S.submitted = true;
    clearInterval(S.timerInterval);
    if (root.IE.proctor && typeof root.IE.proctor.teardownSecurity === 'function') {
      // teardownSecurity() now also clears the webcam
      // interval and stops MediaStream tracks via
      // IE.state.stopMediaStreams(). Single source of
      // truth for the cleanup path.
      root.IE.proctor.teardownSecurity();
    }

    const payload = buildSubmitPayload(autoSubmit);
    let data;
    if (!isOnline()) {
      queuePendingAction(S.code, 'submit', payload);
      showPendingSubmission();
      return;
    }
    try {
      data = await apiJson('/api/submit', {
        method: 'POST',
        body: JSON.stringify(payload)
      }, { timeoutMs: 15000, retries: 0 });
    } catch (_e) {
      data = null;
    }

    if (!data || !data.ok || !data.result) {
      queuePendingAction(S.code, 'submit', payload);
      showPendingSubmission();
      return;
    }
    clearPendingState(S.code);
    showResultsFromRecord(data.result);
  }

  function showPendingSubmission() {
    S.screen = 'submit-pending';
    document.body.classList.remove('exam-bg');
    render(`<div class="screen" style="max-width:540px">
      <div class="card" style="margin-top:56px;text-align:center">
        <div style="font-size:42px;margin-bottom:10px">⏳</div>
        <h2 style="margin-bottom:10px">Submission Queued</h2>
        <p style="color:#555;font-size:14px;line-height:1.7">Your browser could not reach the server, so the final submission was saved locally on this device. Keep this tab or reopen with the same code once your connection returns.</p>
        <p style="color:#8a5b00;font-size:13px;margin:12px 0 18px">Access code: <strong style="font-family:monospace">${_esc(S.code)}</strong></p>
        <button class="btn btn-primary btn-full" data-action="retryPendingSubmission">Retry Submission</button>
        <button class="btn btn-secondary btn-full" data-action="showCodeEntry">Return to Start</button>
      </div>
    </div>`);
  }

  async function retryPendingSubmission() {
    if (!isOnline()) {
      modal('⚠️', 'Still Offline', 'Reconnect to the internet first, then retry submission.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    if (!S.examToken) {
      if (root.IE.codeEntry && typeof root.IE.codeEntry.showCodeEntry === 'function') {
        root.IE.codeEntry.showCodeEntry();
      }
      return;
    }
    const done = await replayPendingActions();
    if (!done) {
      modal('❌', 'Retry Failed', 'Submission is still pending. Keep this tab and try again shortly.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  // ---- Results ----
  function showResultsFromRecord(rec) {
    S.screen = 'results';
    document.body.classList.remove('exam-bg');
    const dur = durationLabel(rec.durationSecs || 0);
    const sectionResults = Array.isArray(rec.sectionResults) ? rec.sectionResults : [];
    const questionResults = Array.isArray(rec.questionResults) ? rec.questionResults : [];
    const showPracticeReview = rec.showCorrectAnswers === true && questionResults.length > 0;
    const passPct = Number(rec.passPct || S.passPct || 80);
    const passScore = rec.total ? Math.ceil((Number(rec.total) * passPct) / 100) : (S.passScore || 0);
    const studyFocus = sectionResults
      .slice()
      .sort((a, b) => Number(a.pct || 0) - Number(b.pct || 0))
      .slice(0, 3)
      .map((section) => `${section.name || 'Segment'} (${Number(section.pct || 0)}%)`);
    const sectionBreakdown = sectionResults.length ? `
      <div class="divider"></div>
      <div style="text-align:left">
        <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:10px">Performance by Segment</div>
        <div style="display:grid;gap:10px">
          ${sectionResults.map((section) => {
            const wrong = Math.max(0, Number(section.total || 0) - Number(section.correct || 0));
            return `<div style="border:1px solid #d9e3f0;border-radius:12px;padding:12px 14px;background:#f8fbff">
              <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:6px">
                <div style="font-weight:800;color:#1F3864">${_esc(section.name || 'Segment')}</div>
                <div style="font-size:12px;color:#6c7a90">${Number(section.pct || 0)}%</div>
              </div>
              <div style="font-size:13px;color:#445;line-height:1.8">
                <strong>Right:</strong> ${Number(section.correct || 0)}<br>
                <strong>Wrong:</strong> ${wrong}<br>
                <strong>Total:</strong> ${Number(section.total || 0)}
              </div>
            </div>`;
          }).join('')}
        </div>
        <p style="font-size:12px;color:#667;margin-top:10px;margin-bottom:0">Use the segments with the lowest scores as your main study focus before the next attempt.</p>
      </div>` : '';
    const practiceReview = showPracticeReview ? `
      <div class="divider"></div>
      <div style="text-align:left">
        <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:8px">Question Review</div>
        <p style="font-size:12px;color:#667;margin-bottom:12px">Practice mode only: review each answer and use missed questions for follow-up study.</p>
        <div style="display:grid;gap:12px">
          ${questionResults.map((item, idx) => {
            const displayOptions = Array.isArray(item.displayOptions) && item.displayOptions.length
              ? item.displayOptions
              : (Array.isArray(item.opts) ? item.opts : []);
            const labels = 'ABCDEF';
            const formatDisplay = (indexes) => {
              if (!Array.isArray(indexes) || !indexes.length) return 'No answer selected';
              return indexes.map((displayIdx) => {
                const label = labels[displayIdx] || String(displayIdx + 1);
                return `${label}. ${displayOptions[displayIdx] || `Option ${displayIdx + 1}`}`;
              }).join('<br>');
            };
            return `<div style="border:1px solid ${item.correct ? '#b8dfc1' : '#f1c0c0'};border-radius:14px;padding:14px;background:${item.correct ? '#f3fbf5' : '#fff7f7'}">
              <div style="display:flex;justify-content:space-between;gap:10px;align-items:flex-start;margin-bottom:8px">
                <div style="font-weight:800;color:#1F3864">Question ${idx + 1}</div>
                ${item.correct ? '<span class="chip chip-pass">Correct</span>' : '<span class="chip chip-fail">Wrong</span>'}
              </div>
              <div style="font-size:13px;font-weight:700;color:#223;line-height:1.55;margin-bottom:10px">${_esc(item.stem || 'Question')}</div>
              ${item.sectionName ? `<div style="font-size:11px;color:#7a8ca8;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px">${_esc(item.sectionName)}</div>` : ''}
              <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;font-size:12px;line-height:1.6">
                <div><strong>Your answer</strong><br>${formatDisplay(item.givenDisplay || [])}</div>
                <div><strong>Correct answer</strong><br>${formatDisplay(item.expectedDisplay || [])}</div>
              </div>
            </div>`;
          }).join('')}
        </div>
      </div>` : '';
    render(`<div class="screen" style="max-width:${showPracticeReview ? '860px' : '500px'}">
      <div class="card" style="margin-top:48px;text-align:center">
        <h2 style="text-align:center;margin-bottom:18px">${rec.autoSubmit ? 'Time Expired - Auto-Submitted' : (rec.isPractice ? 'Practice Submitted' : 'Exam Submitted')}</h2>
        <div class="score-circle ${rec.pass ? 'pass' : 'fail'}">
          <div class="score-num">${rec.score}</div>
          <div class="score-den">out of ${rec.total}</div>
        </div>
        <div style="font-size:22px;font-weight:800;color:${rec.pass ? '#1a5c1a' : '#c0392b'};margin-bottom:4px">${rec.isPractice ? (rec.pass ? 'TARGET MET' : 'KEEP PRACTICING') : (rec.pass ? 'PASS' : 'DID NOT PASS')}</div>
        <div style="font-size:17px;font-weight:700;margin-bottom:4px">${rec.pct}%</div>
        <div style="font-size:13px;color:#888;margin-bottom:22px">Pass threshold: ${passScore}/${rec.total} (${passPct}%)</div>
        <div class="divider"></div>
        <div style="text-align:left;font-size:14px;color:#555;line-height:2.2">
          <div><strong>Access Code:</strong> <span style="font-family:monospace;font-size:16px;font-weight:800;letter-spacing:2px;color:#1F3864">${_esc(rec.code)}</span></div>
          <div><strong>Duration:</strong> ${dur}</div>
          <div><strong>Submitted:</strong> ${new Date(rec.submittedAt).toLocaleString()}</div>
          ${rec.tabSwitches > 0 ? `<div style="color:#c55a11"><strong>Tab switches:</strong> ${rec.tabSwitches}</div>` : ''}
          ${rec.incidentCount > 0 ? `<div style="color:#c55a11"><strong>Flags logged:</strong> ${rec.incidentCount}</div>` : ''}
        </div>
        ${sectionBreakdown}
        ${practiceReview}
        <div class="divider"></div>
        ${studyFocus.length ? `<div style="text-align:left;margin-bottom:16px">
          <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:8px">Study Focus</div>
          <div style="font-size:13px;color:#555;line-height:1.9">${studyFocus.map((item) => `• ${_esc(item)}`).join('<br>')}</div>
        </div>` : ''}
        <div style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-bottom:12px">
          <button class="btn btn-secondary btn-sm" data-action="downloadResultSummary" data-args="json">Download JSON Summary</button>
          <button class="btn btn-secondary btn-sm" data-action="downloadResultSummary" data-args="html">Download Study Report</button>
        </div>
        <p style="font-size:13px;color:#999">${rec.isPractice ? 'Your practice attempt has been saved for learning analytics.' : 'Your result has been recorded. You may close this window.'}</p>
      </div>
    </div>`);
    root.__lastResult = rec;
  }

  function downloadResultSummary(kind) {
    const rec = root.__lastResult;
    if (!rec) return;
    const fileSafeCode = String(rec.code || 'result').replace(/[^A-Z0-9_-]/gi, '_');
    let blob;
    let fileName;
    if (kind === 'html') {
      const sections = Array.isArray(rec.sectionResults) ? rec.sectionResults : [];
      const html = `<!doctype html><html><head><meta charset="utf-8"><title>Academy Exam Summary ${_esc(rec.code || '')}</title></head><body><h1>Academy Exam Summary</h1><p><strong>Code:</strong> ${_esc(rec.code || '')}</p><p><strong>Score:</strong> ${_esc(rec.score)} / ${_esc(rec.total)} (${_esc(rec.pct)}%)</p><p><strong>Submitted:</strong> ${_esc(rec.submittedAt || '')}</p><h2>Section Results</h2><ul>${sections.map((section) => `<li>${_esc(section.name || 'Segment')}: ${_esc(section.correct)} / ${_esc(section.total)} (${_esc(section.pct)}%)</li>`).join('')}</ul></body></html>`;
      blob = new Blob([html], { type: 'text/html;charset=utf-8' });
      fileName = `academy_exam_summary_${fileSafeCode}.html`;
    } else {
      blob = new Blob([JSON.stringify(rec, null, 2)], { type: 'application/json;charset=utf-8' });
      fileName = `academy_exam_summary_${fileSafeCode}.json`;
    }
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    a.click();
    URL.revokeObjectURL(url);
  }

  // Shared with admin dashboard — used by the status chip column.
  function statusChip(row) {
    if (row.status === 'completed' && row.isPractice) return '<span class="chip chip-pass">PRACTICE</span>';
    if (row.status === 'completed') return `<span class="chip ${row.pass ? 'chip-pass' : 'chip-fail'}">${row.pass ? 'PASS' : 'FAIL'}</span>`;
    if (row.status === 'active') return '<span class="chip chip-active">ACTIVE</span>';
    return '<span class="chip chip-unused">UNUSED</span>';
  }

  // ---- Global event listeners for in-exam security ----
  // These run once at module load. They only take effect when the
  // current screen is 'exam', so they don't interfere with the admin
  // or consent flows.
  document.addEventListener('contextmenu', (e) => {
    if (S.screen === 'exam') {
      e.preventDefault();
      logIncident('right_click', 'Right-click');
      queueProgressSave();
    }
  });
  document.addEventListener('selectstart', (e) => {
    if (S.screen === 'exam') e.preventDefault();
  });
  document.addEventListener('keydown', (e) => {
    if (S.screen !== 'exam') return;
    const activeTag = String(document.activeElement?.tagName || '').toUpperCase();
    const inTextField = activeTag === 'INPUT' || activeTag === 'TEXTAREA' || document.activeElement?.isContentEditable;
    if (e.key === 'F12' || (e.ctrlKey && 'uUiIjJ'.includes(e.key))) {
      e.preventDefault();
      logIncident('shortcut', `Blocked shortcut: ${e.key}`);
      queueProgressSave();
      return;
    }
    const isWinPrintScreen = e.key === 'PrintScreen' && !e.metaKey;
    const isAltPrintScreen = e.key === 'PrintScreen' && e.altKey;
    const isCtrlP = e.ctrlKey && (e.key === 'p' || e.key === 'P');
    const isCtrlShiftS = e.ctrlKey && e.shiftKey && (e.key === 's' || e.key === 'S');
    const isSnippingTool = e.metaKey && e.shiftKey && (e.key === 's' || e.key === 'S');
    if (isWinPrintScreen || isAltPrintScreen || isCtrlP || isCtrlShiftS || isSnippingTool) {
      e.preventDefault();
      const label = isWinPrintScreen ? 'PrintScreen' : isAltPrintScreen ? 'Alt+PrintScreen' : isCtrlP ? 'Ctrl+P' : isCtrlShiftS ? 'Ctrl+Shift+S' : 'Win+Shift+S';
      logIncident('screenshot_attempt', label);
      queueProgressSave();
      modal('🚨', 'Screenshot Detected', `A screenshot attempt (${label}) has been detected and logged against your access code. Please do not capture exam content.`, [{ label: 'I understand', cls: 'btn-danger' }]);
      return;
    }
    if (!inTextField && !e.metaKey && !e.ctrlKey && !e.altKey) {
      if (e.key === 'ArrowLeft') {
        e.preventDefault();
        prevQ();
        return;
      }
      if (e.key === 'ArrowRight') {
        e.preventDefault();
        nextQ();
        return;
      }
      if (/^[1-9]$/.test(e.key)) {
        const target = Number(e.key) - 1;
        if (target < S.total) {
          e.preventDefault();
          goToQ(target);
        }
      }
    }
  });

  root.IE = root.IE || {};
  root.IE.exam = {
    renderQ: renderQ,
    goToQ: goToQ,
    prevQ: prevQ,
    nextQ: nextQ,
    pick: pick,
    trySubmit: trySubmit,
    startTimer: startTimer,
    getRemainingMs: getRemainingMs,
    updateTimer: updateTimer,
    submitExam: submitExam,
    showResultsFromRecord: showResultsFromRecord,
    downloadResultSummary: downloadResultSummary,
    showPendingSubmission: showPendingSubmission,
    retryPendingSubmission: retryPendingSubmission,
    statusChip: statusChip
  };
})(typeof window !== 'undefined' ? window : globalThis);
