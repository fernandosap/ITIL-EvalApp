/* eslint-disable no-console */
// client/admin-question-sets.js — exam set management: create, configure,
// activate, archive, publish, delete, upload CSV, edit questions and
// sections, and view per-set analytics. The most action-heavy module.
//
// Public surface (attached to window.IE.questionSets):
//   assignQuestionSet, showQuestionSetAnalytics, createQuestionSet,
//   configQuestionSet, saveQuestionSetConfig, syncExamModeHelp,
//   activateQuestionSet, deleteQuestionSet, downloadQuestionTemplate,
//   showUploadQuestionSet, parseQuestionCsv, analyzeQuestionUpload,
//   previewUploadedQuestionSet, submitUploadedQuestionSet,
//   openQuestionSet, showQuestionEditor, saveQuestionEditor,
//   deleteQuestion, editSectionPrompt, deleteSection,
//   exportQuestionSet, cloneQuestionSet, publishQuestionSet,
//   archiveQuestionSet, rollbackImportedSet

(function (root) {
  const { $, _esc, render, modal, apiJson, apiFetch, durationLabel, normalizeExamTitle, roleCan } = root.IE.util;
  const S = root.S;

  // ---- Assign / analytics ----
  async function assignQuestionSet(code, setIdValue) {
    try {
      await apiJson(`/api/admin/codes/${encodeURIComponent(code)}/question-set`, {
        method: 'POST',
        body: JSON.stringify({ questionSetId: setIdValue === '' ? null : Number(setIdValue) })
      }, { timeoutMs: 10000, retries: 0 });
      const row = root._adminRows.find((item) => item.code === code);
      if (row) {
        row.questionSetId = setIdValue === '' ? null : Number(setIdValue);
        const set = root._adminQuestionSets.find((item) => item.id === row.questionSetId);
        row.questionSetName = set ? normalizeExamTitle(set.name) : '';
      }
    } catch (_e) {
      modal('❌', 'Assignment Failed', 'Could not assign that exam set to the access code.', [{ label: 'OK', cls: 'btn-primary', action: () => root.IE.admin.showAdmin() }]);
    }
  }

  async function showQuestionSetAnalytics(setId) {
    S.screen = 'admin-analytics';
    render('<div class="admin-wrap"><div style="padding:60px;text-align:center;color:white;font-size:18px">Loading analytics...</div></div>');
    try {
      const resp = await apiJson(`/api/admin/question-sets/${setId}/analytics`, {}, { timeoutMs: 20000, retries: 1 });
      if (!resp || !resp.ok) throw new Error('analytics_failed');
      const s = resp.summary || {};
      const metric = (label, value, hint = '') => `
        <div style="padding:16px;border:1px solid #d8e1f0;border-radius:16px;background:#f8fbff">
          <div style="font-size:12px;color:#6c7a90;margin-bottom:5px">${_esc(label)}</div>
          <div style="font-size:28px;font-weight:800;color:#1F3864">${value == null ? '—' : _esc(value)}</div>
          ${hint ? `<div style="font-size:11px;color:#76869c;margin-top:4px">${_esc(hint)}</div>` : ''}
        </div>`;
      const questionRows = (items, emptyLabel) => items.length ? items.map((q) => `
        <tr>
          <td style="text-align:center">${q.questionIndex ?? '—'}</td>
          <td>
            <div style="font-weight:700;color:#1F3864">${_esc(String(q.stem || 'Question').slice(0, 180))}${String(q.stem || '').length > 180 ? '...' : ''}</div>
            ${q.sectionName ? `<div style="font-size:11px;color:#7a8ca8;margin-top:4px">${_esc(q.sectionName)}</div>` : ''}
          </td>
          <td style="text-align:center">${q.answered}</td>
          <td style="text-align:center">${q.correct}</td>
          <td style="text-align:center">${q.wrong}</td>
          <td style="text-align:center">${q.pctCorrect == null ? '—' : `${q.pctCorrect}%`}</td>
        </tr>`).join('') : `<tr><td colspan="6" style="text-align:center;color:#888;padding:18px">${emptyLabel}</td></tr>`;
      const sectionRows = (resp.sectionStats || []).length ? resp.sectionStats.map((section) => `
        <tr>
          <td>${_esc(section.name || 'Section')}</td>
          <td style="text-align:center">${section.correct}</td>
          <td style="text-align:center">${section.wrong}</td>
          <td style="text-align:center">${section.total}</td>
          <td style="text-align:center">${section.pctCorrect == null ? '—' : `${section.pctCorrect}%`}</td>
        </tr>`).join('') : '<tr><td colspan="5" style="text-align:center;color:#888;padding:18px">No section-level answer data yet</td></tr>';
      const attemptRows = (resp.recentAttempts || []).length ? resp.recentAttempts.map((attempt) => `
        <tr>
          <td style="font-family:monospace">${_esc(attempt.code || '')}</td>
          <td>${_esc(attempt.label || '')}</td>
          <td style="text-align:center">${attempt.examMode === 'PRACTICE' ? 'Practice' : 'Graded'}</td>
          <td style="text-align:center">${attempt.score == null ? '—' : `${attempt.score}/${attempt.total}`}</td>
          <td style="text-align:center">${attempt.pct == null ? '—' : `${attempt.pct}%`}</td>
          <td style="text-align:center">${attempt.durationSecs == null ? '—' : durationLabel(attempt.durationSecs)}</td>
          <td style="white-space:nowrap">${attempt.submittedAt ? _esc(new Date(attempt.submittedAt).toLocaleString()) : '—'}</td>
        </tr>`).join('') : '<tr><td colspan="7" style="text-align:center;color:#888;padding:18px">No attempts yet</td></tr>';

      render(`<div class="admin-wrap">
        <div class="card" style="margin-bottom:16px">
          <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap">
            <div>
              <div style="font-size:22px;font-weight:800;color:#1F3864">Analytics</div>
              <div style="font-size:13px;color:#666;margin-top:4px">${_esc(resp.questionSet?.name || 'Exam Set')} · ${resp.questionSet?.isPractice ? 'Practice' : 'Graded'} · ${resp.questionSet?.questionCount || 0} questions</div>
            </div>
            <button class="btn btn-secondary btn-sm" onclick="showAdmin()">← Back to Admin</button>
          </div>
        </div>
        <div class="card" style="margin-bottom:16px">
          <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px">
            ${metric('Attempts', s.attempts)}
            ${metric('Completed', s.completed)}
            ${metric('Average Score', s.averageScore == null ? null : s.averageScore)}
            ${metric('Average %', s.averagePct == null ? null : `${s.averagePct}%`)}
            ${metric('Pass Rate', s.passRate == null ? null : `${s.passRate}%`)}
            ${metric('Average Time', s.averageDurationSecs == null ? null : durationLabel(s.averageDurationSecs))}
            ${metric('Practice Attempts', s.practiceAttempts)}
            ${metric('Graded Attempts', s.gradedAttempts)}
          </div>
        </div>
        <div class="card" style="margin-bottom:16px">
          <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:10px">Performance by Section</div>
          <div style="overflow-x:auto"><table class="admin-table"><thead><tr><th>Section</th><th style="text-align:center">Right</th><th style="text-align:center">Wrong</th><th style="text-align:center">Total</th><th style="text-align:center">Avg %</th></tr></thead><tbody>${sectionRows}</tbody></table></div>
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:16px;margin-bottom:16px">
          <div class="card">
            <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:10px">Hardest Questions</div>
            <div style="overflow-x:auto"><table class="admin-table"><thead><tr><th style="text-align:center">#</th><th>Question</th><th style="text-align:center">Answered</th><th style="text-align:center">Right</th><th style="text-align:center">Wrong</th><th style="text-align:center">Right %</th></tr></thead><tbody>${questionRows(resp.hardestQuestions || [], 'No question analytics yet')}</tbody></table></div>
          </div>
          <div class="card">
            <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:10px">Easiest Questions</div>
            <div style="overflow-x:auto"><table class="admin-table"><thead><tr><th style="text-align:center">#</th><th>Question</th><th style="text-align:center">Answered</th><th style="text-align:center">Right</th><th style="text-align:center">Wrong</th><th style="text-align:center">Right %</th></tr></thead><tbody>${questionRows(resp.easiestQuestions || [], 'No question analytics yet')}</tbody></table></div>
          </div>
        </div>
        <div class="card">
          <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:10px">Recent Attempts</div>
          <div style="overflow-x:auto"><table class="admin-table"><thead><tr><th>Code</th><th>Seat</th><th style="text-align:center">Mode</th><th style="text-align:center">Score</th><th style="text-align:center">Pct</th><th style="text-align:center">Time</th><th>Submitted</th></tr></thead><tbody>${attemptRows}</tbody></table></div>
        </div>
      </div>`);
    } catch (_e) {
      modal('❌', 'Analytics Failed', 'Could not load analytics for this exam set.', [{ label: 'Back to Admin', cls: 'btn-primary', action: () => root.IE.admin.showAdmin() }]);
    }
  }

  // ---- Create / configure / activate / delete ----
  async function createQuestionSet() {
    const name = window.prompt('Name for the new exam set:', '');
    if (!name || !name.trim()) return;
    const description = window.prompt('Optional description for this exam set:', '') || '';
    try {
      const resp = await apiJson('/api/admin/question-sets', {
        method: 'POST',
        body: JSON.stringify({ name: name.trim(), description: description.trim() })
      }, { timeoutMs: 10000, retries: 0 });
      if (!resp || !resp.ok || !resp.questionSet) throw new Error('create_failed');
      openQuestionSet(resp.questionSet.id, resp.questionSet.name);
    } catch (_e) {
      modal('❌', 'Create Failed', 'Could not create the new exam set.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  async function configQuestionSet(id, currentDuration, currentPassPct, currentProctor, currentNumQuestions, totalQuestions) {
    const current = root.__currentQuestionSet || null;
    const returnAction = current && current.id === id
      ? `openQuestionSet(${id}, '${_esc(current.name || '')}')`
      : 'showAdmin()';
    const setMeta = root._adminQuestionSets.find((set) => set.id === id) || {};
    const setName = current && current.id === id ? current.name : (setMeta.name || 'Exam Set');
    const setDescription = current && current.id === id ? (current.description || '') : (setMeta.description || '');
    const examMode = current && current.id === id ? (current.meta?.examMode || 'GRADED') : (setMeta.examMode || 'GRADED');
    const showCorrectAnswers = current && current.id === id ? (current.meta?.showCorrectAnswers === true) : (setMeta.showCorrectAnswers === true);
    const countsTowardResults = current && current.id === id ? (current.meta?.countsTowardResults !== false) : (setMeta.countsTowardResults !== false);

    S.screen = 'admin-question-set-config';
    document.body.classList.remove('exam-bg');
    render(`<div class="admin-wrap">
      <div class="card" style="max-width:760px;margin:0 auto">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;margin-bottom:14px">
          <div>
            <div style="font-size:22px;font-weight:800;color:#1F3864">Exam Set Configuration</div>
            <div style="font-size:13px;color:#666;margin-top:4px">${_esc(setName)}</div>
          </div>
          <button class="btn btn-secondary btn-sm" onclick="${returnAction}">← Back</button>
        </div>

        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-bottom:18px">
          <div style="padding:14px;border:1px solid #d8e1f0;border-radius:14px;background:#f8fbff">
            <div style="font-size:12px;color:#6c7a90;margin-bottom:6px">Questions in bank</div>
            <div style="font-size:28px;font-weight:800;color:#1F3864">${Number(totalQuestions || 0)}</div>
          </div>
          <div style="padding:14px;border:1px solid #d8e1f0;border-radius:14px;background:#f8fbff">
            <div style="font-size:12px;color:#6c7a90;margin-bottom:6px">Current delivery mode</div>
            <div style="font-size:18px;font-weight:800;color:#1F3864">${currentNumQuestions == null ? 'All questions' : `${currentNumQuestions} per attempt`}</div>
          </div>
        </div>
        <label class="label">Exam Title</label>
        <input id="cfg-name" type="text" value="${_esc(setName)}" placeholder="Exam title">
        <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">This title appears on the candidate landing screen and in admin assignment lists.</div>

        <label class="label">Description</label>
        <input id="cfg-description" type="text" value="${_esc(setDescription)}" placeholder="Optional description">
        <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">Optional context such as language, cohort, or version.</div>

        <label class="label">Duration (minutes)</label>
        <input id="cfg-duration" type="number" min="1" max="240" value="${Number(currentDuration || 45)}">
        <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">How long candidates have before the exam auto-submits.</div>

        <label class="label">Passing Percentage</label>
        <input id="cfg-pass-pct" type="number" min="1" max="100" value="${Number(currentPassPct || 80)}">
        <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">The score threshold required to pass this exam set.</div>

        <label class="label">Questions Delivered Per Candidate</label>
        <input id="cfg-num-questions" type="number" min="1" max="${Math.max(1, Number(totalQuestions || 1))}" value="${currentNumQuestions == null ? '' : Number(currentNumQuestions)}" placeholder="Leave blank to deliver all ${Number(totalQuestions || 0)} questions">
        <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">Leave this blank to present the full question bank. Set a number to randomly draw a subset for each candidate.</div>

        <label class="label">Proctoring</label>
        <div style="display:flex;align-items:center;gap:10px;padding:12px 14px;border:1px solid #d0d8e8;border-radius:12px;background:#f8fbff;margin-bottom:18px">
          <input id="cfg-proctor-enabled" type="checkbox" ${currentProctor ? 'checked' : ''} style="width:18px;height:18px">
          <label for="cfg-proctor-enabled" style="margin:0;font-size:14px;color:#334">Require webcam and screen sharing for this exam set</label>
        </div>

        <label class="label">Exam Mode</label>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin-bottom:16px">
          <label style="display:block;padding:14px;border:2px solid #d0d8e8;border-radius:14px;background:#fff;cursor:pointer">
            <input type="radio" name="cfg-exam-mode" value="GRADED" ${examMode !== 'PRACTICE' ? 'checked' : ''} onchange="syncExamModeHelp()" style="width:16px;height:16px;margin-right:8px">
            <strong style="color:#1F3864">Graded Exam</strong>
            <div style="font-size:12px;color:#666;margin-top:6px;line-height:1.55">Official exam behavior. Candidates do not see the answer key after submission.</div>
          </label>
          <label style="display:block;padding:14px;border:2px solid #8acb95;border-radius:14px;background:#f3fbf5;cursor:pointer">
            <input type="radio" name="cfg-exam-mode" value="PRACTICE" ${examMode === 'PRACTICE' ? 'checked' : ''} onchange="syncExamModeHelp()" style="width:16px;height:16px;margin-right:8px">
            <strong style="color:#1a5c1a">Practice / Knowledge Check</strong>
            <div style="font-size:12px;color:#466;margin-top:6px;line-height:1.55">Learning mode. Candidates can review right/wrong answers at the end.</div>
          </label>
        </div>

        <div id="cfg-practice-options" style="display:${examMode === 'PRACTICE' ? 'block' : 'none'};padding:12px 14px;border:1px solid #b8dfc1;border-radius:12px;background:#f3fbf5;margin-bottom:18px">
          <div class="checkbox-row" style="margin-top:0">
            <input id="cfg-show-correct" type="checkbox" ${showCorrectAnswers || examMode === 'PRACTICE' ? 'checked' : ''}>
            <label for="cfg-show-correct">Show correct answers and right/wrong review after practice submission</label>
          </div>
          <div class="checkbox-row">
            <input id="cfg-counts-results" type="checkbox" ${countsTowardResults ? 'checked' : ''} ${examMode === 'PRACTICE' ? 'disabled' : ''}>
            <label for="cfg-counts-results">Count attempts as official graded results</label>
          </div>
          <div style="font-size:12px;color:#5c735f;margin-top:8px">Practice attempts are intentionally kept separate from official graded behavior to avoid accidental confusion.</div>
        </div>

        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn btn-primary" onclick="saveQuestionSetConfig(${id}, '${_esc(setName)}')">Save Configuration</button>
          <button class="btn btn-secondary" onclick="${returnAction}">Cancel</button>
        </div>
      </div>
    </div>`);
  }

  async function saveQuestionSetConfig(id, setName) {
    const name = String($('cfg-name')?.value || '').trim();
    const description = String($('cfg-description')?.value || '').trim();
    const durationMinutes = Number($('cfg-duration')?.value || 0);
    const passPct = Number($('cfg-pass-pct')?.value || 0);
    const numQuestionsRaw = String($('cfg-num-questions')?.value || '').trim();
    const numQuestions = numQuestionsRaw === '' ? null : Number(numQuestionsRaw);
    const proctorEnabled = Boolean($('cfg-proctor-enabled')?.checked);
    const modeInput = document.querySelector('input[name="cfg-exam-mode"]:checked');
    const examMode = modeInput ? modeInput.value : 'GRADED';
    const showCorrectAnswers = examMode === 'PRACTICE' && $('cfg-show-correct')?.checked !== false;
    const countsTowardResults = examMode === 'PRACTICE' ? false : $('cfg-counts-results')?.checked !== false;

    if (!name) {
      modal('⚠️', 'Title Required', 'Please enter a title for the exam set.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    if (!Number.isInteger(durationMinutes) || durationMinutes < 1 || durationMinutes > 240) {
      modal('⚠️', 'Invalid Duration', 'Please enter a duration between 1 and 240 minutes.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    if (!Number.isInteger(passPct) || passPct < 1 || passPct > 100) {
      modal('⚠️', 'Invalid Passing Percentage', 'Please enter a passing percentage between 1 and 100.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    if (numQuestions !== null && (!Number.isInteger(numQuestions) || numQuestions < 1)) {
      modal('⚠️', 'Invalid Question Count', 'Questions delivered per candidate must be blank or a positive whole number.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }

    try {
      await apiJson(`/api/admin/question-sets/${id}/config`, {
        method: 'POST',
        body: JSON.stringify({
          name,
          description,
          durationMinutes,
          passPct,
          numQuestions,
          proctorEnabled,
          examMode,
          showCorrectAnswers,
          countsTowardResults
        })
      }, { timeoutMs: 10000, retries: 0 });

      modal('✅', 'Configuration Saved', `The configuration for "${name}" was updated.`, [{
        label: 'Continue',
        cls: 'btn-primary',
        action: () => {
          const current = root.__currentQuestionSet || null;
          if (current && current.id === id) openQuestionSet(id, name);
          else root.IE.admin.showAdmin();
        }
      }]);
    } catch (_e) {
      modal('❌', 'Update Failed', 'Could not update that exam set configuration.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  function syncExamModeHelp() {
    const modeInput = document.querySelector('input[name="cfg-exam-mode"]:checked');
    const examMode = modeInput ? modeInput.value : 'GRADED';
    const box = $('cfg-practice-options');
    const showCorrect = $('cfg-show-correct');
    const counts = $('cfg-counts-results');
    if (box) box.style.display = examMode === 'PRACTICE' ? 'block' : 'none';
    if (showCorrect && examMode === 'PRACTICE') showCorrect.checked = true;
    if (counts) {
      counts.disabled = examMode === 'PRACTICE';
      if (examMode === 'PRACTICE') counts.checked = false;
    }
  }

  async function activateQuestionSet(id) {
    try {
      await apiJson(`/api/admin/question-sets/${id}/activate`, { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 10000, retries: 0 });
      root.IE.admin.showAdmin();
    } catch (_e) {
      modal('❌', 'Activation Failed', 'Could not set that exam as the default.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  function deleteQuestionSet(id, name) {
    modal('⚠️', 'Delete Exam Set', `Delete "${name}"? This removes its questions and sections permanently.`, [
      { label: 'Delete', cls: 'btn-danger', action: async () => {
        try {
          await apiJson(`/api/admin/question-sets/${id}`, { method: 'DELETE' }, { timeoutMs: 10000, retries: 0 });
          root.IE.admin.showAdmin();
        } catch (_e) {
          modal('❌', 'Delete Failed', 'The exam set could not be deleted. Active sets cannot be deleted.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  function downloadQuestionTemplate() {
    const lines = [
      'q_num,stem,note,multi,option_1,option_2,option_3,option_4,option_5,option_6,correct_indices',
      '"1","What is the purpose of incident management?","Leave blank if you do not need a hint","false","Restore service quickly","Approve all changes","Create new services","Manage suppliers","","","0"',
      '"2","Which TWO items are service management dimensions?","Use pipe characters for multi-select answers","true","Organizations and people","Value streams and processes","Incident logging","Server patching","","","0|1"',
      '"3","What should a candidate do before starting the exam?","Optional hint shown to the candidate if you want","false","Read the instructions","Skip the tech check","Close the browser","Wait for a CAB meeting","","","0"'
    ];
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'exam_set_template_excel_friendly.csv';
    a.click();
    URL.revokeObjectURL(url);
  }

  // ---- CSV upload flow ----
  function showUploadQuestionSet() {
    S.screen = 'admin-upload';
    document.body.classList.remove('exam-bg');
    render(`<div class="admin-wrap">
      <div class="card" style="max-width:760px;margin:0 auto">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;margin-bottom:12px">
          <div>
            <div style="font-size:22px;font-weight:800;color:#1F3864">Upload Exam Set</div>
            <div style="font-size:13px;color:#666">Import a new exam from a spreadsheet-friendly CSV without changing application code.</div>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <button class="btn btn-secondary btn-sm" onclick="downloadQuestionTemplate()">Download Excel-Friendly Template</button>
            <button class="btn btn-secondary btn-sm" onclick="showAdmin()">← Back to Admin</button>
          </div>
        </div>
        <div style="padding:16px 18px;border:1px solid #d8e1f0;border-radius:14px;background:#f8fbff;margin-bottom:18px">
          <div style="font-size:15px;font-weight:800;color:#1F3864;margin-bottom:10px">How this works</div>
          <div style="font-size:13px;color:#445;line-height:1.7">
            1. Download the template and open it in Excel, Google Sheets, or Numbers.<br>
            2. Fill one row per question.<br>
            3. Save the sheet as <strong>CSV</strong>.<br>
            4. Upload it here and the app will create a new exam set for you.
          </div>
        </div>

        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-bottom:18px">
          <div style="padding:14px;border:1px solid #d8e1f0;border-radius:14px;background:#fff">
            <div style="font-size:14px;font-weight:800;color:#1F3864;margin-bottom:8px">Required columns</div>
            <div style="font-size:12px;color:#555;line-height:1.7">
              <strong>q_num</strong>: question number<br>
              <strong>stem</strong>: full question text<br>
              <strong>multi</strong>: <code>true</code> or <code>false</code><br>
              <strong>correct_indices</strong>: answer position(s)
            </div>
          </div>
          <div style="padding:14px;border:1px solid #d8e1f0;border-radius:14px;background:#fff">
            <div style="font-size:14px;font-weight:800;color:#1F3864;margin-bottom:8px">Options</div>
            <div style="font-size:12px;color:#555;line-height:1.7">
              Add answer choices in <code>option_1</code>, <code>option_2</code>, and so on.<br>
              Leave unused option columns blank.
            </div>
          </div>
          <div style="padding:14px;border:1px solid #d8e1f0;border-radius:14px;background:#fff">
            <div style="font-size:14px;font-weight:800;color:#1F3864;margin-bottom:8px">Correct answers</div>
            <div style="font-size:12px;color:#555;line-height:1.7">
              Use <strong>zero-based indexes</strong>.<br>
              Single answer: <code>0</code><br>
              Multi answer: <code>0|2</code>
            </div>
          </div>
        </div>

        <label class="label">Exam Name</label>
        <input id="upload-name" type="text" placeholder="e.g. Academy Practice Exam A">
        <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">This is the name admins will see when assigning the exam to candidates.</div>
        <label class="label">Description</label>
        <input id="upload-desc" type="text" placeholder="Optional">
        <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">Optional notes like cohort, language, version, or intended audience.</div>
        <label class="label">CSV File</label>
        <input id="upload-file" type="file" accept=".csv" style="width:100%" onchange="previewUploadedQuestionSet()">
        <div style="font-size:12px;color:#666;margin:10px 0 18px;line-height:1.7">
          Upload the CSV exported from your spreadsheet. The template already contains the correct headers, sample rows, and formatting examples.<br>
          Tip: if Excel asks how to save, choose <strong>CSV UTF-8</strong> when available.
        </div>
        <div id="upload-preview" style="margin:0 0 18px;padding:14px;border:1px solid #d8e1f0;border-radius:14px;background:#fff">
          <div style="font-size:12px;color:#777">Select a CSV to preview row count, warnings, and import readiness.</div>
        </div>
        <button class="btn btn-primary btn-full" onclick="submitUploadedQuestionSet()">Upload Exam Set</button>
      </div>
    </div>`);
  }

  function parseQuestionCsv(text) {
    const rows = [];
    let row = [];
    let cell = '';
    let inQuotes = false;
    for (let i = 0; i < text.length; i += 1) {
      const ch = text[i];
      const next = text[i + 1];
      if (inQuotes) {
        if (ch === '"' && next === '"') {
          cell += '"';
          i += 1;
        } else if (ch === '"') {
          inQuotes = false;
        } else {
          cell += ch;
        }
      } else if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        row.push(cell);
        cell = '';
      } else if (ch === '\n') {
        row.push(cell);
        rows.push(row);
        row = [];
        cell = '';
      } else if (ch !== '\r') {
        cell += ch;
      }
    }
    if (cell.length || row.length) {
      row.push(cell);
      rows.push(row);
    }
    if (!rows.length) return [];

    const headers = rows[0].map((h) => String(h || '').trim().toLowerCase());
    const optionIndexes = headers
      .map((header, idx) => ({ header, idx }))
      .filter((item) => item.header.startsWith('option_'))
      .map((item) => item.idx);

    const qNumIdx = headers.indexOf('q_num');
    const stemIdx = headers.indexOf('stem');
    const noteIdx = headers.indexOf('note');
    const multiIdx = headers.indexOf('multi');
    const correctIdx = headers.indexOf('correct_indices');
    if (qNumIdx === -1 || stemIdx === -1 || multiIdx === -1 || correctIdx === -1 || !optionIndexes.length) {
      throw new Error('CSV headers are missing required columns.');
    }

    return rows.slice(1).filter((r) => r.some((cellValue) => String(cellValue || '').trim() !== '')).map((r) => {
      const opts = optionIndexes.map((idx) => String(r[idx] || '').trim()).filter(Boolean);
      const correctIndices = String(r[correctIdx] || '')
        .split(/[|,]/)
        .map((val) => Number(val.trim()))
        .filter((n) => Number.isInteger(n) && n >= 0);
      return {
        qNum: Number(r[qNumIdx]),
        stem: String(r[stemIdx] || '').trim(),
        note: noteIdx === -1 ? '' : String(r[noteIdx] || '').trim(),
        multi: /^(true|yes|1)$/i.test(String(r[multiIdx] || '').trim()),
        opts,
        correctIndices
      };
    });
  }

  function analyzeQuestionUpload(questions) {
    const errors = [];
    const warnings = [];
    const qNums = new Set();
    const stems = new Set();
    const duplicates = [];
    const section = {
      count: questions.length,
      multiCount: 0,
      maxOptions: 0
    };
    for (let i = 0; i < questions.length; i += 1) {
      const q = questions[i];
      const rowLabel = `Row ${i + 2}`;
      if (!Number.isInteger(q.qNum) || q.qNum < 1) errors.push(`${rowLabel}: invalid q_num`);
      if (!q.stem) errors.push(`${rowLabel}: missing stem`);
      if (!Array.isArray(q.opts) || q.opts.length < 2) errors.push(`${rowLabel}: need at least 2 options`);
      if (!Array.isArray(q.correctIndices) || !q.correctIndices.length) errors.push(`${rowLabel}: missing correct_indices`);
      if (!q.multi && q.correctIndices.length !== 1) errors.push(`${rowLabel}: single-select needs exactly 1 correct index`);
      if (q.correctIndices.some((idx) => idx >= q.opts.length)) errors.push(`${rowLabel}: correct index out of range`);
      if (qNums.has(q.qNum)) duplicates.push(`Question number ${q.qNum}`);
      else qNums.add(q.qNum);
      const stemKey = String(q.stem || '').trim().toLowerCase();
      if (stemKey) {
        if (stems.has(stemKey)) warnings.push(`${rowLabel}: duplicate stem detected`);
        else stems.add(stemKey);
      }
      if (new Set(q.opts.map((opt) => String(opt).trim().toLowerCase())).size !== q.opts.length) {
        warnings.push(`${rowLabel}: duplicate option text`);
      }
      if (q.multi) section.multiCount += 1;
      section.maxOptions = Math.max(section.maxOptions, q.opts.length);
    }
    duplicates.forEach((item) => warnings.push(item));
    return {
      ok: errors.length === 0,
      errors,
      warnings,
      summary: section
    };
  }

  async function previewUploadedQuestionSet() {
    const file = $('upload-file')?.files?.[0];
    root._uploadPreview = null;
    const host = $('upload-preview');
    if (!host) return;
    if (!file) {
      host.innerHTML = '<div style="font-size:12px;color:#777">Select a CSV to preview row count, warnings, and import readiness.</div>';
      return;
    }
    try {
      const text = await file.text();
      const questions = parseQuestionCsv(text);
      const analysis = analyzeQuestionUpload(questions);
      let serverPreview = null;
      if (roleCan('imports:write')) {
        try {
          serverPreview = await apiJson('/api/admin/question-sets/upload/preview', {
            method: 'POST',
            body: JSON.stringify({ questions })
          }, { timeoutMs: 15000, retries: 0 });
        } catch (_e) {
          serverPreview = null;
        }
      }
      root._uploadPreview = { questions, analysis, serverPreview };
      host.innerHTML = `
        <div style="font-size:13px;color:#1F3864;font-weight:800;margin-bottom:8px">Preview</div>
        <div style="font-size:12px;color:#445;line-height:1.8">
          Rows: <strong>${analysis.summary.count}</strong><br>
          Multi-select: <strong>${analysis.summary.multiCount}</strong><br>
          Max options: <strong>${analysis.summary.maxOptions}</strong><br>
          Status: <strong style="color:${analysis.ok ? '#1a5c1a' : '#c0392b'}">${analysis.ok ? 'Ready to upload' : 'Fix issues first'}</strong>
        </div>
        ${serverPreview?.duplicatesAgainstDatabase?.length ? `<div style="margin-top:10px;font-size:12px;color:#8a5b00">Possible duplicates already in database:<br>${serverPreview.duplicatesAgainstDatabase.slice(0, 5).map((item) => `• ${_esc(String(item).slice(0, 120))}`).join('<br>')}</div>` : ''}
        ${analysis.errors.length ? `<div style="margin-top:10px;font-size:12px;color:#9f2d22">${analysis.errors.slice(0, 8).map((item) => `• ${_esc(item)}`).join('<br>')}</div>` : ''}
        ${(analysis.warnings.length || serverPreview?.warnings?.length) ? `<div style="margin-top:10px;font-size:12px;color:#8a5b00">${[...analysis.warnings, ...(serverPreview?.warnings || [])].slice(0, 8).map((item) => `• ${_esc(item)}`).join('<br>')}</div>` : ''}
      `;
    } catch (err) {
      host.innerHTML = `<div style="font-size:12px;color:#9f2d22">Preview failed: ${_esc(err.message || 'Could not parse CSV')}</div>`;
    }
  }

  async function submitUploadedQuestionSet() {
    const name = String($('upload-name')?.value || '').trim();
    const description = String($('upload-desc')?.value || '').trim();
    const file = $('upload-file')?.files?.[0];
    if (!name) {
      modal('⚠️', 'Name Required', 'Please enter a name for the exam set.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    if (!file) {
      modal('⚠️', 'CSV Required', 'Please select a CSV file to upload.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    try {
      if (!root._uploadPreview) await previewUploadedQuestionSet();
      const questions = root._uploadPreview?.questions || [];
      const analysis = root._uploadPreview?.analysis || analyzeQuestionUpload(questions);
      if (!questions.length) throw new Error('The CSV does not contain any questions.');
      if (!analysis.ok) throw new Error(analysis.errors[0] || 'CSV validation failed.');
      const resp = await apiJson('/api/admin/question-sets/upload', {
        method: 'POST',
        body: JSON.stringify({ name, description, questions })
      }, { timeoutMs: 20000, retries: 0 });
      if (!resp || !resp.ok) throw new Error('upload_failed');
      const note = analysis.warnings.length ? `\n\nWarnings noted: ${analysis.warnings.slice(0, 3).join(' · ')}` : '';
      modal('✅', 'Upload Complete', `${resp.count} questions were imported into "${name}".${note}`, [{ label: 'Manage Exam Set', cls: 'btn-primary', action: () => openQuestionSet(resp.questionSetId, name) }]);
    } catch (err) {
      modal('❌', 'Upload Failed', err.message || 'Could not import that CSV file.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  // ---- Open / edit question set ----
  async function openQuestionSet(setId, fallbackName) {
    S.screen = 'admin-question-set';
    render('<div class="admin-wrap"><div style="padding:60px;text-align:center;color:white;font-size:18px">Loading exam set…</div></div>');
    try {
      const [qData, sData, setList] = await Promise.all([
        apiJson(`/api/admin/question-sets/${setId}/questions`, {}, { timeoutMs: 12000, retries: 1 }),
        apiJson(`/api/admin/question-sets/${setId}/sections`, {}, { timeoutMs: 12000, retries: 1 }),
        apiJson('/api/admin/question-sets', {}, { timeoutMs: 12000, retries: 1 })
      ]);
      const questionSet = qData?.questionSet || {};
      const questions = Array.isArray(qData?.questions) ? qData.questions : [];
      const sections = Array.isArray(sData?.sections) ? sData.sections : [];
      const setMeta = (setList?.sets || []).find((item) => item.id === setId) || {};
      const sectionMap = new Map(sections.map((s) => [s.id, s]));
      const questionRows = questions.map((q) => `
        <tr>
          <td style="text-align:center">${q.qNum}</td>
          <td>${_esc(String(q.stem || '').slice(0, 120))}${String(q.stem || '').length > 120 ? '…' : ''}</td>
          <td>${_esc(sectionMap.get(q.sectionId)?.name || '—')}</td>
          <td style="text-align:center">${q.multi ? 'Multi' : 'Single'}</td>
          <td style="text-align:center">${Array.isArray(q.opts) ? q.opts.length : 0}</td>
          <td style="text-align:center;white-space:nowrap">
            <button class="btn btn-secondary btn-sm" onclick="showQuestionEditor(${setId}, ${q.id})">Edit</button>
            <button class="btn btn-danger btn-sm" onclick="deleteQuestion(${setId}, ${q.id})">Delete</button>
          </td>
        </tr>`).join('');
      const sectionRows = sections.map((section) => `
        <tr>
          <td>${_esc(section.name)}</td>
          <td>${_esc(section.description || '—')}</td>
          <td style="text-align:center">${section.displayOrder || 0}</td>
          <td style="text-align:center">${section.drawCount == null ? '—' : section.drawCount}</td>
          <td style="text-align:center">${section.questionCount || 0}</td>
          <td style="text-align:center;white-space:nowrap">
            <button class="btn btn-secondary btn-sm" onclick="editSectionPrompt(${setId}, ${section.id})">Edit</button>
            <button class="btn btn-danger btn-sm" onclick="deleteSection(${setId}, ${section.id})">Delete</button>
          </td>
        </tr>`).join('');

      root.__currentQuestionSet = {
        id: setId,
        name: questionSet.name || fallbackName || 'Exam Set',
        description: questionSet.description || '',
        isActive: Boolean(questionSet.isActive),
        questions,
        sections,
        meta: setMeta
      };

      render(`<div class="admin-wrap">
        <div class="card" style="margin-bottom:16px">
          <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap">
            <div>
              <div style="font-size:22px;font-weight:800;color:#1F3864">${_esc(root.__currentQuestionSet.name)}</div>
              <div style="font-size:13px;color:#666;margin-top:4px">${_esc(root.__currentQuestionSet.description || 'No description')} ${root.__currentQuestionSet.isActive ? '· Default exam set' : ''}</div>
              <div style="font-size:12px;color:#777;margin-top:6px">${questions.length} questions · ${sections.length} sections · ${setMeta.numQuestions ? `${setMeta.numQuestions} delivered per candidate` : 'All questions delivered'} · ${setMeta.durationMinutes || 45}m · ${setMeta.passPct || 80}% target · ${setMeta.examMode === 'PRACTICE' ? 'Practice mode' : 'Graded mode'}</div>
            </div>
            <div style="display:flex;gap:8px;flex-wrap:wrap">
              <button class="btn btn-primary btn-sm" onclick="showQuestionEditor(${setId})">+ Add Question</button>
              <button class="btn btn-secondary btn-sm" onclick="editSectionPrompt(${setId})">+ Add Section</button>
              <button class="btn btn-secondary btn-sm" onclick="configQuestionSet(${setId}, ${setMeta.durationMinutes || 45}, ${setMeta.passPct || 80}, ${setMeta.proctorEnabled !== false}, ${setMeta.numQuestions == null ? 'null' : setMeta.numQuestions}, ${setMeta.questionCount || questions.length})">Config</button>
              <button class="btn btn-secondary btn-sm" onclick="showAdmin()">← Back</button>
            </div>
          </div>
        </div>
        <div class="card" style="margin-bottom:16px">
          <div style="font-size:16px;font-weight:800;color:#1F3864;margin-bottom:10px">Sections</div>
          <div style="overflow-x:auto">
            <table class="admin-table">
              <thead><tr><th>Name</th><th>Description</th><th style="text-align:center">Order</th><th style="text-align:center">Draw</th><th style="text-align:center">Questions</th><th style="text-align:center">Actions</th></tr></thead>
              <tbody>${sectionRows || '<tr><td colspan="6" style="text-align:center;color:#888;padding:16px">No sections defined yet</td></tr>'}</tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:10px">
            <div style="font-size:16px;font-weight:800;color:#1F3864">Questions</div>
            <div style="font-size:12px;color:#666">Question content stays in HANA; candidates only receive one question at a time.</div>
          </div>
          <div style="overflow-x:auto">
            <table class="admin-table">
              <thead><tr><th style="text-align:center">#</th><th>Stem</th><th>Section</th><th style="text-align:center">Type</th><th style="text-align:center">Opts</th><th style="text-align:center">Actions</th></tr></thead>
              <tbody>${questionRows || '<tr><td colspan="6" style="text-align:center;color:#888;padding:16px">No questions in this exam set yet</td></tr>'}</tbody>
            </table>
          </div>
        </div>
      </div>`);
    } catch (_e) {
      modal('❌', 'Load Failed', 'Could not load that exam set.', [{ label: 'Back to Admin', cls: 'btn-primary', action: () => root.IE.admin.showAdmin() }]);
    }
  }

  function showQuestionEditor(setId, questionId) {
    const current = root.__currentQuestionSet || { questions: [], sections: [] };
    const question = current.questions.find((item) => item.id === questionId) || null;
    const sectionOptions = ['<option value="">No section</option>']
      .concat((current.sections || []).map((section) => `<option value="${section.id}" ${question?.sectionId === section.id ? 'selected' : ''}>${_esc(section.name)}</option>`))
      .join('');
    const optionLines = question ? (question.opts || []).join('\n') : '';
    const answerLines = question ? (question.correctIndices || []).join(',') : '';
    render(`<div class="admin-wrap">
      <div class="card" style="max-width:860px;margin:0 auto">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;margin-bottom:12px">
          <div>
            <div style="font-size:22px;font-weight:800;color:#1F3864">${question ? 'Edit Question' : 'Add Question'}</div>
            <div style="font-size:13px;color:#666">${_esc(current.name || 'Exam Set')}</div>
          </div>
          <button class="btn btn-secondary btn-sm" onclick="openQuestionSet(${setId}, '${_esc(current.name || '')}')">← Back</button>
        </div>
        <label class="label">Question Number</label>
        <input id="qe-qnum" type="number" min="1" value="${question?.qNum || (current.questions.length + 1)}">
        <label class="label">Question Stem</label>
        <textarea id="qe-stem" rows="5" style="width:100%;padding:12px;border:1px solid #d0d8e8;border-radius:12px">${_esc(question?.stem || '')}</textarea>
        <label class="label">Note / Hint</label>
        <input id="qe-note" type="text" value="${_esc(question?.note || '')}" placeholder="Optional">
        <label class="label">Section</label>
        <select id="qe-section">${sectionOptions}</select>
        <label class="label">Question Type</label>
        <select id="qe-multi">
          <option value="false" ${question?.multi ? '' : 'selected'}>Single-select</option>
          <option value="true" ${question?.multi ? 'selected' : ''}>Multi-select</option>
        </select>
        <label class="label">Options (one per line)</label>
        <textarea id="qe-opts" rows="8" style="width:100%;padding:12px;border:1px solid #d0d8e8;border-radius:12px" placeholder="Option A&#10;Option B&#10;Option C">${_esc(optionLines)}</textarea>
        <label class="label">Correct Option Indexes</label>
        <input id="qe-correct" type="text" value="${_esc(answerLines)}" placeholder="e.g. 1 or 0,2">
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:16px">
          <button class="btn btn-primary" onclick="saveQuestionEditor(${setId}, ${question ? question.id : 'null'})">${question ? 'Save Question' : 'Create Question'}</button>
          <button class="btn btn-secondary" onclick="openQuestionSet(${setId}, '${_esc(current.name || '')}')">Cancel</button>
        </div>
      </div>
    </div>`);
  }

  async function saveQuestionEditor(setId, questionId) {
    const opts = String($('qe-opts')?.value || '').split('\n').map((line) => line.trim()).filter(Boolean);
    const correctIndices = String($('qe-correct')?.value || '').split(',').map((item) => Number(item.trim())).filter((n) => Number.isInteger(n) && n >= 0);
    try {
      await apiJson(`/api/admin/question-sets/${setId}/questions`, {
        method: 'POST',
        body: JSON.stringify({
          id: questionId,
          qNum: Number($('qe-qnum')?.value || 0),
          stem: String($('qe-stem')?.value || ''),
          note: String($('qe-note')?.value || ''),
          sectionId: $('qe-section')?.value || null,
          multi: $('qe-multi')?.value === 'true',
          opts,
          correctIndices
        })
      }, { timeoutMs: 12000, retries: 0 });
      openQuestionSet(setId, root.__currentQuestionSet?.name || '');
    } catch (_e) {
      modal('❌', 'Save Failed', 'Could not save that question. Please check the question number, options, and correct indexes.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  function deleteQuestion(setId, questionId) {
    modal('⚠️', 'Delete Question', 'Delete this question from the exam set?', [
      { label: 'Delete', cls: 'btn-danger', action: async () => {
        try {
          await apiJson(`/api/admin/question-sets/${setId}/questions/${questionId}`, { method: 'DELETE' }, { timeoutMs: 10000, retries: 0 });
          openQuestionSet(setId, root.__currentQuestionSet?.name || '');
        } catch (_e) {
          modal('❌', 'Delete Failed', 'Could not delete that question.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  async function editSectionPrompt(setId, sectionId) {
    if (sectionId === undefined) sectionId = null;
    const current = root.__currentQuestionSet || { sections: [] };
    const section = current.sections.find((item) => item.id === sectionId) || null;
    const name = window.prompt('Section name:', section?.name || '');
    if (!name || !name.trim()) return;
    const description = window.prompt('Section description (optional):', section?.description || '') || '';
    const displayOrder = window.prompt('Display order:', section ? String(section.displayOrder || 0) : '0');
    if (displayOrder == null) return;
    const drawCount = window.prompt('Draw count (blank = no section quota):', section?.drawCount == null ? '' : String(section.drawCount));
    if (drawCount == null) return;
    try {
      await apiJson(`/api/admin/question-sets/${setId}/sections`, {
        method: 'POST',
        body: JSON.stringify({
          id: sectionId,
          name: name.trim(),
          description: description.trim(),
          displayOrder: Number(displayOrder),
          drawCount: drawCount.trim() === '' ? null : Number(drawCount)
        })
      }, { timeoutMs: 10000, retries: 0 });
      openQuestionSet(setId, current.name || '');
    } catch (_e) {
      modal('❌', 'Section Save Failed', 'Could not save that section.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  function deleteSection(setId, sectionId) {
    modal('⚠️', 'Delete Section', 'Delete this section? Questions stay in the set and become unsectioned.', [
      { label: 'Delete', cls: 'btn-danger', action: async () => {
        try {
          await apiJson(`/api/admin/question-sets/${setId}/sections/${sectionId}`, { method: 'DELETE' }, { timeoutMs: 10000, retries: 0 });
          openQuestionSet(setId, root.__currentQuestionSet?.name || '');
        } catch (_e) {
          modal('❌', 'Delete Failed', 'Could not delete that section.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  // ---- Misc question-set ops ----
  async function exportQuestionSet(id) {
    try {
      const resp = await apiFetch(`/api/admin/question-sets/${id}/export.json`, {}, { timeoutMs: 12000, retries: 1 });
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `exam_set_${id}.json`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (_e) {
      modal('❌', 'Export Failed', 'Could not export that exam set.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  function cloneQuestionSet(id, name) {
    const nextName = window.prompt('Name for cloned exam set:', `${name} Copy`);
    if (!nextName) return;
    apiJson(`/api/admin/question-sets/${id}/clone`, {
      method: 'POST',
      body: JSON.stringify({ name: nextName })
    }, { timeoutMs: 20000, retries: 0 }).then(() => root.IE.admin.showAdmin()).catch(() => {
      modal('❌', 'Clone Failed', 'Could not clone that exam set.', [{ label: 'OK', cls: 'btn-primary' }]);
    });
  }

  function publishQuestionSet(id) {
    modal('⚠️', 'Publish Exam Version', 'Publish this version for its exam family? Older versions in the same family will be marked archived.', [
      { label: 'Publish', cls: 'btn-primary', action: async () => {
        try {
          await apiJson(`/api/admin/question-sets/${id}/publish`, { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 12000, retries: 0 });
          root.IE.admin.showAdmin();
        } catch (_e) {
          modal('❌', 'Publish Failed', 'Could not publish that exam version.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  function archiveQuestionSet(id) {
    modal('⚠️', 'Archive Exam Version', 'Archive this version? It will stay available for reporting but should not be used for new candidates.', [
      { label: 'Archive', cls: 'btn-danger', action: async () => {
        try {
          await apiJson(`/api/admin/question-sets/${id}/archive`, { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 12000, retries: 0 });
          root.IE.admin.showAdmin();
        } catch (_e) {
          modal('❌', 'Archive Failed', 'Could not archive that exam version.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  function rollbackImportedSet(id) {
    modal('⚠️', 'Rollback Imported Exam', 'Delete this imported exam set and all its questions? This only works when no results reference it.', [
      { label: 'Rollback', cls: 'btn-danger', action: async () => {
        try {
          await apiJson(`/api/admin/question-sets/${id}/rollback-import`, { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 12000, retries: 0 });
          root.IE.admin.showAdmin();
        } catch (_e) {
          modal('❌', 'Rollback Failed', 'Could not rollback that imported exam set.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  root.IE = root.IE || {};
  root.IE.questionSets = {
    assignQuestionSet: assignQuestionSet,
    showQuestionSetAnalytics: showQuestionSetAnalytics,
    createQuestionSet: createQuestionSet,
    configQuestionSet: configQuestionSet,
    saveQuestionSetConfig: saveQuestionSetConfig,
    syncExamModeHelp: syncExamModeHelp,
    activateQuestionSet: activateQuestionSet,
    deleteQuestionSet: deleteQuestionSet,
    downloadQuestionTemplate: downloadQuestionTemplate,
    showUploadQuestionSet: showUploadQuestionSet,
    parseQuestionCsv: parseQuestionCsv,
    analyzeQuestionUpload: analyzeQuestionUpload,
    previewUploadedQuestionSet: previewUploadedQuestionSet,
    submitUploadedQuestionSet: submitUploadedQuestionSet,
    openQuestionSet: openQuestionSet,
    showQuestionEditor: showQuestionEditor,
    saveQuestionEditor: saveQuestionEditor,
    deleteQuestion: deleteQuestion,
    editSectionPrompt: editSectionPrompt,
    deleteSection: deleteSection,
    exportQuestionSet: exportQuestionSet,
    cloneQuestionSet: cloneQuestionSet,
    publishQuestionSet: publishQuestionSet,
    archiveQuestionSet: archiveQuestionSet,
    rollbackImportedSet: rollbackImportedSet
  };
})(typeof window !== 'undefined' ? window : globalThis);
