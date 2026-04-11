'use strict';

const CFG = {
  webcamIntervalS: 28
};

let _adminToken = null;
let _adminRole = 'admin';
let _adminSystemStatus = null;
let _adminAuditEntries = [];
let _adminQuestionSets = [];
let _activeQuestionSet = null;
const API_TIMEOUT_MS = 12000;
const API_BASE_RETRY_MS = 600;
const SAVE_UI = { cls: 'save-pill', text: 'Saved' };
const S = {
  screen: 'code',
  code: '',
  examToken: null,
  currentQCache: null,
  answers: [],
  visited: new Set(),
  currentQ: 0,
  startTime: null,
  elapsed: 0,
  timerInterval: null,
  webcamInterval: null,
  incidents: [],
  tabSwitches: 0,
  webcamStream: null,
  screenStream: null,
  webcamOk: false,
  screenOk: false,
  submitted: false,
  durationSecs: 45 * 60,
  passPct: 80,
  passScore: 24,
  total: 30,
  proctorOn: true,
  examMode: 'GRADED',
  isPractice: false,
  showCorrectAnswers: false,
  freshStart: false,
  securityBound: false
};
let _blurTime = null;
let _progressSaveTimer = null;
let _adminRows = [];
let _selectedCodes = new Set();

function $(id) { return document.getElementById(id); }
function render(html) { $('app').innerHTML = html; }
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function _esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
function fmt(secs) {
  const s = Math.max(0, secs);
  return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
}
function durationLabel(totalSecs) {
  const m = Math.floor(totalSecs / 60);
  const s = totalSecs % 60;
  return `${m}m ${String(s).padStart(2, '0')}s`;
}
function logIncident(type, detail) {
  S.incidents.push({ time: new Date().toLocaleTimeString(), type, detail });
}
function proctorEnabled() {
  return S.proctorOn !== false;
}
function setSavePill(cls, text) {
  SAVE_UI.cls = cls;
  SAVE_UI.text = text;
  const el = $('save-pill');
  if (el) {
    el.className = cls;
    el.textContent = text;
  }
}
function markSaveStart() { setSavePill('save-pill saving', 'Saving...'); }
function markRetry(attempt) { setSavePill('save-pill retry', `Retry ${attempt}`); }
function markSaveDone(ok) { setSavePill(ok ? 'save-pill' : 'save-pill error', ok ? 'Saved' : 'Save failed'); }

async function _sha256(str) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, '0')).join('');
}

function errorMessage(err, fallback) {
  if (!err) return fallback;
  if (err.name === 'AbortError') return 'Request timed out';
  if (err.message) return err.message;
  return fallback;
}

async function apiFetch(url, opts = {}, cfg = {}) {
  const method = String(opts.method || 'GET').toUpperCase();
  const retries = Number.isInteger(cfg.retries) ? cfg.retries : (method === 'GET' ? 1 : 1);
  const timeoutMs = cfg.timeoutMs || API_TIMEOUT_MS;
  const isSave = !!cfg.isSave;

  if (isSave) markSaveStart();
  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const headers = { ...(opts.headers || {}) };
      if (_adminToken) headers['X-Admin-Token'] = _adminToken;
      if (S.examToken) headers['X-Exam-Token'] = S.examToken;
      const resp = await fetch(url, { ...opts, headers, signal: controller.signal });
      clearTimeout(timer);
      if (!resp.ok) {
        const e = new Error(`HTTP ${resp.status}`);
        e.status = resp.status;
        throw e;
      }
      if (isSave) markSaveDone(true);
      return resp;
    } catch (err) {
      clearTimeout(timer);
      lastErr = err;
      const status = err && err.status ? err.status : 0;
      const retryable = err.name === 'AbortError' || status === 0 || status === 429 || status >= 500;
      if (attempt < retries && retryable) {
        if (isSave) markRetry(attempt + 1);
        await sleep(API_BASE_RETRY_MS * Math.pow(2, attempt));
        continue;
      }
      if (isSave) markSaveDone(false);
      throw new Error(errorMessage(err, 'API request failed'));
    }
  }
  if (isSave) markSaveDone(false);
  throw new Error(errorMessage(lastErr, 'API request failed'));
}

async function apiJson(url, opts = {}, cfg = {}) {
  const headers = { ...(opts.headers || {}) };
  if (opts.body && !headers['Content-Type']) headers['Content-Type'] = 'application/json';
  const resp = await apiFetch(url, { ...opts, headers }, cfg);
  const ct = (resp.headers.get('content-type') || '').toLowerCase();
  if (!ct.includes('application/json')) return null;
  return resp.json();
}

function modal(icon, title, body, btns) {
  $('m-icon').textContent = icon;
  $('m-title').textContent = title;
  $('m-body').textContent = body;
  $('m-btns').innerHTML = btns.map((b, i) =>
    `<button class="btn ${b.cls || 'btn-primary'}" id="mb-${i}">${_esc(b.label)}</button>`
  ).join('');
  btns.forEach((b, i) => {
    const el = $(`mb-${i}`);
    if (!el) return;
    el.onclick = () => {
      $('modal').classList.remove('show');
      if (b.action) b.action();
    };
  });
  $('modal').classList.add('show');
}

function serializeProgress() {
  return {
    answers: S.answers,
    visited: [...S.visited],
    currentQ: S.currentQ,
    incidents: S.incidents,
    tabSwitches: S.tabSwitches,
    elapsedMs: S.elapsed + (S.startTime ? Date.now() - S.startTime : 0)
  };
}

async function saveProgress() {
  if (S.screen !== 'exam' || S.submitted || !S.code || !S.examToken) return;
  try {
    await apiJson('/api/progress', {
      method: 'POST',
      body: JSON.stringify({ code: S.code, ...serializeProgress() })
    }, { isSave: true, timeoutMs: 9000, retries: 1 });
  } catch (_e) {
    // save pill already reflects issue
  }
}

function queueProgressSave() {
  clearTimeout(_progressSaveTimer);
  _progressSaveTimer = setTimeout(() => { saveProgress(); }, 250);
}

async function fetchStatus() {
  try {
    return await apiJson('/api/status');
  } catch (_e) {
    return null;
  }
}

async function showCodeEntry() {
  S.screen = 'code';
  S.examToken = null;
  document.body.classList.remove('exam-bg');
  const status = await fetchStatus();
  const examName = status?.examName || 'ITIL 4 Foundation';
  const examActive = status?.examActive !== false;

  const logoBlock = `<div style="text-align:center;margin:40px 0 28px">
    <div style="display:inline-block;background:#1F3864;border-radius:12px;padding:18px 36px;box-shadow:0 6px 28px rgba(0,0,0,0.35)">
      <div style="font-size:11px;letter-spacing:3px;color:rgba(255,255,255,0.75);text-transform:uppercase;margin-bottom:8px">SAP Academy for Cloud Delivery</div>
      <div style="font-size:28px;font-weight:800;color:white;letter-spacing:1px">${_esc(examName)}</div>
    </div>
  </div>`;

  if (!examActive) {
    render(`<div class="screen" style="max-width:480px">${logoBlock}
      <div class="glass-card" style="text-align:center">
        <div style="font-size:48px;margin-bottom:12px">🔒</div>
        <h2 style="margin-bottom:8px">Exam Closed</h2>
        <p style="color:#666;font-size:14px;margin-bottom:8px">This exam is not currently open for access codes.</p>
        <p style="color:#999;font-size:13px">Please wait for your proctor to open the session, then refresh this page.</p>
      </div>
    </div>`);
    return;
  }

  render(`<div class="screen" style="max-width:480px">
    ${logoBlock}
    <div class="glass-card">
      <h2>Enter Your Access Code</h2>
      <p style="color:#666;font-size:14px;margin-bottom:20px">Enter the 6-character code provided to you by your proctor. The code is case-insensitive.</p>
      <input class="code-input" id="code-inp" type="text" maxlength="6"
        placeholder="• • • • • •" autocomplete="off" autocorrect="off" spellcheck="false"
        oninput="this.value=this.value.toUpperCase().replace(/[^A-Z0-9]/g,'')"
        onkeydown="if(event.key==='Enter')handleCodeSubmit()">
      <button class="btn btn-primary btn-full" style="margin-top:6px" onclick="handleCodeSubmit()">Continue →</button>
      <p style="font-size:11px;color:#bbb;text-align:center;margin-top:8px">If you are resuming an interrupted exam, enter the same code to restore your progress.</p>
    </div>
  </div>`);
  setTimeout(() => { const el = $('code-inp'); if (el) el.focus(); }, 60);
}

async function handleCodeSubmit() {
  const raw = String(($('code-inp')?.value || '')).trim().toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(raw)) {
    modal('⚠️', 'Invalid Code', 'Please enter your full 6-character access code.', [{ label: 'Try Again', cls: 'btn-primary' }]);
    return;
  }

  const btn = document.querySelector('.btn-primary');
  if (btn) btn.textContent = 'Checking...';

  let data;
  try {
    data = await apiJson('/api/validate', { method: 'POST', body: JSON.stringify({ code: raw }) }, { timeoutMs: 10000, retries: 1 });
  } catch (_e) {
    data = null;
  }

  if (!data || data.error) {
    modal('❌', 'Connection Error', 'Unable to reach the exam server. Please check your connection and try again.', [{ label: 'Try Again', cls: 'btn-primary' }]);
    if (btn) btn.textContent = 'Continue →';
    return;
  }

  if (!data.valid) {
    let title = 'Code Not Recognised';
    let msg = 'This code was not found. Please check your code and try again, or contact your proctor.';
    if (data.reason === 'exam_not_active') {
      title = 'Exam Not Live';
      msg = 'The exam is not currently open. Please wait for your proctor to open the exam session.';
    } else if (data.reason === 'too_many_attempts') {
      title = 'Too Many Attempts';
      msg = 'Too many incorrect attempts. Please wait 10 minutes before trying again.';
    }
    modal('❌', title, msg, [{ label: 'OK', cls: 'btn-primary' }]);
    if (btn) btn.textContent = 'Continue →';
    return;
  }

  S.code = raw;
  S.durationSecs = data.durationSecs || S.durationSecs;
  S.passPct = data.passPct || S.passPct;
  S.passScore = data.passScore || S.passScore;
  S.total = data.total || S.total;
  S.proctorOn = data.proctorEnabled !== false;
  S.examMode = data.examMode || 'GRADED';
  S.isPractice = data.isPractice === true || S.examMode === 'PRACTICE';
  S.showCorrectAnswers = data.showCorrectAnswers === true;

  if (data.status === 'completed' && data.result) {
    showResultsFromRecord(data.result);
    return;
  }

  if (data.status === 'active' && data.progress) {
    const answered = (data.progress.answers || []).filter((a) => Array.isArray(a) && a.length).length;
    modal('↩️', 'Resume Exam',
      `A saved session was found for code ${raw}.\n\nYou were on question ${(data.progress.currentQ || 0) + 1} with ${answered} questions answered.\n\nWould you like to resume where you left off?`,
      [
        { label: 'Resume', cls: 'btn-primary', action: () => { S.freshStart = false; proceedToTechCheck(true); } },
        { label: 'Start Fresh', cls: 'btn-secondary', action: startFresh }
      ]);
    return;
  }

  S.freshStart = false;
  showConsent();
}

function startFresh() {
  S.freshStart = true;
  showConsent();
}

function showConsent() {
  S.screen = 'consent';
  document.body.classList.remove('exam-bg');
  render(`<div class="screen" style="max-width:620px">
    <div class="card">
      <h2>Before You Begin</h2>
      ${S.isPractice ? `
      <div class="consent-box" style="background:#eef9f1;border-color:#8acb95">
        <strong>Practice / Knowledge Check:</strong>
        <ul>
          <li>This attempt is for learning and review, not an official graded exam.</li>
          <li>You will see which questions you got right or wrong after submitting.</li>
          <li>Use the final review to focus your study by question and segment.</li>
        </ul>
      </div>` : ''}
      ${proctorEnabled() ? `
      <p style="margin-bottom:14px;color:#555">This exam uses automated proctoring. Please read carefully.</p>
      <div class="consent-box">
        <strong>During this exam the system will:</strong>
        <ul>
          <li>Activate your webcam for continuous monitoring</li>
          <li>Require screen sharing to discourage external content usage</li>
          <li>Log tab switches, focus loss, and screenshot shortcut attempts</li>
          <li>Record answers, timing, and flagged events against your access code</li>
        </ul>
        <p style="margin-top:8px;font-size:12px;color:#777">Webcam images are analysed for proctoring only. No answer key is stored in the browser.</p>
      </div>` : ''}
      <div class="consent-box" style="background:#fff9ec;border-color:#f0c040">
        <strong>Exam rules:</strong>
        <ul>
          <li><strong>Time limit: ${Math.round(S.durationSecs / 60)} minutes</strong></li>
          <li>You may skip and return to questions freely</li>
          <li>All questions must be answered before submitting</li>
          <li>Multi-select questions require every correct option — partial selections score zero</li>
          <li>${S.isPractice ? 'Target score' : 'Passing score'}: ${S.passScore}/${S.total} (${S.passPct}%)</li>
        </ul>
      </div>
      <div class="checkbox-row">
        <input type="checkbox" id="cb-consent">
        <label for="cb-consent">I understand and agree to the exam conditions${proctorEnabled() ? ' and proctoring' : ''} described above</label>
      </div>
      <div style="margin-top:18px">
        <button class="btn btn-primary btn-full" onclick="handleConsentNext()">Continue →</button>
        <button class="btn btn-secondary btn-full" onclick="showCodeEntry()">← Back</button>
      </div>
    </div>
  </div>`);
}

function handleConsentNext() {
  if (!$('cb-consent')?.checked) {
    modal('⚠️', 'Consent Required', 'Please tick the box to confirm you accept the exam conditions.', [{ label: 'OK', cls: 'btn-primary' }]);
    return;
  }
  proceedToTechCheck(false);
}

function proceedToTechCheck(isResume) {
  if (!proctorEnabled()) {
    startExam();
    return;
  }
  showTechCheck(isResume);
}

function showTechCheck(isResume) {
  S.screen = 'tech';
  document.body.classList.remove('exam-bg');
  render(`<div class="screen" style="max-width:520px">
    <div class="card" style="margin-top:30px">
      <h2>${isResume ? 'Re-enable Monitoring' : 'Tech Check'}</h2>
      <p style="margin-bottom:18px;color:#666;font-size:14px">Both webcam and screen share must be active before the exam unlocks.</p>
      <div class="check-item">
        <div class="check-icon">📷</div>
        <div class="check-info"><strong>Webcam</strong><span>Required for continuous proctoring throughout the exam</span></div>
        <div id="st-cam"><span class="status-pend">Pending</span></div>
      </div>
      <div class="check-item">
        <div class="check-icon">🖥️</div>
        <div class="check-info"><strong>Screen Share</strong><span>Select your entire screen when prompted</span></div>
        <div id="st-screen"><span class="status-pend">Pending</span></div>
      </div>
      <div class="webcam-preview" id="cam-preview">
        <video id="preview-vid" autoplay muted playsinline></video>
        <p style="font-size:11px;color:#888;margin-top:4px">Ensure your face is clearly visible</p>
      </div>
      <div style="margin-top:18px">
        <button class="btn btn-primary btn-full" id="btn-cam" onclick="reqWebcam()">Enable Webcam</button>
        <button class="btn btn-secondary btn-full" id="btn-screen" onclick="reqScreen()" disabled>Share Screen</button>
        <button class="btn btn-success btn-full" id="btn-start" onclick="startExam()" disabled>${isResume ? 'Resume Exam' : 'Start Exam'} →</button>
        <button class="btn btn-secondary btn-full" onclick="showConsent()">← Back</button>
      </div>
    </div>
  </div>`);
}

async function reqWebcam() {
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ video: { width: 320, height: 240 }, audio: false });
    S.webcamStream = stream;
    S.webcamOk = true;
    $('st-cam').innerHTML = '<span class="status-ok">✓ Active</span>';
    $('btn-cam').textContent = '✓ Webcam Active';
    $('btn-cam').disabled = true;
    $('btn-screen').disabled = false;
    const pv = $('preview-vid');
    if (pv) pv.srcObject = stream;
    $('cam-preview').style.display = 'block';
    $('hidden-cam').srcObject = stream;
  } catch (_e) {
    $('st-cam').innerHTML = '<span class="status-err">Denied</span>';
    modal('❌', 'Webcam Required', 'Please allow camera access in your browser settings and try again.', [{ label: 'Retry', cls: 'btn-primary', action: reqWebcam }]);
  }
}

async function reqScreen() {
  try {
    const stream = await navigator.mediaDevices.getDisplayMedia({ video: { displaySurface: 'monitor' }, audio: false });
    S.screenStream = stream;
    S.screenOk = true;
    const track = stream.getVideoTracks()[0];
    track.addEventListener('ended', () => {
      logIncident('screen_stopped', 'Candidate stopped screen sharing mid-exam');
      if (S.screen === 'exam' && !S.submitted) {
        modal('🚨', 'Screen Share Stopped', 'You have stopped screen sharing. This has been logged. Please restart your screen share immediately.', [{ label: 'I understand', cls: 'btn-danger' }]);
      }
    });
    $('st-screen').innerHTML = '<span class="status-ok">✓ Sharing</span>';
    $('btn-screen').textContent = '✓ Screen Active';
    $('btn-screen').disabled = true;
    $('btn-start').disabled = false;
  } catch (_e) {
    $('st-screen').innerHTML = '<span class="status-err">Denied</span>';
    modal('❌', 'Screen Share Required', 'Screen sharing is required to sit this exam. Please try again and select your entire screen.', [{ label: 'Retry', cls: 'btn-primary', action: reqScreen }]);
  }
}

async function startExam() {
  if (proctorEnabled() && (!S.webcamOk || !S.screenOk)) {
    modal('⚠️', 'Camera/Screen Required', 'Proctoring is enabled for this exam. Please enable webcam and screen sharing before continuing.', [{ label: 'OK', cls: 'btn-primary' }]);
    return;
  }

  let data;
  try {
    data = await apiJson('/api/session/start', {
      method: 'POST',
      body: JSON.stringify({ code: S.code, fresh: S.freshStart })
    }, { timeoutMs: 12000, retries: 1 });
  } catch (_e) {
    data = null;
  }

  if (!data || !data.ok || !data.examToken) {
    modal('❌', 'Session Error', data?.error || 'Could not start exam session. Please check your connection and try again.', [{ label: 'Try Again', cls: 'btn-primary', action: startExam }]);
    return;
  }

  S.examToken = data.examToken;
  S.total = data.total || S.total;
  S.durationSecs = data.durationSecs || S.durationSecs;
  S.passPct = data.passPct || S.passPct;
  S.passScore = data.passScore || S.passScore;
  S.proctorOn = data.proctorEnabled !== false;
  S.examMode = data.examMode || S.examMode || 'GRADED';
  S.isPractice = data.isPractice === true || S.examMode === 'PRACTICE';
  S.showCorrectAnswers = data.showCorrectAnswers === true;
  S.submitted = false;
  S.startTime = Date.now();
  S.elapsed = 0;

  if (data.progress && !S.freshStart) {
    const p = data.progress;
    S.answers = p.answers || [];
    S.visited = new Set(p.visited || []);
    S.currentQ = p.currentQ || 0;
    S.incidents = p.incidents || [];
    S.tabSwitches = p.tabSwitches || 0;
    S.elapsed = p.elapsedMs || 0;
  } else {
    S.answers = Array(S.total).fill(null).map(() => []);
    S.visited = new Set();
    S.currentQ = 0;
    S.incidents = [];
    S.tabSwitches = 0;
  }

  S.freshStart = false;
  setupSecurity();
  startTimer();
  startProctor();
  await renderQ();
}

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

  const sel = S.answers[S.currentQ] || [];
  const answered = S.answers.filter((a) => a && a.length > 0).length;
  const pct = (answered / S.total) * 100;
  const isLast = S.currentQ === S.total - 1;
  const unanswered = S.answers.filter((a) => !a || a.length === 0).length;

  const opts = q.opts.map((text, displayIdx) => {
    const isSel = sel.includes(displayIdx);
    return `<button class="option${isSel ? ' selected' : ''}" onclick="pick(${displayIdx})">
      <span class="opt-letter">${'ABCDEF'[displayIdx]}</span>
      <span class="opt-text">${_esc(text)}</span>
    </button>`;
  }).join('');

  const dots = Array.from({ length: S.total }, (_, i) => {
    const a = S.answers[i] || [];
    const vis = S.visited.has(i);
    let cls = 'nav-dot';
    if (i === S.currentQ) cls += ' current';
    else if (a.length > 0) cls += ' answered';
    else if (vis) cls += ' skipped';
    return `<div class="${cls}" onclick="goToQ(${i})" title="Q${i + 1}"></div>`;
  }).join('');

  render(`<div class="no-select" style="min-height:100vh;display:flex;flex-direction:column">
    <div class="exam-header">
      <div class="header-info">
        <span class="header-title">SAP Academy for Cloud Delivery · ${S.isPractice ? 'Practice Knowledge Check' : 'Secure Exam'}</span>
        <span class="header-code">CODE: ${_esc(S.code)}</span>
      </div>
      <div style="display:flex;align-items:center;gap:8px">
        <div id="save-pill" class="${SAVE_UI.cls}">${_esc(SAVE_UI.text)}</div>
        <div id="timer" class="timer">--:--</div>
      </div>
    </div>
    <div style="background:white;padding:8px 20px;border-bottom:1px solid #e0e6f0">
      <div style="display:flex;justify-content:space-between;font-size:12px;color:#777;margin-bottom:5px">
        <span><strong>Q${S.currentQ + 1}</strong> of ${S.total} · <span style="color:${answered === S.total ? '#1a5c1a' : '#c55a11'}">${answered}/${S.total} answered</span></span>
        <span>${sel.length ? (q.multi ? `${sel.length} selected` : '✓ Answered') : '<span style="color:#c55a11">⚠ Unanswered</span>'}</span>
      </div>
      <div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div>
      <div class="nav-dots">${dots}</div>
      <div style="font-size:10px;color:#bbb;text-align:center;margin-top:3px">Tap any dot to jump · answered / skipped / not visited</div>
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
          <button class="btn btn-secondary" onclick="prevQ()" ${S.currentQ === 0 ? 'disabled' : ''}>← Back</button>
          <span class="sel-count">${sel.length ? (q.multi ? `${sel.length} selected` : '✓ Answered') : 'No answer'}</span>
          ${isLast
            ? `<button class="btn btn-primary" onclick="trySubmit()" ${unanswered > 0 ? 'disabled' : ''}>Submit Exam</button>`
            : `<button class="btn btn-primary" onclick="nextQ()">Next →</button>`}
        </div>
        ${S.isPractice ? '<p style="text-align:center;font-size:12px;color:#1a5c1a;margin-top:8px;font-weight:700">Practice mode: you will see right/wrong feedback after submission.</p>' : ''}
        ${isLast && unanswered > 0 ? `<p style="text-align:center;font-size:12px;color:#c55a11;margin-top:8px">⚠ ${unanswered} unanswered — use the dots above to go back.</p>` : ''}
      </div>
    </div>
  </div>
  ${proctorEnabled() ? `<div class="webcam-corner"><video id="exam-cam" autoplay muted playsinline></video><div class="webcam-label">PROCTORED</div></div>` : ''}`);

  const ec = $('exam-cam');
  if (ec && S.webcamStream && proctorEnabled()) ec.srcObject = S.webcamStream;
  setSavePill(SAVE_UI.cls, SAVE_UI.text);
  updateTimer();
  queueProgressSave();
}

function goToQ(i) {
  if (S.submitted) return;
  S.currentQ = i;
  renderQ();
}
function prevQ() {
  if (S.submitted || S.currentQ <= 0) return;
  S.currentQ -= 1;
  renderQ();
}
function nextQ() {
  if (S.submitted) return;
  S.currentQ = Math.min(S.total - 1, S.currentQ + 1);
  renderQ();
}
function pick(displayOptIdx) {
  if (S.submitted) return;
  const q = S.currentQCache;
  let a = [...(S.answers[S.currentQ] || [])];
  if (q.multi) a = a.includes(displayOptIdx) ? a.filter((x) => x !== displayOptIdx) : [...a, displayOptIdx].sort((x, y) => x - y);
  else a = [displayOptIdx];
  S.answers[S.currentQ] = a;
  queueProgressSave();
  renderQ();
}

function trySubmit() {
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

function setupSecurity() {
  if (S.securityBound) return;
  document.addEventListener('visibilitychange', onVisChange);
  window.addEventListener('blur', onBlur);
  window.addEventListener('focus', onFocus);
  S.securityBound = true;
}
function teardownSecurity() {
  document.removeEventListener('visibilitychange', onVisChange);
  window.removeEventListener('blur', onBlur);
  window.removeEventListener('focus', onFocus);
  S.securityBound = false;
}
function onBlur() {
  if (S.screen !== 'exam' || S.submitted) return;
  _blurTime = Date.now();
  logIncident('focus_lost', 'Window lost focus');
  queueProgressSave();
}
function onFocus() {
  if (S.screen !== 'exam' || S.submitted || !_blurTime) return;
  const awayMs = Date.now() - _blurTime;
  const secs = Math.round(awayMs / 1000);
  _blurTime = null;
  if (awayMs > 0 && awayMs < 450) logIncident('possible_screenshot', `Brief focus loss: ${awayMs}ms`);
  if (secs >= 2) {
    logIncident('focus_returned', `Away ${secs}s`);
    modal('⚠️', 'Browser Window Lost Focus', `Your exam window was inactive for ${secs} second${secs !== 1 ? 's' : ''}. Please keep this window open and in the foreground at all times.`, [{ label: 'Return to Exam', cls: 'btn-primary' }]);
  }
  queueProgressSave();
}
function onVisChange() {
  if (!document.hidden || S.screen !== 'exam' || S.submitted) return;
  S.tabSwitches += 1;
  logIncident('tab_switch', `Tab switch #${S.tabSwitches}`);
  queueProgressSave();
  setTimeout(() => {
    if (!document.hidden) {
      modal('🚨', 'Tab Switch Detected', `Tab or window switch #${S.tabSwitches} has been detected and logged. Please remain in the exam window.`, [{ label: 'Return to Exam', cls: S.tabSwitches >= 3 ? 'btn-danger' : 'btn-primary' }]);
    }
  }, 350);
}

function startProctor() {
  if (!proctorEnabled()) return;
  clearInterval(S.webcamInterval);
  S.webcamInterval = setInterval(proctor, CFG.webcamIntervalS * 1000);
}
async function proctor() {
  if (S.submitted || S.screen !== 'exam') return;
  try {
    const v = $('hidden-cam');
    if (!v || !v.videoWidth) return;
    const c = $('cap-canvas');
    c.width = Math.min(v.videoWidth, 320);
    c.height = Math.min(v.videoHeight, 240);
    c.getContext('2d').drawImage(v, 0, 0, c.width, c.height);
    const b64 = c.toDataURL('image/jpeg', 0.65).split(',')[1];
    const d = await apiJson('/api/proctor/check', {
      method: 'POST',
      body: JSON.stringify({ imageB64: b64 })
    }, { timeoutMs: 15000, retries: 0 });
    if (!d || d.enabled === false) return;
    if (d.flag) {
      logIncident('ai_flag', d.reason || 'AI proctoring flag');
      queueProgressSave();
      modal('🚨', 'Proctoring Alert', `An automated proctoring check flagged this concern:\n\n"${d.reason || 'Suspicious behaviour detected'}"\n\nThis event has been logged for review.`, [{ label: 'I understand', cls: 'btn-danger' }]);
    }
  } catch (_e) {
    // soft fail
  }
}

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
  }
});

async function submitExam(autoSubmit) {
  if (S.submitted) return;
  S.submitted = true;
  clearInterval(S.timerInterval);
  clearInterval(S.webcamInterval);
  teardownSecurity();
  if (S.webcamStream) S.webcamStream.getTracks().forEach((t) => t.stop());
  if (S.screenStream) S.screenStream.getTracks().forEach((t) => t.stop());

  const durationSecs = Math.round((S.elapsed + (Date.now() - S.startTime)) / 1000);
  let data;
  try {
    data = await apiJson('/api/submit', {
      method: 'POST',
      body: JSON.stringify({
        code: S.code,
        answers: S.answers,
        durationSecs,
        tabSwitches: S.tabSwitches,
        incidents: S.incidents,
        autoSubmit
      })
    }, { timeoutMs: 15000, retries: 0 });
  } catch (_e) {
    data = null;
  }

  if (!data || !data.ok || !data.result) {
    modal('❌', 'Submit Failed', 'The exam could not be submitted. Please contact your proctor immediately with your access code.', [{ label: 'OK', cls: 'btn-danger' }]);
    return;
  }
  showResultsFromRecord(data.result);
}

function showResultsFromRecord(rec) {
  S.screen = 'results';
  document.body.classList.remove('exam-bg');
  const duration = durationLabel(rec.durationSecs || 0);
  const sectionResults = Array.isArray(rec.sectionResults) ? rec.sectionResults : [];
  const questionResults = Array.isArray(rec.questionResults) ? rec.questionResults : [];
  const showPracticeReview = rec.showCorrectAnswers === true && questionResults.length > 0;
  const passPct = Number(rec.passPct || S.passPct || 80);
  const passScore = rec.total ? Math.ceil((Number(rec.total) * passPct) / 100) : (S.passScore || 0);
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
        <div><strong>Duration:</strong> ${duration}</div>
        <div><strong>Submitted:</strong> ${new Date(rec.submittedAt).toLocaleString()}</div>
        ${rec.tabSwitches > 0 ? `<div style="color:#c55a11"><strong>Tab switches:</strong> ${rec.tabSwitches}</div>` : ''}
        ${rec.incidentCount > 0 ? `<div style="color:#c55a11"><strong>Flags logged:</strong> ${rec.incidentCount}</div>` : ''}
      </div>
      ${sectionBreakdown}
      ${practiceReview}
      <div class="divider"></div>
      <p style="font-size:13px;color:#999">${rec.isPractice ? 'Your practice attempt has been saved for learning analytics.' : 'Your result has been recorded. You may close this window.'}</p>
    </div>
  </div>`);
}

function statusChip(row) {
  if (row.status === 'completed' && row.isPractice) return '<span class="chip chip-pass">PRACTICE</span>';
  if (row.status === 'completed') return `<span class="chip ${row.pass ? 'chip-pass' : 'chip-fail'}">${row.pass ? 'PASS' : 'FAIL'}</span>`;
  if (row.status === 'active') return '<span class="chip chip-active">ACTIVE</span>';
  return '<span class="chip chip-unused">UNUSED</span>';
}

async function showAdminLogin() {
  S.screen = 'admin-login';
  document.body.classList.remove('exam-bg');
  render(`<div class="screen" style="max-width:380px">
    <div class="card" style="margin-top:80px">
      <h2>Admin Access</h2>
      <p style="margin-bottom:18px;color:#666;font-size:14px">Proctor console — restricted access.</p>
      <label class="label">Password</label>
      <input type="password" id="pwd" placeholder="Admin password" autocomplete="off" onkeydown="if(event.key==='Enter')doLogin()">
      <button class="btn btn-primary btn-full" onclick="doLogin()">Access Console</button>
      <button class="btn btn-secondary btn-full" onclick="showCodeEntry()">← Back</button>
    </div>
  </div>`);
}

async function doLogin() {
  const entered = String($('pwd')?.value || '');
  const hash = await _sha256(entered);
  let resp;
  try {
    resp = await apiJson('/api/admin/login', { method: 'POST', body: JSON.stringify({ hash }) }, { timeoutMs: 10000, retries: 0 });
  } catch (_e) {
    resp = null;
  }
  if (!resp || !resp.ok || !resp.token) {
    modal('❌', 'Incorrect Password', 'The password you entered is incorrect, or the server could not verify it.', [{ label: 'Try Again', cls: 'btn-primary' }]);
    return;
  }
  _adminToken = resp.token;
  _adminRole = resp.role || 'admin';
  showAdmin();
}

function summaryValue(rows, status) {
  return rows.filter((r) => r.status === status).length;
}

function seatSortValue(row) {
  const label = String(row?.label || '');
  const match = label.match(/(\d+)/);
  if (match) return Number(match[1]);
  return Number.MAX_SAFE_INTEGER;
}

function sortAdminRows(rows) {
  return [...rows].sort((a, b) => {
    const seatDiff = seatSortValue(a) - seatSortValue(b);
    if (seatDiff !== 0) return seatDiff;
    return String(a.code || '').localeCompare(String(b.code || ''));
  });
}

function flagsFor(code) {
  const row = _adminRows.find((r) => r.code === code);
  if (!row || !row.incidents || !row.incidents.length) {
    modal('ℹ️', 'No Flags', 'No incidents are recorded for this access code.', [{ label: 'OK', cls: 'btn-primary' }]);
    return;
  }
  const body = row.incidents.map((i) => `• ${i.time || ''} ${i.type || ''}${i.detail ? ` — ${i.detail}` : ''}`).join('\n');
  modal('🚨', `Flags for ${code}`, body, [{ label: 'Close', cls: 'btn-primary' }]);
}

async function clearStaleSessions() {
  modal('⚠️', 'Clear Stale Sessions', `Clear all stale active sessions older than ${_adminSystemStatus?.staleSessionMinutes || 30} minutes? This will remove saved in-progress state for those stale entries only.`, [
    { label: 'Clear Stale Sessions', cls: 'btn-danger', action: async () => {
      try {
        const resp = await apiJson('/api/admin/clear-stale-sessions', { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 15000, retries: 0 });
        if (!resp || !resp.ok) throw new Error('clear_failed');
        modal('✅', 'Stale Sessions Cleared', `${resp.clearedCount} stale session(s) were cleared.`, [{ label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }]);
      } catch (_e) {
        modal('❌', 'Clear Failed', 'Could not clear stale sessions.', [{ label: 'OK', cls: 'btn-primary' }]);
      }
    }},
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function toggleExamAvailability(enabled) {
  const title = enabled ? 'Open Exams' : 'Close Exams';
  const body = enabled
    ? 'Candidates will be able to enter access codes again.'
    : 'Candidates will be blocked at the access-code screen until exams are turned back on.';
  modal(enabled ? '🟢' : '⛔', title, body, [
    {
      label: enabled ? 'Open Exams' : 'Close Exams',
      cls: enabled ? 'btn-primary' : 'btn-danger',
      action: async () => {
        try {
          await apiJson('/api/admin/exam-availability', {
            method: 'POST',
            body: JSON.stringify({ enabled })
          }, { timeoutMs: 10000, retries: 0 });
          showAdmin();
        } catch (_e) {
          modal('❌', 'Update Failed', 'Could not update exam availability.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }
    },
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function repairResultSummaries() {
  modal('🛠️', 'Repair Scores', 'This will refill missing score and percentage values from saved result records wherever possible.', [
    {
      label: 'Repair Scores',
      cls: 'btn-primary',
      action: async () => {
        try {
          const resp = await apiJson('/api/admin/results/repair-summaries', {
            method: 'POST',
            body: JSON.stringify({})
          }, { timeoutMs: 20000, retries: 0 });
          modal('✅', 'Repair Complete', `${resp.repaired || 0} completed row(s) were repaired.${resp.skipped ? ` ${resp.skipped} row(s) could not be repaired from historical data.` : ''}`, [
            { label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }
          ]);
        } catch (_e) {
          modal('❌', 'Repair Failed', 'Could not repair score summaries.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }
    },
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function clearResultSummaries() {
  modal('⚠️', 'Clear All Scores', 'This will blank the score and percentage summary columns for all access codes and completed results. The underlying result JSON remains, but the overview table will show blank scores until repaired.', [
    {
      label: 'Clear All Scores',
      cls: 'btn-danger',
      action: async () => {
        try {
          await apiJson('/api/admin/results/clear-summaries', {
            method: 'POST',
            body: JSON.stringify({})
          }, { timeoutMs: 20000, retries: 0 });
          modal('✅', 'Scores Cleared', 'All summary score fields were cleared.', [
            { label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }
          ]);
        } catch (_e) {
          modal('❌', 'Clear Failed', 'Could not clear the score summaries.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }
    },
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function deleteCode(code, status) {
  const detail = status === 'completed'
    ? 'This will permanently remove the code, its saved result, and any stored progress.'
    : status === 'active'
      ? 'This will permanently remove the code and any in-progress session.'
      : 'This will permanently remove the unused code.';
  modal('⚠️', 'Delete Access Code', `${detail}\n\nCode: ${code}`, [
    {
      label: 'Delete Code',
      cls: 'btn-danger',
      action: async () => {
        try {
          await apiJson(`/api/admin/codes/${encodeURIComponent(code)}`, { method: 'DELETE' }, { timeoutMs: 12000, retries: 0 });
          showAdmin();
        } catch (_e) {
          modal('❌', 'Delete Failed', 'The access code could not be deleted.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }
    },
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

function updateSelectedCodeCount() {
  const el = $('selected-code-count');
  if (el) el.textContent = String(_selectedCodes.size);
}

function toggleCodeSelection(code, checked) {
  if (checked) _selectedCodes.add(code);
  else _selectedCodes.delete(code);
  updateSelectedCodeCount();
}

function toggleAllVisibleCodes(checked) {
  for (const row of _adminRows) {
    if (checked) _selectedCodes.add(row.code);
    else _selectedCodes.delete(row.code);
  }
  document.querySelectorAll('.code-select').forEach((el) => { el.checked = checked; });
  updateSelectedCodeCount();
}

function selectAllVisibleCodes() {
  toggleAllVisibleCodes(true);
}

function clearCodeSelection() {
  _selectedCodes.clear();
  document.querySelectorAll('.code-select').forEach((el) => { el.checked = false; });
  updateSelectedCodeCount();
}

async function bulkDeleteCodes() {
  const codes = [..._selectedCodes];
  if (!codes.length) {
    modal('ℹ️', 'No Codes Selected', 'Select one or more access codes first.', [{ label: 'OK', cls: 'btn-primary' }]);
    return;
  }
  const selectedRows = _adminRows.filter((row) => _selectedCodes.has(row.code));
  const statusSummary = ['unused', 'active', 'completed'].map((status) => `${selectedRows.filter((row) => row.status === status).length} ${status}`).join('\n');
  modal('⚠️', 'Delete Selected Codes', `Delete ${codes.length} selected access code(s)?\n\n${statusSummary}\n\nThe codes will be removed from the normal admin view and can no longer be used. Historical result records are preserved when the database migration is installed.`, [
    {
      label: 'Delete Selected',
      cls: 'btn-danger',
      action: async () => {
        try {
          const resp = await apiJson('/api/admin/codes/bulk-delete', {
            method: 'POST',
            body: JSON.stringify({ codes })
          }, { timeoutMs: 30000, retries: 0 });
          _selectedCodes.clear();
          modal('✅', 'Codes Deleted', `${resp.deletedCount || 0} access code(s) were deleted.`, [{ label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }]);
        } catch (_e) {
          modal('❌', 'Bulk Delete Failed', 'The selected codes could not be deleted.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }
    },
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function reviewResult(code) {
  render('<div class="admin-wrap"><div style="padding:60px;text-align:center;color:white;font-size:18px">Loading candidate answers...</div></div>');
  try {
    const resp = await apiJson(`/api/admin/results/${encodeURIComponent(code)}/review`, {}, { timeoutMs: 12000, retries: 1 });
    if (!resp || !resp.ok) throw new Error('review_failed');
    if (!resp.reviewAvailable) {
      modal('ℹ️', 'Review Not Available', 'This completed exam does not include per-question answer detail. It was likely submitted before answer review was added.', [
        { label: 'Back to Admin', cls: 'btn-primary', action: () => showAdmin() }
      ]);
      return;
    }
    const result = resp.result || {};
    const questionResults = Array.isArray(result.questionResults) ? result.questionResults : [];
    const rows = questionResults.map((item, idx) => {
      const opts = Array.isArray(item.opts) ? item.opts : [];
      const formatIndexes = (indexes) => {
        if (!Array.isArray(indexes) || !indexes.length) return 'No answer selected';
        return indexes.map((originalIdx) => `${originalIdx + 1}. ${opts[originalIdx] || `Option ${originalIdx + 1}`}`).join('<br>');
      };
      return `
        <tr>
          <td style="text-align:center">${idx + 1}</td>
          <td>
            <div style="font-weight:700;color:#1F3864">${_esc(item.stem || 'Question')}</div>
            ${item.note ? `<div style="font-size:12px;color:#666;margin-top:4px">${_esc(item.note)}</div>` : ''}
            ${item.sectionName ? `<div style="font-size:11px;color:#7a8ca8;margin-top:6px;text-transform:uppercase;letter-spacing:.04em">${_esc(item.sectionName)}</div>` : ''}
          </td>
          <td style="font-size:12px;line-height:1.6">${formatIndexes(item.given)}</td>
          <td style="font-size:12px;line-height:1.6">${formatIndexes(item.expected)}</td>
          <td style="text-align:center">${item.correct ? '<span class="chip chip-pass">Correct</span>' : '<span class="chip chip-fail">Wrong</span>'}</td>
        </tr>`;
    }).join('');

    render(`<div class="admin-wrap">
      <div class="card" style="margin-bottom:16px">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap">
          <div>
            <div style="font-size:22px;font-weight:800;color:#1F3864">Answer Review</div>
            <div style="font-size:13px;color:#666;margin-top:4px">${_esc(resp.label || code)} · ${_esc(result.questionSetName || 'Exam')}</div>
            <div style="font-size:12px;color:#777;margin-top:6px">${result.score ?? '—'} / ${result.total ?? '—'} · ${result.pct == null ? '—' : `${result.pct}%`} · ${result.pass ? 'Passed' : 'Did not pass'}</div>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <button class="btn btn-secondary btn-sm" onclick="showAdmin()">← Back to Admin</button>
          </div>
        </div>
      </div>
      <div class="card">
        <div style="overflow-x:auto">
          <table class="admin-table">
            <thead>
              <tr><th style="text-align:center">#</th><th>Question</th><th>Your Answer</th><th>Correct Answer</th><th style="text-align:center">Result</th></tr>
            </thead>
            <tbody>${rows || '<tr><td colspan="5" style="text-align:center;color:#888;padding:18px">No answer detail available</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    </div>`);
  } catch (_e) {
    modal('❌', 'Review Failed', 'Could not load the answer review for that exam.', [{ label: 'Back to Admin', cls: 'btn-primary', action: () => showAdmin() }]);
  }
}

function auditActionLabel(action) {
  switch (action) {
    case 'admin_login_success': return 'Login success';
    case 'admin_login_failed': return 'Login failed';
    case 'admin_exam_availability_updated': return 'Exam availability changed';
    case 'admin_note_saved': return 'Note saved';
    case 'admin_code_reset': return 'Code reset';
    case 'admin_code_deleted': return 'Code deleted';
    case 'admin_codes_bulk_deleted': return 'Codes bulk deleted';
    case 'admin_codes_generated': return 'Codes generated';
    case 'admin_stale_sessions_cleared': return 'Stale sessions cleared';
    case 'admin_result_summaries_repaired': return 'Scores repaired';
    case 'admin_result_summaries_cleared': return 'Scores cleared';
    case 'admin_code_question_set_assigned': return 'Code exam assigned';
    case 'admin_question_set_created': return 'Exam created';
    case 'admin_question_set_uploaded': return 'Exam uploaded';
    case 'admin_question_set_config_updated': return 'Exam config updated';
    case 'admin_question_set_activated': return 'Exam set active';
    case 'admin_question_set_deleted': return 'Exam deleted';
    case 'admin_question_created': return 'Question created';
    case 'admin_question_updated': return 'Question updated';
    case 'admin_question_deleted': return 'Question deleted';
    case 'admin_section_created': return 'Section created';
    case 'admin_section_updated': return 'Section updated';
    case 'admin_section_deleted': return 'Section deleted';
    default: return action || 'Unknown action';
  }
}

async function showAdmin() {
  S.screen = 'admin';
  document.body.classList.remove('exam-bg');
  render('<div class="admin-wrap"><div style="padding:60px;text-align:center;color:white;font-size:18px">Loading admin data...</div></div>');
  let data;
  let systemStatus;
  let auditData;
  try {
    [data, systemStatus, auditData] = await Promise.all([
      apiJson('/api/admin/codes', {}, { timeoutMs: 12000, retries: 1 }),
      apiJson('/api/admin/system-status', {}, { timeoutMs: 12000, retries: 1 }),
      apiJson('/api/admin/audit?limit=12', {}, { timeoutMs: 12000, retries: 1 })
    ]);
  } catch (_e) {
    data = null;
    systemStatus = null;
    auditData = null;
  }
  if (!data || data.error) {
    modal('❌', 'Error', 'Could not load admin data from the server.', [{ label: 'OK', cls: 'btn-primary' }]);
    return;
  }
  _adminSystemStatus = systemStatus;
  _adminAuditEntries = Array.isArray(auditData?.entries) ? auditData.entries : [];
  _adminRole = data.role || _adminRole || 'admin';
  const canAdmin = _adminRole === 'admin';
  _adminRows = sortAdminRows(data.codes || []);
  _selectedCodes = new Set([..._selectedCodes].filter((code) => _adminRows.some((row) => row.code === code)));
  _adminQuestionSets = Array.isArray(data.questionSets) ? data.questionSets : [];
  _activeQuestionSet = _adminQuestionSets.find((set) => set.isActive) || _adminQuestionSets[0] || null;
  const unused = summaryValue(_adminRows, 'unused');
  const active = summaryValue(_adminRows, 'active');
  const completed = summaryValue(_adminRows, 'completed');
  const warnings = Array.isArray(systemStatus?.warnings) ? systemStatus.warnings : [];
  const staleSessions = Array.isArray(systemStatus?.staleSessions) ? systemStatus.staleSessions : [];
  const examOpen = systemStatus?.examEnabled !== false;
  const systemBanner = systemStatus ? `
    <div class="card" style="margin-bottom:16px;background:${systemStatus.ok ? 'rgba(238,247,242,.98)' : 'rgba(255,245,245,.98)'};border-left:6px solid ${systemStatus.ok ? '#2e7d32' : '#c0392b'}">
      <div style="display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:flex-start">
        <div>
          <div style="font-size:16px;font-weight:800;color:${systemStatus.ok ? '#1f5f2c' : '#9f2d22'}">
            ${systemStatus.ok ? 'System status healthy' : 'System status needs attention'}
          </div>
          <div style="font-size:13px;color:#555;margin-top:4px">
            ${systemStatus.questionCount} questions across ${systemStatus.questionSetCount || 0} exam set${systemStatus.questionSetCount === 1 ? '' : 's'} · ${systemStatus.accessCodeCount} codes · ${systemStatus.activeSessionCount} active sessions · ${systemStatus.resultCount} completed results
          </div>
        </div>
        <div style="font-size:12px;color:#666;text-align:right">
          <div>Version: ${_esc(systemStatus.appVersion || '—')}</div>
          <div>Revision: ${_esc(systemStatus.appRevision || '—')}</div>
          <div>Deployed: ${systemStatus.deployedAt ? _esc(new Date(systemStatus.deployedAt).toLocaleString()) : '—'}</div>
          <div>Schema: ${_esc(systemStatus.schema || '—')}</div>
          <div>Active exam: ${_esc(systemStatus.activeQuestionSet?.name || '—')}</div>
          <div>Exam access: ${examOpen ? 'open' : 'closed'}</div>
          <div>Notes: ${systemStatus.notesEnabled ? 'enabled' : 'missing'}</div>
          <div>Stale sessions: ${systemStatus.staleSessionCount || 0}</div>
          <div>Audit log: ${systemStatus.auditEnabled ? `${systemStatus.auditCount} entries` : 'missing'}</div>
          <div>Admin env: ${systemStatus.adminConfigured ? 'configured' : 'missing'}</div>
          <div>Manager login: ${systemStatus.managerConfigured ? 'configured' : 'not configured'}</div>
        </div>
      </div>
      ${staleSessions.length ? `<div style="margin-top:10px;padding:10px 12px;background:rgba(255,248,230,.9);border-radius:10px;color:#8a5b00;font-size:13px">
        <strong>Stale active sessions (${systemStatus.staleSessionMinutes}+ min):</strong><br>
        ${staleSessions.map((s) => `${_esc(s.code)} · last save ${s.updatedAt ? _esc(new Date(s.updatedAt).toLocaleString()) : 'unknown'}`).join('<br>')}
        ${canAdmin ? '<div style="margin-top:10px"><button class="btn btn-danger btn-sm" onclick="clearStaleSessions()">Clear Stale Sessions</button></div>' : ''}
      </div>` : ''}
      ${warnings.length ? `<div style="margin-top:10px;font-size:13px;color:#7a251d">${warnings.map((w) => `• ${_esc(w)}`).join('<br>')}</div>` : ''}
    </div>` : '';

  const setRows = _adminQuestionSets.map((set) => `
    <tr style="background:${set.isActive ? 'rgba(236,247,239,.9)' : 'white'}">
      <td>
        <strong>${_esc(set.name)}</strong>${set.isActive ? ' <span style="color:#1a5c1a;font-size:11px;font-weight:700">● DEFAULT</span>' : ''}
        ${set.description ? `<div style="font-size:12px;color:#777;margin-top:3px">${_esc(set.description)}</div>` : ''}
      </td>
      <td style="text-align:center">${set.questionCount || 0}</td>
      <td style="text-align:center">${set.numQuestions ? `${set.numQuestions} of ${set.questionCount || 0}` : `All ${set.questionCount || 0}`}</td>
      <td style="text-align:center">${set.durationMinutes || 45}m</td>
      <td style="text-align:center">${set.passPct || 80}%</td>
      <td style="text-align:center">${set.examMode === 'PRACTICE' ? '<span class="chip chip-pass">Practice</span>' : '<span class="chip chip-active">Graded</span>'}</td>
      <td style="text-align:center">${set.proctorEnabled !== false ? 'On' : 'Off'}</td>
      <td style="text-align:center;white-space:nowrap">
        <button class="btn btn-secondary btn-sm" onclick="showQuestionSetAnalytics(${set.id})">Analytics</button>
        ${canAdmin ? `<button class="btn btn-secondary btn-sm" onclick="openQuestionSet(${set.id}, '${_esc(set.name)}')">Manage</button>` : ''}
        ${canAdmin ? `<button class="btn btn-secondary btn-sm" onclick="configQuestionSet(${set.id}, ${set.durationMinutes || 45}, ${set.passPct || 80}, ${set.proctorEnabled !== false}, ${set.numQuestions == null ? 'null' : set.numQuestions}, ${set.questionCount || 0})">Config</button>` : ''}
        ${canAdmin && !set.isActive ? `<button class="btn btn-primary btn-sm" onclick="activateQuestionSet(${set.id})">Set Default</button>` : ''}
        ${canAdmin && !set.isActive ? `<button class="btn btn-danger btn-sm" onclick="deleteQuestionSet(${set.id}, '${_esc(set.name)}')">Delete</button>` : ''}
      </td>
    </tr>`).join('');

  const rows = _adminRows.map((row) => `
    <tr>
      <td style="text-align:center">${canAdmin ? `<input type="checkbox" class="code-select" ${_selectedCodes.has(row.code) ? 'checked' : ''} onchange="toggleCodeSelection('${row.code}', this.checked)">` : ''}</td>
      <td style="font-family:monospace;font-weight:700">${_esc(row.code)}</td>
      <td>${_esc(row.label || '')}</td>
      <td>
        ${row.status === 'unused'
          ? `<select style="margin:0;width:220px;font-size:12px;padding:6px 8px" onchange="assignQuestionSet('${row.code}', this.value)">
              <option value="" ${row.questionSetId == null ? 'selected' : ''}>${_activeQuestionSet ? `${_esc(_activeQuestionSet.name)} (default)` : 'Default active set'}</option>
              ${_adminQuestionSets.map((set) => `<option value="${set.id}" ${row.questionSetId === set.id ? 'selected' : ''}>${_esc(set.name)}${set.isActive ? ' ⭐' : ''}</option>`).join('')}
            </select>`
          : `<span style="font-size:12px;color:#555">${_esc(row.questionSetName || _activeQuestionSet?.name || 'Default active set')}</span>`}
      </td>
      <td><input type="text" value="${_esc(row.notes || '')}" style="margin:0;width:220px;font-size:12px;padding:6px 8px" onblur="saveNote('${row.code}', this.value); this.style.borderColor='#d0d8e8'"></td>
      <td>${statusChip(row)}</td>
      <td style="text-align:center">${row.score == null ? '—' : row.score}</td>
      <td style="text-align:center">${row.pct == null ? '—' : `${row.pct}%`}</td>
      <td style="text-align:center">${row.durationSecs == null ? '—' : durationLabel(row.durationSecs)}</td>
      <td style="text-align:center">${row.tabSwitches || 0}</td>
      <td style="text-align:center">${row.incidentCount ? `<button class="btn btn-secondary btn-sm" onclick="flagsFor('${row.code}')">${row.incidentCount}</button>` : '0'}</td>
      <td style="text-align:center">${row.submittedAt ? new Date(row.submittedAt).toLocaleString() : '—'}</td>
      <td style="text-align:center;white-space:nowrap">
        ${row.status === 'completed' ? `<button class="btn btn-secondary btn-sm" onclick="reviewResult('${row.code}')">Review</button>` : ''}
        ${canAdmin ? `<button class="btn btn-danger btn-sm" onclick="resetCode('${row.code}')">Reset</button>` : ''}
        ${canAdmin ? `<button class="btn btn-danger btn-sm" onclick="deleteCode('${row.code}', '${row.status}')">Delete</button>` : ''}
      </td>
    </tr>`).join('');

  render(`<div class="admin-wrap">
    <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:16px">
      <div>
        <div style="font-size:22px;font-weight:800;color:white">Admin Console</div>
        <div style="font-size:13px;color:rgba(255,255,255,.75)">${unused} unused · ${active} active · ${completed} completed · ${_adminQuestionSets.length} exam set${_adminQuestionSets.length === 1 ? '' : 's'} · Role: ${_adminRole === 'admin' ? 'Admin' : 'Manager'}</div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-primary btn-sm" onclick="generateCodes()">+ Generate Codes</button>
        ${canAdmin ? '<button class="btn btn-primary btn-sm" onclick="createQuestionSet()">+ New Exam Set</button>' : ''}
        ${canAdmin ? '<button class="btn btn-secondary btn-sm" onclick="showUploadQuestionSet()">Upload Exam CSV</button>' : ''}
        ${canAdmin ? `<button class="btn btn-secondary btn-sm" onclick="toggleExamAvailability(${examOpen ? 'false' : 'true'})">${examOpen ? 'Close Exams' : 'Open Exams'}</button>` : ''}
        ${canAdmin ? '<button class="btn btn-secondary btn-sm" onclick="repairResultSummaries()">Repair Scores</button>' : ''}
        ${canAdmin ? '<button class="btn btn-secondary btn-sm" onclick="clearResultSummaries()">Clear Scores</button>' : ''}
        <button class="btn btn-secondary btn-sm" onclick="downloadExport()">Export CSV</button>
        <button class="btn btn-secondary btn-sm" onclick="showAdmin()">↻ Refresh</button>
      </div>
    </div>
    ${systemBanner}
    <div class="card" style="margin-bottom:16px">
      <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:10px">
        <div>
          <div style="font-size:16px;font-weight:800;color:#1F3864">Exam Sets</div>
          <div style="font-size:12px;color:#666">${_activeQuestionSet ? `Default exam: ${_activeQuestionSet.name}` : 'No default exam set configured yet'}</div>
        </div>
        <div style="font-size:12px;color:#666">Manage exams, upload new banks, and assign a set per code.</div>
      </div>
      <div style="overflow-x:auto">
        <table class="admin-table">
          <thead>
            <tr>
              <th>Name</th><th style="text-align:center">Questions</th><th style="text-align:center">Delivered</th><th style="text-align:center">Duration</th><th style="text-align:center">Pass</th><th style="text-align:center">Mode</th><th style="text-align:center">Proctor</th><th style="text-align:center">Actions</th>
            </tr>
          </thead>
          <tbody>${setRows || '<tr><td colspan="8" style="text-align:center;color:#888;padding:18px">No exam sets found</td></tr>'}</tbody>
        </table>
      </div>
    </div>
    <div class="card" style="margin-bottom:16px">
      <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:10px">
        <div style="font-size:16px;font-weight:800;color:#1F3864">Recent Admin Activity</div>
        <div style="font-size:12px;color:#666">${_adminAuditEntries.length ? `${_adminAuditEntries.length} latest events` : 'No audit events yet'}</div>
      </div>
      <div style="overflow-x:auto">
        <table class="admin-table">
          <thead>
            <tr>
              <th>Time</th><th>Action</th><th>Target</th><th>IP</th><th>Details</th>
            </tr>
          </thead>
          <tbody>${
            _adminAuditEntries.length
              ? _adminAuditEntries.map((entry) => `
                <tr>
                  <td style="white-space:nowrap">${entry.createdAt ? _esc(new Date(entry.createdAt).toLocaleString()) : '—'}</td>
                  <td>${_esc(auditActionLabel(entry.action))}</td>
                  <td style="font-family:monospace">${_esc(entry.targetCode || '—')}</td>
                  <td style="font-family:monospace">${_esc(entry.clientIp || '—')}</td>
                  <td>${_esc(entry.details ? JSON.stringify(entry.details) : '—')}</td>
                </tr>`).join('')
              : '<tr><td colspan="5" style="text-align:center;color:#888;padding:16px">No admin audit activity recorded yet</td></tr>'
          }</tbody>
        </table>
      </div>
    </div>
    <div class="card" style="padding:0;overflow-x:auto">
      <div style="padding:14px 16px 0;font-size:12px;color:#666;display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap">
        <span>Sorted by seat number to make the roster easier to scan.</span>
        ${canAdmin ? `<span style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-secondary btn-sm" onclick="selectAllVisibleCodes()">Select Visible</button>
          <button class="btn btn-secondary btn-sm" onclick="clearCodeSelection()">Clear Selection</button>
          <button class="btn btn-danger btn-sm" onclick="bulkDeleteCodes()">Delete Selected (<span id="selected-code-count">${_selectedCodes.size}</span>)</button>
        </span>` : ''}
      </div>
      <table class="admin-table">
        <thead>
          <tr>
            <th style="text-align:center">${canAdmin ? '<input type="checkbox" onchange="toggleAllVisibleCodes(this.checked)">' : ''}</th><th>Code</th><th>Seat</th><th>Exam Set</th><th>Notes</th><th>Status</th><th style="text-align:center">Score</th><th style="text-align:center">Pct</th><th style="text-align:center">Duration</th><th style="text-align:center">Tabs</th><th style="text-align:center">Flags</th><th style="text-align:center">Submitted</th><th style="text-align:center">Actions</th>
          </tr>
        </thead>
        <tbody>${rows || '<tr><td colspan="13" style="text-align:center;color:#888;padding:20px">No access codes found</td></tr>'}</tbody>
      </table>
    </div>
  </div>`);
}

async function assignQuestionSet(code, setIdValue) {
  try {
    await apiJson(`/api/admin/codes/${encodeURIComponent(code)}/question-set`, {
      method: 'POST',
      body: JSON.stringify({ questionSetId: setIdValue === '' ? null : Number(setIdValue) })
    }, { timeoutMs: 10000, retries: 0 });
    const row = _adminRows.find((item) => item.code === code);
    if (row) {
      row.questionSetId = setIdValue === '' ? null : Number(setIdValue);
      const set = _adminQuestionSets.find((item) => item.id === row.questionSetId);
      row.questionSetName = set ? set.name : '';
    }
  } catch (_e) {
    modal('❌', 'Assignment Failed', 'Could not assign that exam set to the access code.', [{ label: 'OK', cls: 'btn-primary', action: () => showAdmin() }]);
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
    modal('❌', 'Analytics Failed', 'Could not load analytics for this exam set.', [{ label: 'Back to Admin', cls: 'btn-primary', action: () => showAdmin() }]);
  }
}

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
  const current = window.__currentQuestionSet || null;
  const returnAction = current && current.id === id
    ? `openQuestionSet(${id}, '${_esc(current.name || '')}')`
    : 'showAdmin()';
  const setMeta = _adminQuestionSets.find((set) => set.id === id) || {};
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
        const current = window.__currentQuestionSet || null;
        if (current && current.id === id) openQuestionSet(id, name);
        else showAdmin();
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
    showAdmin();
  } catch (_e) {
    modal('❌', 'Activation Failed', 'Could not set that exam as the default.', [{ label: 'OK', cls: 'btn-primary' }]);
  }
}

function deleteQuestionSet(id, name) {
  modal('⚠️', 'Delete Exam Set', `Delete "${name}"? This removes its questions and sections permanently.`, [
    { label: 'Delete', cls: 'btn-danger', action: async () => {
      try {
        await apiJson(`/api/admin/question-sets/${id}`, { method: 'DELETE' }, { timeoutMs: 10000, retries: 0 });
        showAdmin();
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
      <input id="upload-name" type="text" placeholder="e.g. ITIL 4 Practice Exam A">
      <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">This is the name admins will see when assigning the exam to candidates.</div>
      <label class="label">Description</label>
      <input id="upload-desc" type="text" placeholder="Optional">
      <div style="font-size:12px;color:#666;margin-top:-6px;margin-bottom:12px">Optional notes like cohort, language, version, or intended audience.</div>
      <label class="label">CSV File</label>
      <input id="upload-file" type="file" accept=".csv" style="width:100%">
      <div style="font-size:12px;color:#666;margin:10px 0 18px;line-height:1.7">
        Upload the CSV exported from your spreadsheet. The template already contains the correct headers, sample rows, and formatting examples.<br>
        Tip: if Excel asks how to save, choose <strong>CSV UTF-8</strong> when available.
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
    const text = await file.text();
    const questions = parseQuestionCsv(text);
    if (!questions.length) throw new Error('The CSV does not contain any questions.');
    const resp = await apiJson('/api/admin/question-sets/upload', {
      method: 'POST',
      body: JSON.stringify({ name, description, questions })
    }, { timeoutMs: 20000, retries: 0 });
    if (!resp || !resp.ok) throw new Error('upload_failed');
    modal('✅', 'Upload Complete', `${resp.count} questions were imported into "${name}".`, [{ label: 'Manage Exam Set', cls: 'btn-primary', action: () => openQuestionSet(resp.questionSetId, name) }]);
  } catch (err) {
    modal('❌', 'Upload Failed', err.message || 'Could not import that CSV file.', [{ label: 'OK', cls: 'btn-primary' }]);
  }
}

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

    window.__currentQuestionSet = {
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
            <div style="font-size:22px;font-weight:800;color:#1F3864">${_esc(window.__currentQuestionSet.name)}</div>
            <div style="font-size:13px;color:#666;margin-top:4px">${_esc(window.__currentQuestionSet.description || 'No description')} ${window.__currentQuestionSet.isActive ? '· Default exam set' : ''}</div>
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
    modal('❌', 'Load Failed', 'Could not load that exam set.', [{ label: 'Back to Admin', cls: 'btn-primary', action: () => showAdmin() }]);
  }
}

function showQuestionEditor(setId, questionId) {
  const current = window.__currentQuestionSet || { questions: [], sections: [] };
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
    openQuestionSet(setId, window.__currentQuestionSet?.name || '');
  } catch (_e) {
    modal('❌', 'Save Failed', 'Could not save that question. Please check the question number, options, and correct indexes.', [{ label: 'OK', cls: 'btn-primary' }]);
  }
}

function deleteQuestion(setId, questionId) {
  modal('⚠️', 'Delete Question', 'Delete this question from the exam set?', [
    { label: 'Delete', cls: 'btn-danger', action: async () => {
      try {
        await apiJson(`/api/admin/question-sets/${setId}/questions/${questionId}`, { method: 'DELETE' }, { timeoutMs: 10000, retries: 0 });
        openQuestionSet(setId, window.__currentQuestionSet?.name || '');
      } catch (_e) {
        modal('❌', 'Delete Failed', 'Could not delete that question.', [{ label: 'OK', cls: 'btn-primary' }]);
      }
    }},
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function editSectionPrompt(setId, sectionId = null) {
  const current = window.__currentQuestionSet || { sections: [] };
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
        openQuestionSet(setId, window.__currentQuestionSet?.name || '');
      } catch (_e) {
        modal('❌', 'Delete Failed', 'Could not delete that section.', [{ label: 'OK', cls: 'btn-primary' }]);
      }
    }},
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function saveNote(code, val) {
  try {
    await apiJson('/api/admin/note', { method: 'POST', body: JSON.stringify({ code, notes: String(val || '').trim() }) }, { timeoutMs: 8000, retries: 0 });
  } catch (_e) {
    // quiet failure; refresh will show truth
  }
}

async function resetCode(code) {
  modal('⚠️', 'Reset Access Code', `Reset code ${code}? This will delete saved progress and any submitted result for that candidate.`, [
    { label: 'Reset Code', cls: 'btn-danger', action: async () => {
      try {
        await apiJson('/api/admin/reset', { method: 'POST', body: JSON.stringify({ code }) }, { timeoutMs: 10000, retries: 0 });
        showAdmin();
      } catch (_e) {
        modal('❌', 'Reset Failed', 'The code could not be reset.', [{ label: 'OK', cls: 'btn-primary' }]);
      }
    }},
    { label: 'Cancel', cls: 'btn-secondary' }
  ]);
}

async function generateCodes() {
  const raw = window.prompt('How many new access codes should be generated?', '10');
  if (!raw) return;
  const count = Number(raw);
  if (!Number.isInteger(count) || count < 1) return;
  try {
    const resp = await apiJson('/api/admin/generate', { method: 'POST', body: JSON.stringify({ count }) }, { timeoutMs: 12000, retries: 0 });
    if (!resp || !resp.ok) throw new Error('generate_failed');
    showAdmin();
  } catch (_e) {
    modal('❌', 'Generate Failed', 'New access codes could not be generated.', [{ label: 'OK', cls: 'btn-primary' }]);
  }
}

async function downloadExport() {
  try {
    const resp = await apiFetch('/api/admin/export.csv', {}, { timeoutMs: 12000, retries: 1 });
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'ITIL4_Exam_Results.csv';
    a.click();
    URL.revokeObjectURL(url);
  } catch (_e) {
    modal('❌', 'Export Failed', 'Could not download the CSV export.', [{ label: 'OK', cls: 'btn-primary' }]);
  }
}

window.handleCodeSubmit = handleCodeSubmit;
window.showCodeEntry = showCodeEntry;
window.handleConsentNext = handleConsentNext;
window.reqWebcam = reqWebcam;
window.reqScreen = reqScreen;
window.startExam = startExam;
window.goToQ = goToQ;
window.prevQ = prevQ;
window.nextQ = nextQ;
window.pick = pick;
window.trySubmit = trySubmit;
window.showAdminLogin = showAdminLogin;
window.doLogin = doLogin;
window.showAdmin = showAdmin;
window.assignQuestionSet = assignQuestionSet;
window.showQuestionSetAnalytics = showQuestionSetAnalytics;
window.createQuestionSet = createQuestionSet;
window.configQuestionSet = configQuestionSet;
window.saveQuestionSetConfig = saveQuestionSetConfig;
window.activateQuestionSet = activateQuestionSet;
window.deleteQuestionSet = deleteQuestionSet;
window.showUploadQuestionSet = showUploadQuestionSet;
window.downloadQuestionTemplate = downloadQuestionTemplate;
window.submitUploadedQuestionSet = submitUploadedQuestionSet;
window.openQuestionSet = openQuestionSet;
window.showQuestionEditor = showQuestionEditor;
window.saveQuestionEditor = saveQuestionEditor;
window.deleteQuestion = deleteQuestion;
window.editSectionPrompt = editSectionPrompt;
window.deleteSection = deleteSection;
window.saveNote = saveNote;
window.resetCode = resetCode;
window.deleteCode = deleteCode;
window.generateCodes = generateCodes;
window.downloadExport = downloadExport;
window.flagsFor = flagsFor;
window.clearStaleSessions = clearStaleSessions;
window.toggleExamAvailability = toggleExamAvailability;
window.reviewResult = reviewResult;
window.repairResultSummaries = repairResultSummaries;
window.clearResultSummaries = clearResultSummaries;
window.toggleCodeSelection = toggleCodeSelection;
window.toggleAllVisibleCodes = toggleAllVisibleCodes;
window.selectAllVisibleCodes = selectAllVisibleCodes;
window.clearCodeSelection = clearCodeSelection;
window.bulkDeleteCodes = bulkDeleteCodes;
window.syncExamModeHelp = syncExamModeHelp;

window.addEventListener('beforeunload', () => {
  if (S.screen === 'exam' && !S.submitted) saveProgress();
});

document.addEventListener('DOMContentLoaded', async () => {
  if (new URLSearchParams(window.location.search).get('admin') === '1') showAdminLogin();
  else showCodeEntry();
});
