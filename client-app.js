'use strict';

const CFG = {
  webcamIntervalS: 28
};

let _adminToken = null;
let _adminSystemStatus = null;
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
  freshStart: false,
  securityBound: false
};
let _blurTime = null;
let _progressSaveTimer = null;
let _adminRows = [];

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
          <li>Passing score: ${S.passScore}/${S.total} (${S.passPct}%)</li>
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
        <span class="header-title">SAP Academy for Cloud Delivery · Secure Exam</span>
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
        ${isLast && unanswered > 0 ? `<p style="text-align:center;font-size:12px;color:#c55a11;margin-top:8px">⚠ ${unanswered} unanswered — use the dots above to go back.</p>` : ''}
      </div>
    </div>
  </div>
  ${proctorEnabled() ? `<div class="webcam-corner"><video id="exam-cam" autoplay muted playsinline></video><div class="webcam-label">🔴 PROCTORED</div></div>` : ''}`);

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
  render(`<div class="screen" style="max-width:500px">
    <div class="card" style="margin-top:48px;text-align:center">
      <h2 style="text-align:center;margin-bottom:18px">${rec.autoSubmit ? '⏰ Time Expired — Auto-Submitted' : 'Exam Submitted'}</h2>
      <div class="score-circle ${rec.pass ? 'pass' : 'fail'}">
        <div class="score-num">${rec.score}</div>
        <div class="score-den">out of ${rec.total}</div>
      </div>
      <div style="font-size:22px;font-weight:800;color:${rec.pass ? '#1a5c1a' : '#c0392b'};margin-bottom:4px">${rec.pass ? '✓ PASS' : '✗ DID NOT PASS'}</div>
      <div style="font-size:17px;font-weight:700;margin-bottom:4px">${rec.pct}%</div>
      <div style="font-size:13px;color:#888;margin-bottom:22px">Pass threshold: ${S.passScore}/${S.total} (${S.passPct}%)</div>
      <div class="divider"></div>
      <div style="text-align:left;font-size:14px;color:#555;line-height:2.2">
        <div><strong>Access Code:</strong> <span style="font-family:monospace;font-size:16px;font-weight:800;letter-spacing:2px;color:#1F3864">${_esc(rec.code)}</span></div>
        <div><strong>Duration:</strong> ${duration}</div>
        <div><strong>Submitted:</strong> ${new Date(rec.submittedAt).toLocaleString()}</div>
        ${rec.tabSwitches > 0 ? `<div style="color:#c55a11"><strong>Tab switches:</strong> ${rec.tabSwitches}</div>` : ''}
        ${rec.incidentCount > 0 ? `<div style="color:#c55a11"><strong>Flags logged:</strong> ${rec.incidentCount}</div>` : ''}
      </div>
      <div class="divider"></div>
      <p style="font-size:13px;color:#999">Your result has been recorded. You may close this window.</p>
    </div>
  </div>`);
}

function statusChip(row) {
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
  showAdmin();
}

function summaryValue(rows, status) {
  return rows.filter((r) => r.status === status).length;
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

async function probeQuestions() {
  try {
    const resp = await apiJson('/api/admin/question-probe', {}, { timeoutMs: 10000, retries: 0 });
    if (!resp || !resp.ok) throw new Error('probe_failed');
    const body = [
      `Question ${resp.questionIndex + 1} of ${resp.total}`,
      resp.multi ? 'Type: multi-select' : 'Type: single-select',
      `Options: ${resp.optionCount}`,
      `Note present: ${resp.notePresent ? 'yes' : 'no'}`,
      '',
      resp.stemPreview || '(no question text returned)'
    ].join('\n');
    modal('🧪', 'Question Bank Probe', body, [{ label: 'Close', cls: 'btn-primary' }]);
  } catch (_e) {
    modal('❌', 'Probe Failed', 'Could not read a sample question from HANA.', [{ label: 'OK', cls: 'btn-primary' }]);
  }
}

async function showAdmin() {
  S.screen = 'admin';
  document.body.classList.remove('exam-bg');
  render('<div class="admin-wrap"><div style="padding:60px;text-align:center;color:white;font-size:18px">Loading admin data...</div></div>');
  let data;
  let systemStatus;
  try {
    [data, systemStatus] = await Promise.all([
      apiJson('/api/admin/codes', {}, { timeoutMs: 12000, retries: 1 }),
      apiJson('/api/admin/system-status', {}, { timeoutMs: 12000, retries: 1 })
    ]);
  } catch (_e) {
    data = null;
    systemStatus = null;
  }
  if (!data || data.error) {
    modal('❌', 'Error', 'Could not load admin data from the server.', [{ label: 'OK', cls: 'btn-primary' }]);
    return;
  }
  _adminSystemStatus = systemStatus;
  _adminRows = data.codes || [];
  const unused = summaryValue(_adminRows, 'unused');
  const active = summaryValue(_adminRows, 'active');
  const completed = summaryValue(_adminRows, 'completed');
  const warnings = Array.isArray(systemStatus?.warnings) ? systemStatus.warnings : [];
  const systemBanner = systemStatus ? `
    <div class="card" style="margin-bottom:16px;background:${systemStatus.ok ? 'rgba(238,247,242,.98)' : 'rgba(255,245,245,.98)'};border-left:6px solid ${systemStatus.ok ? '#2e7d32' : '#c0392b'}">
      <div style="display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:flex-start">
        <div>
          <div style="font-size:16px;font-weight:800;color:${systemStatus.ok ? '#1f5f2c' : '#9f2d22'}">
            ${systemStatus.ok ? 'System status healthy' : 'System status needs attention'}
          </div>
          <div style="font-size:13px;color:#555;margin-top:4px">
            ${systemStatus.questionCount} questions · ${systemStatus.accessCodeCount} codes · ${systemStatus.activeSessionCount} active sessions · ${systemStatus.resultCount} completed results
          </div>
        </div>
        <div style="font-size:12px;color:#666;text-align:right">
          <div>Version: ${_esc(systemStatus.appVersion || '—')}</div>
          <div>Revision: ${_esc(systemStatus.appRevision || '—')}</div>
          <div>Deployed: ${systemStatus.deployedAt ? _esc(new Date(systemStatus.deployedAt).toLocaleString()) : '—'}</div>
          <div>Schema: ${_esc(systemStatus.schema || '—')}</div>
          <div>Notes: ${systemStatus.notesEnabled ? 'enabled' : 'missing'}</div>
          <div>Admin env: ${systemStatus.adminConfigured ? 'configured' : 'missing'}</div>
        </div>
      </div>
      ${warnings.length ? `<div style="margin-top:10px;font-size:13px;color:#7a251d">${warnings.map((w) => `• ${_esc(w)}`).join('<br>')}</div>` : ''}
    </div>` : '';

  const rows = _adminRows.map((row) => `
    <tr>
      <td style="font-family:monospace;font-weight:700">${_esc(row.code)}</td>
      <td>${_esc(row.label || '')}</td>
      <td><input type="text" value="${_esc(row.notes || '')}" style="margin:0;width:220px;font-size:12px;padding:6px 8px" onblur="saveNote('${row.code}', this.value); this.style.borderColor='#d0d8e8'"></td>
      <td>${statusChip(row)}</td>
      <td style="text-align:center">${row.score == null ? '—' : row.score}</td>
      <td style="text-align:center">${row.pct == null ? '—' : `${row.pct}%`}</td>
      <td style="text-align:center">${row.durationSecs == null ? '—' : durationLabel(row.durationSecs)}</td>
      <td style="text-align:center">${row.tabSwitches || 0}</td>
      <td style="text-align:center">${row.incidentCount ? `<button class="btn btn-secondary btn-sm" onclick="flagsFor('${row.code}')">${row.incidentCount}</button>` : '0'}</td>
      <td style="text-align:center">${row.submittedAt ? new Date(row.submittedAt).toLocaleString() : '—'}</td>
      <td style="text-align:center"><button class="btn btn-danger btn-sm" onclick="resetCode('${row.code}')">Reset</button></td>
    </tr>`).join('');

  render(`<div class="admin-wrap">
    <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:16px">
      <div>
        <div style="font-size:22px;font-weight:800;color:white">Admin Console</div>
        <div style="font-size:13px;color:rgba(255,255,255,.75)">${unused} unused · ${active} active · ${completed} completed</div>
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-primary btn-sm" onclick="generateCodes()">+ Generate Codes</button>
        <button class="btn btn-secondary btn-sm" onclick="probeQuestions()">🧪 Probe Questions</button>
        <button class="btn btn-secondary btn-sm" onclick="downloadExport()">⬇ Export CSV</button>
        <button class="btn btn-secondary btn-sm" onclick="showAdmin()">↻ Refresh</button>
      </div>
    </div>
    ${systemBanner}
    <div class="card" style="padding:0;overflow-x:auto">
      <table class="admin-table">
        <thead>
          <tr>
            <th>Code</th><th>Seat</th><th>Notes</th><th>Status</th><th style="text-align:center">Score</th><th style="text-align:center">Pct</th><th style="text-align:center">Duration</th><th style="text-align:center">Tabs</th><th style="text-align:center">Flags</th><th style="text-align:center">Submitted</th><th style="text-align:center">Action</th>
          </tr>
        </thead>
        <tbody>${rows || '<tr><td colspan="11" style="text-align:center;color:#888;padding:20px">No access codes found</td></tr>'}</tbody>
      </table>
    </div>
  </div>`);
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
window.saveNote = saveNote;
window.resetCode = resetCode;
window.generateCodes = generateCodes;
window.downloadExport = downloadExport;
window.flagsFor = flagsFor;
window.probeQuestions = probeQuestions;

window.addEventListener('beforeunload', () => {
  if (S.screen === 'exam' && !S.submitted) saveProgress();
});

document.addEventListener('DOMContentLoaded', async () => {
  if (new URLSearchParams(window.location.search).get('admin') === '1') showAdminLogin();
  else showCodeEntry();
});
