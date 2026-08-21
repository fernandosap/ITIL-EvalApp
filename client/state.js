/* eslint-disable no-console */
// client/state.js — global state, persistence, and progress lifecycle.
// Loaded as a UMD module. Defines window.S (the exam-session state),
// window._adminToken, etc. (admin auth state), and the helpers that
// mutate them (saveProgress, queueProgressSave, replayPendingActions,
// fetchStatus). Other modules mutate S.* directly through the shared
// reference.
//
// Public surface (attached to window.IE.state):
//   init, reset, resetExam, getS, getAdmin, setAdminToken,
//   setAdminRole, setAdminAuthMethod,
//   logIncident, isOnline, connectivityBanner, proctorEnabled,
//   saveProgress, queueProgressSave, replayPendingActions, fetchStatus,
//   serializeProgress, buildSubmitPayload,
//   readPendingState, writePendingState, clearPendingState, queuePendingAction

(function (root) {
  // ---- Globals ----
  root.CFG = { webcamIntervalS: 28 };

  // Per-browser-session correlation ID for diagnostic
  // telemetry. This is NOT a credential — it's a random
  // UUID generated once per tab and persisted to
  // sessionStorage. It lets the admin correlate multiple
  // error beacons from the same session without including
  // any exam access code (which IS a credential and must
  // never appear in anonymous telemetry).
  //
  // Generated lazily on first read so we don't pay the
  // crypto cost at boot. sessionStorage is used (not
  // localStorage) so the ID is wiped when the tab closes —
  // it's purely a per-tab correlation handle.
  function getDiagnosticSessionId() {
    if (root.S.diagnosticSessionId) return root.S.diagnosticSessionId;
    let id = null;
    try {
      // crypto.randomUUID is the standard modern way; fall
      // back to a Math.random + Date combo for ancient
      // browsers (none in our supported matrix but defensive).
      if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
        id = crypto.randomUUID();
      } else {
        id = 'dsid-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10);
      }
      if (typeof sessionStorage !== 'undefined') {
        try { sessionStorage.setItem('academy_diag_sid', id); } catch (_e) { /* private mode etc. */ }
      }
    } catch (_e) { id = null; }
    if (!id) id = 'dsid-anon-' + Date.now().toString(36);
    root.S.diagnosticSessionId = id;
    return id;
  }

  root.S = root.S || {
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
    // Set to 'webcam' or 'screen' while an in-exam proctoring stream is
    // disconnected. Exam interactions remain blocked until it is restored.
    proctorRecoveryRequired: null,
    examMode: 'GRADED',
    isPractice: false,
    showCorrectAnswers: false,
    freshStart: false,
    securityBound: false,
    // Populated lazily on first read by getDiagnosticSessionId().
    diagnosticSessionId: null
  };

  // Try to restore a previously-stored diagnostic session
  // ID for this tab. Falls through to lazy generation on
  // first read if the storage was cleared (private mode,
  // cleared cookies, etc.).
  try {
    if (typeof sessionStorage !== 'undefined') {
      const existing = sessionStorage.getItem('academy_diag_sid');
      if (existing && /^[A-Za-z0-9._-]{1,80}$/.test(existing)) {
        root.S.diagnosticSessionId = existing;
      }
    }
  } catch (_e) { /* ignore */ }
  root._adminToken = root._adminToken || null;
  root._adminAuthMethod = root._adminAuthMethod || 'token';
  root._adminRole = root._adminRole || 'admin';
  root._adminSystemStatus = null;
  root._adminAuditEntries = [];
  root._adminQuestionSets = [];
  root._adminNotifications = [];
  root._adminOverview = null;
  root._adminRows = [];
  root._selectedCodes = new Set();
  root._uploadPreview = null;
  root._exportFilters = { questionSetId: '', status: '', mode: '', dateFrom: '', dateTo: '' };
  root._blurTime = null;
  root._progressSaveTimer = null;
  root.SAVE_UI = root.SAVE_UI || { cls: 'save-pill', text: 'Saved' };
  root.API_TIMEOUT_MS = 12000;
  root.API_BASE_RETRY_MS = 600;

  // ---- Helpers ----
  function getS() { return root.S; }
  function getAdmin() {
    return {
      token: root._adminToken,
      role: root._adminRole,
      authMethod: root._adminAuthMethod
    };
  }
  function setAdminToken(v) { root._adminToken = v; }
  function setAdminRole(v) { root._adminRole = v; }
  function setAdminAuthMethod(v) { root._adminAuthMethod = v; }

  function resetExam() {
    // Stop any live MediaStream tracks first. resetExam()
    // is called both on "start over" and on screen exit;
    // we don't want to leave camera/screen share open.
    stopMediaStreams();
    root.S.screen = 'code';
    root.S.code = '';
    root.S.examToken = null;
    root.S.currentQCache = null;
    root.S.answers = [];
    root.S.visited = new Set();
    root.S.currentQ = 0;
    root.S.startTime = null;
    root.S.elapsed = 0;
    root.S.timerInterval = null;
    root.S.webcamInterval = null;
    root.S._proctorInFlight = false;
    root.S.incidents = [];
    root.S.tabSwitches = 0;
    root.S.webcamStream = null;
    root.S.screenStream = null;
    root.S.webcamOk = false;
    root.S.screenOk = false;
    root.S.proctorRecoveryRequired = null;
    root.S.submitted = false;
    root.S.freshStart = false;
  }

  function logIncident(type, detail) {
    root.S.incidents.push({ time: new Date().toLocaleTimeString(), type, detail });
  }
  function isOnline() { return navigator.onLine !== false; }
  function connectivityBanner() {
    if (isOnline()) return '';
    return `<div class="offline-banner" role="status" aria-live="polite">Offline. Answers stay local until connection returns.</div>`;
  }
  function proctorEnabled() { return root.S.proctorOn !== false; }
  function isProctorRecoveryRequired() {
    return proctorEnabled()
      && root.S.screen === 'exam'
      && (!root.S.webcamOk || !root.S.screenOk);
  }

  // ---- Pending state (offline-safe progress + submit) ----
  function pendingKey(code) {
    return `academy_exam_pending_${String(code || '').trim().toUpperCase()}`;
  }
  function readPendingState(code) {
    try {
      return JSON.parse(localStorage.getItem(pendingKey(code)) || 'null') || { progress: null, submit: null };
    } catch (_e) {
      return { progress: null, submit: null };
    }
  }
  function writePendingState(code, state) {
    localStorage.setItem(pendingKey(code), JSON.stringify(state || { progress: null, submit: null }));
  }
  function clearPendingState(code) {
    localStorage.removeItem(pendingKey(code));
  }
  function queuePendingAction(code, type, payload) {
    const state = readPendingState(code);
    if (type === 'progress') state.progress = payload;
    if (type === 'submit') state.submit = payload;
    writePendingState(code, state);
  }

  // ---- Progress lifecycle ----
  function serializeProgress() {
    return {
      answers: root.S.answers,
      visited: Array.from(root.S.visited),
      currentQ: root.S.currentQ,
      incidents: root.S.incidents,
      tabSwitches: root.S.tabSwitches,
      elapsedMs: root.S.elapsed + (root.S.startTime ? Date.now() - root.S.startTime : 0)
    };
  }
  function buildSubmitPayload(autoSubmit) {
    return {
      code: root.S.code,
      answers: root.S.answers,
      durationSecs: Math.round((root.S.elapsed + (root.S.startTime ? (Date.now() - root.S.startTime) : 0)) / 1000),
      tabSwitches: root.S.tabSwitches,
      incidents: root.S.incidents,
      autoSubmit
    };
  }

  async function saveProgress() {
    if (root.S.screen !== 'exam' || root.S.submitted || !root.S.code || !root.S.examToken) return;
    const payload = Object.assign({ code: root.S.code }, serializeProgress());
    if (!isOnline()) {
      queuePendingAction(root.S.code, 'progress', payload);
      root.IE.util.markOfflineSave();
      return;
    }
    try {
      await root.IE.util.apiJson('/api/progress', {
        method: 'POST',
        body: JSON.stringify(payload)
      }, { isSave: true, timeoutMs: 9000, retries: 1 });
      const state = readPendingState(root.S.code);
      state.progress = null;
      writePendingState(root.S.code, state);
    } catch (_e) {
      queuePendingAction(root.S.code, 'progress', payload);
    }
  }

  function queueProgressSave() {
    clearTimeout(root._progressSaveTimer);
    root._progressSaveTimer = setTimeout(() => { saveProgress(); }, 250);
  }

  async function replayPendingActions() {
    if (!root.S.code || !root.S.examToken || !isOnline()) return false;
    const state = readPendingState(root.S.code);
    if (!state.progress && !state.submit) return false;
    if (state.progress) {
      try {
        await root.IE.util.apiJson('/api/progress', {
          method: 'POST',
          body: JSON.stringify(state.progress)
        }, { isSave: true, timeoutMs: 9000, retries: 1 });
        state.progress = null;
        writePendingState(root.S.code, state);
      } catch (_e) {
        root.IE.util.markOfflineSave();
        return false;
      }
    }
    if (state.submit) {
      try {
        const data = await root.IE.util.apiJson('/api/submit', {
          method: 'POST',
          body: JSON.stringify(state.submit)
        }, { timeoutMs: 15000, retries: 1 });
        if (data && data.ok && data.result) {
          clearPendingState(root.S.code);
          // showResultsFromRecord is defined in client/exam.js — late
          // binding via the IE namespace so we don't need a circular
          // require.
          if (root.IE.exam && typeof root.IE.exam.showResultsFromRecord === 'function') {
            root.IE.exam.showResultsFromRecord(data.result);
            return true;
          }
        }
      } catch (_e) {
        root.IE.util.markOfflineSave();
        return false;
      }
    }
    return false;
  }

  async function fetchStatus() {
    try {
      return await root.IE.util.apiJson('/api/status');
    } catch (_e) {
      return null;
    }
  }

  // Stop all live MediaStream tracks (webcam + screen share)
  // and clear the references on the live <video> elements.
  //
  // Why this exists: getUserMedia / getDisplayMedia allocate
  // hardware resources and a black "REC" indicator in some
  // browsers. teardownSecurity() only removes window
  // listeners; it never stops the tracks. Long exam
  // sessions + re-entry could leave multiple tracks alive
  // and burn memory + keep the camera light on.
  //
  // Safe to call multiple times; idempotent. Does NOT throw.
  // Logs the stop so the admin can see if a track was
  // dropped unexpectedly before submit.
  function stopMediaStreams() {
    try {
      const streams = [root.S && root.S.webcamStream, root.S && root.S.screenStream].filter(Boolean);
      let stopped = 0;
      for (const stream of streams) {
        if (!stream || typeof stream.getTracks !== 'function') continue;
        for (const track of stream.getTracks()) {
          try {
            if (typeof track.stop === 'function' && track.readyState !== 'ended') {
              track.stop();
              stopped += 1;
            }
          } catch (_e) { /* one bad track shouldn't block others */ }
        }
      }
      // Clear references so any future drawImage() on
      // hidden-cam doesn't read from a frozen srcObject.
      const hidden = document.getElementById('hidden-cam');
      if (hidden) hidden.srcObject = null;
      const preview = document.getElementById('preview-vid');
      if (preview) preview.srcObject = null;
      if (root.S) {
        if (root.S.webcamStream) { root.S.webcamStream = null; }
        if (root.S.screenStream) { root.S.screenStream = null; }
        root.S.webcamOk = false;
        root.S.screenOk = false;
      }
      if (stopped > 0 && root.S && root.S.screen === 'exam' && !root.S.submitted) {
        // Defensive: we shouldn't reach here mid-exam, but
        // if we do, record it. submitExam() / resetExam() /
        // teardownSecurity() are the normal call sites.
        if (typeof logIncident === 'function') {
          logIncident('streams_stopped_early', `Stopped ${stopped} tracks unexpectedly`);
        }
      }
    } catch (_e) { /* never throw from a cleanup path */ }
  }

  root.IE = root.IE || {};
  root.IE.state = {
    getS: getS,
    getAdmin: getAdmin,
    setAdminToken: setAdminToken,
    setAdminRole: setAdminRole,
    setAdminAuthMethod: setAdminAuthMethod,
    resetExam: resetExam,
    logIncident: logIncident,
    isOnline: isOnline,
    connectivityBanner: connectivityBanner,
    proctorEnabled: proctorEnabled,
    isProctorRecoveryRequired: isProctorRecoveryRequired,
    pendingKey: pendingKey,
    readPendingState: readPendingState,
    writePendingState: writePendingState,
    clearPendingState: clearPendingState,
    queuePendingAction: queuePendingAction,
    serializeProgress: serializeProgress,
    buildSubmitPayload: buildSubmitPayload,
    saveProgress: saveProgress,
    stopMediaStreams: stopMediaStreams,
    queueProgressSave: queueProgressSave,
    replayPendingActions: replayPendingActions,
    fetchStatus: fetchStatus,
    getDiagnosticSessionId: getDiagnosticSessionId
  };

  // CommonJS export so Node-side unit tests can require()
  // the same module. The browser path (UMD/IIFE) doesn't
  // expose `module`, so this line is a no-op there.
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = root.IE.state;
  }
})(typeof window !== 'undefined' ? window : globalThis);
