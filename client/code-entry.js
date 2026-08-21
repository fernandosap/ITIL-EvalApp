/* eslint-disable no-console */
// client/code-entry.js — the candidate-facing pre-exam flow: access
// code entry, server validation, consent screen, tech check (webcam +
// screen share), and the startExam call that hands off to exam.js.
//
// Public surface (attached to window.IE.codeEntry):
//   showCodeEntry, handleCodeSubmit, startFresh, showConsent,
//   handleConsentNext, proceedToTechCheck, showTechCheck,
//   reqWebcam, reqScreen, startExam

(function (root) {
  const { $, render, modal, brandLockup, apiJson } = root.IE.util;
  const S = root.S;

  // Build the "session secure" banner shown on the candidate landing.
  // Surfaces the exam duration / pass mark so the candidate knows what
  // they're walking into, and a last-refreshed marker so they know the
  // server is still talking to them. Refreshed every time showCodeEntry
  // runs (i.e. on every /api/status call).
  function buildSessionBanner(status) {
    if (!status) return '';
    const duration = Number(status.durationSecs || 0);
    const total = Number(status.total || 0);
    const passPct = Number(status.passPct || 0);
    const passScore = Number(status.passScore || 0);
    const mins = Math.round(duration / 60);
    const items = [];
    if (total) items.push(`<strong>${total}</strong> questions`);
    if (mins) items.push(`<strong>${mins}</strong> min`);
    if (passScore) items.push(`<strong>${passScore}/${total || '?'}</strong> to pass (${passPct}%)`);
    if (!items.length) return '';
    return `<div class="session-banner" role="status" aria-live="polite">
      <span class="session-banner__dot" aria-hidden="true"></span>
      <span class="session-banner__label">Session secure</span>
      <span class="session-banner__detail">${items.join(' &middot; ')}</span>
    </div>`;
  }

  async function showCodeEntry() {
    S.screen = 'code';
    S.examToken = null;
    document.body.classList.remove('exam-bg');
    const status = await root.IE.state.fetchStatus();
    const statusLoaded = Boolean(status && !status.error);
    const examName = root.IE.util.normalizeExamTitle(status?.examName || 'Academy Exam App');
    const examActive = statusLoaded && status?.examActive !== false;
    const sessionBanner = statusLoaded ? buildSessionBanner(status) : '';

    const logoBlock = brandLockup(
      examName,
      !statusLoaded
        ? 'Service connection unavailable'
        : examActive
          ? 'Secure exam delivery and practice flow'
          : 'Exam access temporarily paused'
    );

    if (!statusLoaded) {
      render(`<div class="screen" style="max-width:480px">${logoBlock}
        ${root.IE.state.connectivityBanner()}
        <div class="glass-card" style="text-align:center">
          <div style="font-size:48px;margin-bottom:12px">⚠️</div>
          <h2 style="margin-bottom:8px">Service Unavailable</h2>
          <p style="color:#666;font-size:14px;margin-bottom:8px">The exam service could not be reached or is not fully configured.</p>
          <p style="color:#999;font-size:13px">Please try again shortly or contact your proctor or administrator.</p>
        </div>
      </div>`);
      return;
    }

    if (!examActive) {
      render(`<div class="screen" style="max-width:480px">${logoBlock}
        ${root.IE.state.connectivityBanner()}
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
      ${sessionBanner}
      ${root.IE.state.connectivityBanner()}
      <div class="glass-card">
        <h2>Enter Your Access Code</h2>
        <p style="color:#666;font-size:14px;margin-bottom:20px">Enter the 6-character code provided to you by your proctor. The code is case-insensitive.</p>
        <input class="code-input" id="code-inp" type="text" maxlength="6"
          placeholder="• • • • • •" autocomplete="off" autocorrect="off" spellcheck="false"
          oninput="this.value=this.value.toUpperCase().replace(/[^A-Z0-9]/g,'')"
          data-enter-action="handleCodeSubmit">
        <button class="btn btn-primary btn-full" style="margin-top:6px" data-action="handleCodeSubmit">Continue →</button>
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
      root.IE.state.clearPendingState(raw);
      if (root.IE.exam && typeof root.IE.exam.showResultsFromRecord === 'function') {
        root.IE.exam.showResultsFromRecord(data.result);
      }
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
        ${root.IE.state.proctorEnabled() ? `
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
          <label for="cb-consent">I understand and agree to the exam conditions${root.IE.state.proctorEnabled() ? ' and proctoring' : ''} described above</label>
        </div>
        <div style="margin-top:18px">
          <button class="btn btn-primary btn-full" data-action="handleConsentNext">Continue →</button>
          <button class="btn btn-secondary btn-full" data-action="showCodeEntry">← Back</button>
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
    if (!root.IE.state.proctorEnabled()) {
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
          <button class="btn btn-primary btn-full" id="btn-cam" data-action="reqWebcam">Enable Webcam</button>
          <button class="btn btn-secondary btn-full" id="btn-screen" data-action="reqScreen" disabled>Share Screen</button>
          <button class="btn btn-success btn-full" id="btn-start" data-action="startExam" disabled>${isResume ? 'Resume Exam' : 'Start Exam'} →</button>
          <button class="btn btn-secondary btn-full" data-action="showConsent">← Back</button>
        </div>
      </div>
    </div>`);
  }

  // ---- Media acquisition (separate from Tech Check UI) ----
  // These two functions are the only place that calls
  // getUserMedia / getDisplayMedia. They manage S.webcamStream
  // / S.screenStream and the hidden-cam / exam-cam bindings,
  // and they register the `ended` handler on the video track.
  //
  // The Tech Check screen has its own buttons + status pills
  // (st-cam, btn-cam, st-screen, btn-screen, cam-preview,
  // preview-vid). Those live ONLY on the Tech Check view and
  // do not exist on the exam view. So the Tech Check wrapper
  // (reqWebcam / reqScreen) is a thin adapter that:
  //   1. calls the pure media acquire function
  //   2. updates the Tech Check DOM
  // The in-exam reconnect path (called from the `ended`
  // handler below) calls the pure acquire function only —
  // it never touches the Tech Check DOM, which doesn't exist
  // during an exam.
  //
  // This split is what makes mid-exam reconnect safe:
  // before this change, the `ended` handler tried to update
  // st-cam / btn-cam from inside the exam view and threw
  // TypeError: Cannot read properties of null.

  // Acquire (or re-acquire) the webcam. Returns the new
  // MediaStream. On failure, throws — the caller decides
  // whether to show a modal.
  async function acquireWebcam() {
    const stream = await navigator.mediaDevices.getUserMedia({ video: { width: 320, height: 240 }, audio: false });
    S.webcamStream = stream;
    S.webcamOk = true;
    // hidden-cam is the offscreen <video> the proctor reads
    // from with drawImage(). exam-cam is the small corner
    // feed shown to the candidate. Both must point at the
    // new stream; without the exam-cam rebind, the corner
    // feed would stay frozen on the last frame.
    const hidden = $('hidden-cam');
    if (hidden) hidden.srcObject = stream;
    const examCam = $('exam-cam');
    if (examCam) examCam.srcObject = stream;
    // Register the ended handler on the new video track.
    // The previous stream's track is already ended; we only
    // care about this one.
    const videoTrack = stream.getVideoTracks()[0];
    if (videoTrack) {
      videoTrack.addEventListener('ended', () => onWebcamTrackEnded());
    }
    return stream;
  }

  // Called when the active webcam MediaStreamTrack ends.
  // The track ends for one of: OS suspend, USB unplug,
  // user revokes camera permission, browser auto-stops
  // the track after long backgrounding. We don't care
  // which — the recovery is the same.
  function onWebcamTrackEnded() {
    S.webcamOk = false;
    // Mark the live preview as stale. exam-cam is the
    // candidate-facing corner feed; it would otherwise
    // freeze on the last frame and look like proctoring
    // was still active. Same for hidden-cam so drawImage()
    // returns null and the proctor's no-videoWidth guard
    // skips the capture.
    const examCam = $('exam-cam');
    if (examCam) examCam.srcObject = null;
    const hidden = $('hidden-cam');
    if (hidden) hidden.srcObject = null;
    if (S.screen !== 'exam' || S.submitted) {
      // On the Tech Check screen we don't show a modal;
      // we just flip the status pill so the next reqWebcam
      // can succeed. (The Tech Check view re-renders the
      // pill when the user goes back to it.)
      const st = $('st-cam');
      if (st) st.innerHTML = '<span class="status-err">Disconnected</span>';
      return;
    }
    // Mid-exam: log + recovery modal. The Reconnect button
    // is the user-gesture that satisfies getUserMedia's
    // permission requirement.
    if (root.IE && root.IE.state && typeof root.IE.state.logIncident === 'function') {
      root.IE.state.logIncident('webcam_disconnected', 'Webcam track ended during exam');
    }
    beginProctorRecovery('webcam', acquireWebcam);
  }

  // Acquire (or re-acquire) the screen share. Returns the
  // new MediaStream. On failure, throws.
  async function acquireScreen() {
    const stream = await navigator.mediaDevices.getDisplayMedia({ video: { displaySurface: 'monitor' }, audio: false });
    S.screenStream = stream;
    S.screenOk = true;
    // No hidden-cam rebind for screen — the proctor only
    // captures from the webcam. Screen sharing is for the
    // proctor's record-keeping (so an admin reviewing the
    // session can see what was on the candidate's screen
    // during a flagged incident), not for AI analysis.
    const track = stream.getVideoTracks()[0];
    if (track) {
      track.addEventListener('ended', () => onScreenTrackEnded());
    }
    return stream;
  }

  // Called when the active screen-share track ends. The
  // previous version only showed a "I understand" modal
  // that let the candidate continue without proctoring —
  // unacceptable, because screen sharing is a proctoring
  // requirement, not a courtesy. We now mark S.screenOk
  // = false, log the incident, and force a Reconnect
  // action before the candidate can proceed.
  function onScreenTrackEnded() {
    S.screenOk = false;
    if (S.screen !== 'exam' || S.submitted) {
      // Tech Check path: flip the status pill. The Tech
      // Check view re-renders it when the user goes back.
      const st = $('st-screen');
      if (st) st.innerHTML = '<span class="status-err">Disconnected</span>';
      return;
    }
    if (root.IE && root.IE.state && typeof root.IE.state.logIncident === 'function') {
      root.IE.state.logIncident('screen_stopped', 'Screen share track ended during exam');
    }
    beginProctorRecovery('screen', acquireScreen);
  }

  function beginProctorRecovery(kind, acquire) {
    const label = kind === 'webcam' ? 'Webcam' : 'Screen Share';
    const disconnectedTitle = kind === 'webcam' ? 'Webcam Disconnected' : 'Screen Share Stopped';
    const disconnectedMessage = kind === 'webcam'
      ? 'Your webcam was disconnected mid-exam. This has been logged. Please reconnect immediately to continue proctoring.'
      : 'You have stopped screen sharing. This has been logged. Screen sharing is required throughout the exam — please reconnect immediately.';
    S.proctorRecoveryRequired = kind;

    const retry = () => {
      Promise.resolve(acquire()).then(() => {
        if (S.proctorRecoveryRequired === kind) S.proctorRecoveryRequired = null;
        if (root.IE && root.IE.state && typeof root.IE.state.logIncident === 'function') {
          root.IE.state.logIncident(`${kind}_reconnected`, `${label} reconnected during exam`);
        }
      }).catch((err) => {
        if (root.IE && root.IE.state && typeof root.IE.state.logIncident === 'function') {
          root.IE.state.logIncident(`${kind}_reconnect_failed`, String(err && err.message ? err.message : err));
        }
        modal('🚨', `${label} Reconnect Failed`, `The browser did not grant ${kind === 'webcam' ? 'camera' : 'screen-sharing'} access. Please allow access and try again.`, [
          { label: 'Retry', cls: 'btn-danger', action: retry }
        ]);
      });
    };

    modal('🚨', disconnectedTitle, disconnectedMessage, [
      { label: `Reconnect ${kind === 'webcam' ? 'Webcam' : 'Screen'}`, cls: 'btn-danger', action: retry }
    ]);
  }

  // Tech Check wrappers. These combine the pure media
  // acquisition with the Tech Check DOM updates. They are
  // ONLY meant to be called from the Tech Check view —
  // never from inside the exam.
  async function reqWebcam() {
    try {
      await acquireWebcam();
      // Tech Check UI updates — only safe because we're on
      // the Tech Check view where st-cam / btn-cam exist.
      $('st-cam').innerHTML = '<span class="status-ok">✓ Active</span>';
      $('btn-cam').textContent = '✓ Webcam Active';
      $('btn-cam').disabled = true;
      $('btn-screen').disabled = false;
      const pv = $('preview-vid');
      if (pv) pv.srcObject = S.webcamStream;
      $('cam-preview').style.display = 'block';
    } catch (_e) {
      $('st-cam').innerHTML = '<span class="status-err">Denied</span>';
      modal('❌', 'Webcam Required', 'Please allow camera access in your browser settings and try again.', [{ label: 'Retry', cls: 'btn-primary', action: reqWebcam }]);
    }
  }

  async function reqScreen() {
    try {
      await acquireScreen();
      // Tech Check UI updates — only safe because we're on
      // the Tech Check view where st-screen / btn-screen /
      // btn-start exist.
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
    if (root.IE.state.proctorEnabled() && (!S.webcamOk || !S.screenOk)) {
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
    if (root.IE.proctor && typeof root.IE.proctor.setupSecurity === 'function') {
      root.IE.proctor.setupSecurity();
    }
    if (root.IE.exam && typeof root.IE.exam.startTimer === 'function') {
      root.IE.exam.startTimer();
    }
    if (root.IE.proctor && typeof root.IE.proctor.startProctor === 'function') {
      root.IE.proctor.startProctor();
    }
    if (await root.IE.state.replayPendingActions()) return;
    if (root.IE.exam && typeof root.IE.exam.renderQ === 'function') {
      await root.IE.exam.renderQ();
    }
  }

  root.IE = root.IE || {};
  root.IE.codeEntry = {
    showCodeEntry: showCodeEntry,
    handleCodeSubmit: handleCodeSubmit,
    startFresh: startFresh,
    showConsent: showConsent,
    handleConsentNext: handleConsentNext,
    proceedToTechCheck: proceedToTechCheck,
    showTechCheck: showTechCheck,
    reqWebcam: reqWebcam,
    reqScreen: reqScreen,
    startExam: startExam,
    // Exposed for unit tests; not part of the SPA's public API.
    __test__: {
      buildSessionBanner: buildSessionBanner,
      onWebcamTrackEnded: onWebcamTrackEnded,
      onScreenTrackEnded: onScreenTrackEnded
    }
  };
})(typeof window !== 'undefined' ? window : globalThis);
