/* eslint-disable no-console */
// client/proctor.js — proctoring helpers: focus/visibility tracking
// and the periodic webcam capture that goes through Anthropic for
// AI proctoring. These are only active while the user is in the
// 'exam' screen.
//
// Public surface (attached to window.IE.proctor):
//   setupSecurity, teardownSecurity, onBlur, onFocus, onVisChange,
//   startProctor, proctor, refreshConnectivityState

(function (root) {
  const { $, apiJson } = root.IE.util;
  const S = root.S;
  const { logIncident, queueProgressSave, isOnline, proctorEnabled } = root.IE.state;

  // ---- Focus / visibility tracking ----
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
    root._blurTime = Date.now();
    logIncident('focus_lost', 'Window lost focus');
    queueProgressSave();
  }
  function onFocus() {
    if (S.screen !== 'exam' || S.submitted || !root._blurTime) return;
    const awayMs = Date.now() - root._blurTime;
    const secs = Math.round(awayMs / 1000);
    root._blurTime = null;
    if (awayMs > 0 && awayMs < 450) logIncident('possible_screenshot', `Brief focus loss: ${awayMs}ms`);
    if (secs >= 2) {
      logIncident('focus_returned', `Away ${secs}s`);
      root.IE.util.modal('⚠️', 'Browser Window Lost Focus', `Your exam window was inactive for ${secs} second${secs !== 1 ? 's' : ''}. Please keep this window open and in the foreground at all times.`, [{ label: 'Return to Exam', cls: 'btn-primary' }]);
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
        root.IE.util.modal('🚨', 'Tab Switch Detected', `Tab or window switch #${S.tabSwitches} has been detected and logged. Please remain in the exam window.`, [{ label: 'Return to Exam', cls: S.tabSwitches >= 3 ? 'btn-danger' : 'btn-primary' }]);
      }
    }, 350);
  }

  // ---- Webcam capture + AI proctoring ----
  function startProctor() {
    if (!proctorEnabled()) return;
    clearInterval(S.webcamInterval);
    S.webcamInterval = setInterval(proctor, (root.CFG.webcamIntervalS || 28) * 1000);
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
        root.IE.util.modal('🚨', 'Proctoring Alert', `An automated proctoring check flagged this concern:\n\n"${d.reason || 'Suspicious behaviour detected'}"\n\nThis event has been logged for review.`, [{ label: 'I understand', cls: 'btn-danger' }]);
      }
    } catch (_e) {
      // soft fail
    }
  }

  // ---- Connectivity ----
  function refreshConnectivityState() {
    const banner = document.querySelector('.offline-banner');
    if (!isOnline() && !banner) {
      const div = document.createElement('div');
      div.className = 'offline-banner';
      div.setAttribute('role', 'status');
      div.setAttribute('aria-live', 'polite');
      div.textContent = 'Offline. Answers stay local until connection returns.';
      document.body.prepend(div);
    } else if (isOnline() && banner) {
      banner.remove();
    }
  }

  root.IE = root.IE || {};
  root.IE.proctor = {
    setupSecurity: setupSecurity,
    teardownSecurity: teardownSecurity,
    onBlur: onBlur,
    onFocus: onFocus,
    onVisChange: onVisChange,
    startProctor: startProctor,
    proctor: proctor,
    refreshConnectivityState: refreshConnectivityState
  };
})(typeof window !== 'undefined' ? window : globalThis);
