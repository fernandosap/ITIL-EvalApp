/* eslint-disable no-console */
// client/main.js — the SPA entry point. Responsibilities:
//   1. Register global event listeners (offline, online, beforeunload,
//      window.error, unhandledrejection).
//   2. Run the DOMContentLoaded bootstrap inside a try/catch so an
//      unhandled startup error never leaves #app empty.
//   3. Provide a recovery screen (no deps on the rest of the SPA)
//      for bootstrap failures.
//
// Click and change events are handled by client/dispatcher.js via event
// delegation (data-action / data-args attributes in the rendered
// HTML). That means we no longer need the long `window.X = X` re-export
// list that this file used to carry. Modules attach to window.IE.*
// and the dispatcher walks those namespaces to find the right
// function for a given data-action.
//
// Load order (in index.html) is strict:
//   shared/constants.js → client/util.js → client/state.js → client/code-entry.js
//   → client/exam.js → client/proctor.js → client/admin-auth.js
//   → client/admin-codes.js → client/admin-question-sets.js
//   → client/dispatcher.js → client/main.js

(function (root) {
  // ---- Global error capture ----
  // window.onerror / unhandledrejection are the LAST line of
  // defense for runtime errors that escape the SPA's internal
  // try/catch. Without them, an uncaught throw during
  // renderQ/goToQ would leave the screen blank with no
  // diagnostic. We ship a sanitized report and log to the
  // console for the developer.
  root.addEventListener('error', (e) => {
    if (!e) return;
    // Ignore errors from the inline module-load reporter and
    // the recovery screen — those are diagnostic paths, not
    // candidate-facing failures.
    if (e.filename && /__reportModuleLoadError|boot-fallback|recovery/i.test(e.filename)) return;
    try {
      const info = {
        message: e.message || (e.error && e.error.message) || 'window.error',
        filename: e.filename || '',
        line: e.lineno || 0,
        col: e.colno || 0,
        stack: (e.error && e.error.stack) || ''
      };
      if (root.IE && root.IE.util && typeof root.IE.util.reportClientError === 'function') {
        root.IE.util.reportClientError('error', info);
      }
      // eslint-disable-next-line no-console
      console.error('[exam:error]', info);
    } catch (_e) { /* never throw from a reporter */ }
  });

  root.addEventListener('unhandledrejection', (e) => {
    if (!e) return;
    try {
      const reason = e.reason;
      const message = reason && reason.message ? reason.message : (typeof reason === 'string' ? reason : 'unhandledrejection');
      const stack = reason && reason.stack ? reason.stack : '';
      const info = { message: String(message).slice(0, 240), filename: '', line: 0, col: 0, stack };
      if (root.IE && root.IE.util && typeof root.IE.util.reportClientError === 'function') {
        root.IE.util.reportClientError('unhandledrejection', info);
      }
      // eslint-disable-next-line no-console
      console.error('[exam:unhandledrejection]', info);
    } catch (_e) { /* never throw */ }
  });

  // ---- Recovery screen (rendered on bootstrap failure) ----
  // Pure DOM, no deps on the rest of the SPA. If the rest of
  // the SPA is broken enough to cause a bootstrap failure, this
  // still has to work.
  function renderRecoveryScreen(err) {
    try {
      // Hide the boot-fallback first so the two don't
      // visually stack.
      const bf = document.getElementById('boot-fallback');
      if (bf) bf.style.display = 'none';
      const app = document.getElementById('app');
      if (!app) return;
      const safeMessage = (err && err.message) ? String(err.message).slice(0, 240) : 'Unknown startup error';
      // HTML-escape the error message before interpolation.
      // Even though the surrounding <pre> is in a <details>
      // and the page has a CSP, we don't want a stray `<img
      // onerror=...>` in an error message to become an
      // inline event handler.
      const escaped = safeMessage
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
      app.innerHTML =
        '<div class="screen" style="padding-top:48px">' +
          '<div class="card" style="text-align:center">' +
            '<div style="font-size:38px;margin-bottom:8px">⚠️</div>' +
            '<h2 style="margin-bottom:8px">App failed to start</h2>' +
            '<p style="color:#666;margin-bottom:18px">' +
              'Your progress is safe — answers are saved on every interaction. ' +
              'Please reload the page to continue.</p>' +
            '<button class="btn btn-primary" id="recovery-reload">Reload</button>' +
            '<details style="margin-top:14px;text-align:left;color:#888;font-size:12px">' +
              '<summary style="cursor:pointer">Technical details</summary>' +
              '<pre style="margin-top:8px;white-space:pre-wrap;word-break:break-word;font-size:11px">' +
                escaped + '</pre>' +
            '</details>' +
          '</div>' +
        '</div>';
      const reloadBtn = document.getElementById('recovery-reload');
      if (reloadBtn) reloadBtn.addEventListener('click', () => window.location.reload());
    } catch (_e) {
      // Last-resort: just reload.
      try { window.location.reload(); } catch (__e) {}
    }
  }

  // ---- beforeunload: save progress mid-exam ----
  root.addEventListener('beforeunload', (e) => {
    if (root.S && root.S.screen === 'exam' && !root.S.submitted) {
      root.IE.state.saveProgress();
      e.preventDefault();
      e.returnValue = '';
    }
  });

  // ---- online / offline: refresh banner + retry pending submit ----
  function onConnectivityChange() {
    if (root.IE.proctor && typeof root.IE.proctor.refreshConnectivityState === 'function') {
      root.IE.proctor.refreshConnectivityState();
    }
    if (root.IE.state && root.S && root.S.screen === 'submit-pending') {
      const IE = root.IE;
      if (IE.exam && typeof IE.exam.retryPendingSubmission === 'function') {
        IE.exam.retryPendingSubmission();
      }
    }
  }
  root.addEventListener('offline', onConnectivityChange);
  root.addEventListener('online', onConnectivityChange);

  // ---- Bootstrap (wrapped in try/catch) ----
  document.addEventListener('DOMContentLoaded', async () => {
    try {
      const params = new URLSearchParams(window.location.search);
      if (params.get('admin') === '1') {
        // After an XSUAA callback, ?auth=ok is in the URL. Try to bootstrap
        // from the cookie first; fall through to the login form if no
        // session was set.
        if (await root.IE.adminAuth.tryBootstrapFromCookie()) {
          root.IE.admin.showAdmin();
          // Strip ?auth=ok from the URL so a refresh doesn't repeat the
          // bootstrap dance. Use replaceState to avoid polluting history.
          params.delete('auth');
          const newSearch = params.toString();
          window.history.replaceState({}, '', window.location.pathname + (newSearch ? '?' + newSearch : ''));
          return;
        }
        root.IE.adminAuth.showAdminLogin();
      } else {
        root.IE.codeEntry.showCodeEntry();
      }
      // Hide the boot-fallback now that the SPA has rendered
      // its first screen. If a render replaced the entire
      // #app innerHTML (e.g. showCodeEntry), the fallback is
      // already gone — that's fine, this is a no-op then.
      const bf = document.getElementById('boot-fallback');
      if (bf) bf.style.display = 'none';
    } catch (err) {
      // The bootstrap threw. Without this catch, the candidate
      // would see the boot-fallback copy with no recovery
      // path. With it, we report + render a recovery screen.
      try {
        if (root.IE && root.IE.util && typeof root.IE.util.reportClientError === 'function') {
          root.IE.util.reportClientError('bootstrap_failure', {
            message: (err && err.message) ? String(err.message) : 'bootstrap_failure',
            stack: (err && err.stack) ? String(err.stack) : '',
            filename: '',
            line: 0,
            col: 0
          });
        }
      } catch (_e) { /* never throw from a reporter */ }
      // eslint-disable-next-line no-console
      console.error('[exam:bootstrap_failure]', err);
      renderRecoveryScreen(err);
    }
  });
})(typeof window !== 'undefined' ? window : globalThis);
