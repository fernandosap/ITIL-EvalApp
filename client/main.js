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

(function (root) {
  function loadAdminOperationsModule() {
    if (document.querySelector('script[data-admin-live-module]') || root.IE?.adminLive) return;
    const script = document.createElement('script');
    script.src = '/client/admin-live.js';
    script.async = true;
    script.dataset.adminLiveModule = '1';
    script.onerror = () => {
      try { root.IE?.util?.reportClientError?.('module_load', { message: 'Failed to load admin-live.js', filename: '/client/admin-live.js', line: 0, col: 0, stack: '' }); } catch (_e) {}
    };
    document.head.appendChild(script);
  }

  // ---- Global error capture ----
  root.addEventListener('error', (e) => {
    if (!e) return;
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
      console.error('[exam:unhandledrejection]', info);
    } catch (_e) { /* never throw */ }
  });

  // ---- Recovery screen (rendered on bootstrap failure) ----
  function renderRecoveryScreen(err) {
    try {
      const bf = document.getElementById('boot-fallback');
      if (bf) bf.style.display = 'none';
      const app = document.getElementById('app');
      if (!app) return;
      const safeMessage = (err && err.message) ? String(err.message).slice(0, 240) : 'Unknown startup error';
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
        loadAdminOperationsModule();
        if (await root.IE.adminAuth.tryBootstrapFromCookie()) {
          root.IE.admin.showAdmin();
          params.delete('auth');
          const newSearch = params.toString();
          window.history.replaceState({}, '', window.location.pathname + (newSearch ? '?' + newSearch : ''));
          return;
        }
        root.IE.adminAuth.showAdminLogin();
      } else {
        root.IE.codeEntry.showCodeEntry();
      }
      const bf = document.getElementById('boot-fallback');
      if (bf) bf.style.display = 'none';
    } catch (err) {
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
      console.error('[exam:bootstrap_failure]', err);
      renderRecoveryScreen(err);
    }
  });
})(typeof window !== 'undefined' ? window : globalThis);
