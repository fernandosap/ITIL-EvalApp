/* eslint-disable no-console */
// client/main.js — the SPA entry point. Two responsibilities:
//   1. Register global event listeners (offline, online, beforeunload).
//   2. Run the DOMContentLoaded bootstrap.
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
  // ---- Global event listeners ----
  // beforeunload: if the candidate is mid-exam, save progress and
  // warn before the page unloads.
  root.addEventListener('beforeunload', (e) => {
    if (root.S && root.S.screen === 'exam' && !root.S.submitted) {
      root.IE.state.saveProgress();
      e.preventDefault();
      e.returnValue = '';
    }
  });

  // online / offline: refresh the connectivity banner and try to
  // replay any pending submit when we come back online.
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

  // ---- Bootstrap ----
  document.addEventListener('DOMContentLoaded', async () => {
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
  });
})(typeof window !== 'undefined' ? window : globalThis);
