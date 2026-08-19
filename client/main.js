/* eslint-disable no-console */
// client/main.js — the SPA entry point. Two responsibilities:
//   1. Re-expose every function used by inline onclick="..." handlers
//      in the rendered HTML onto window.*. (Modules attach to
//      window.IE.<name>; onclick handlers need plain window.X.)
//   2. Register global event listeners (offline, online, beforeunload)
//      and run the DOMContentLoaded bootstrap.
//
// Load order (in index.html) is strict:
//   shared/constants.js → client/util.js → client/state.js → client/code-entry.js
//   → client/exam.js → client/proctor.js → client/admin-auth.js
//   → client/admin-codes.js → client/admin-question-sets.js
//   → client/main.js

(function (root) {
  // ---- window.* re-exports for inline onclick handlers ----
  // Keep this list in sync with the inline onclick attributes in the
  // rendered HTML across all modules. Anything not on window will
  // throw a ReferenceError when the user clicks the corresponding
  // button.
  const ONCLICK_EXPORTS = [
    // code-entry
    ['handleCodeSubmit', 'codeEntry'],
    ['showCodeEntry', 'codeEntry'],
    ['handleConsentNext', 'codeEntry'],
    ['reqWebcam', 'codeEntry'],
    ['reqScreen', 'codeEntry'],
    ['startExam', 'codeEntry'],
    // exam
    ['goToQ', 'exam'],
    ['prevQ', 'exam'],
    ['nextQ', 'exam'],
    ['pick', 'exam'],
    ['trySubmit', 'exam'],
    ['retryPendingSubmission', 'exam'],
    ['downloadResultSummary', 'exam'],
    // admin-auth
    ['showAdminLogin', 'adminAuth'],
    ['doLogin', 'adminAuth'],
    ['logoutAdmin', 'adminAuth'],
    ['revokeAdminSessions', 'adminAuth'],
    // admin-codes (dashboard + per-row handlers)
    ['showAdmin', 'admin'],
    ['flagsFor', 'admin'],
    ['clearStaleSessions', 'admin'],
    ['toggleExamAvailability', 'admin'],
    ['reviewResult', 'admin'],
    ['repairResultSummaries', 'admin'],
    ['clearResultSummaries', 'admin'],
    ['toggleCodeSelection', 'admin'],
    ['toggleAllVisibleCodes', 'admin'],
    ['selectAllVisibleCodes', 'admin'],
    ['clearCodeSelection', 'admin'],
    ['bulkDeleteCodes', 'admin'],
    ['saveNote', 'admin'],
    ['resetCode', 'admin'],
    ['deleteCode', 'admin'],
    ['generateCodes', 'admin'],
    ['setExportFilter', 'admin'],
    ['downloadExport', 'admin'],
    ['downloadAuditExport', 'admin'],
    ['downloadSignedResultSummary', 'admin'],
    // question-sets
    ['assignQuestionSet', 'questionSets'],
    ['showQuestionSetAnalytics', 'questionSets'],
    ['createQuestionSet', 'questionSets'],
    ['configQuestionSet', 'questionSets'],
    ['saveQuestionSetConfig', 'questionSets'],
    ['syncExamModeHelp', 'questionSets'],
    ['activateQuestionSet', 'questionSets'],
    ['deleteQuestionSet', 'questionSets'],
    ['showUploadQuestionSet', 'questionSets'],
    ['downloadQuestionTemplate', 'questionSets'],
    ['previewUploadedQuestionSet', 'questionSets'],
    ['submitUploadedQuestionSet', 'questionSets'],
    ['openQuestionSet', 'questionSets'],
    ['showQuestionEditor', 'questionSets'],
    ['saveQuestionEditor', 'questionSets'],
    ['deleteQuestion', 'questionSets'],
    ['editSectionPrompt', 'questionSets'],
    ['deleteSection', 'questionSets'],
    ['exportQuestionSet', 'questionSets'],
    ['cloneQuestionSet', 'questionSets'],
    ['publishQuestionSet', 'questionSets'],
    ['archiveQuestionSet', 'questionSets'],
    ['rollbackImportedSet', 'questionSets']
  ];

  for (const [name, mod] of ONCLICK_EXPORTS) {
    const fn = root.IE && root.IE[mod] && root.IE[mod][name];
    if (typeof fn === 'function') {
      root[name] = fn;
    } else {
      console.warn(`[client/main] onclick export missing: ${name} on IE.${mod}`);
    }
  }

  // ---- Global event listeners ----
  root.addEventListener('beforeunload', (e) => {
    if (root.S && root.S.screen === 'exam' && !root.S.submitted) {
      root.IE.state.saveProgress();
      e.preventDefault();
      e.returnValue = '';
    }
  });
  root.addEventListener('offline', () => {
    if (root.IE.proctor && typeof root.IE.proctor.refreshConnectivityState === 'function') {
      root.IE.proctor.refreshConnectivityState();
    }
  });
  root.addEventListener('online', () => {
    if (root.IE.proctor && typeof root.IE.proctor.refreshConnectivityState === 'function') {
      root.IE.proctor.refreshConnectivityState();
    }
  });

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
