/* eslint-disable no-console */
// Shared constants between server.js and client-app.js.
// Loaded as a CommonJS module on the server and as a browser global
// (window.SharedConstants) on the client. Single source of truth for
// brand-name normalization and role/permission definitions.

(function (root, factory) {
  if (typeof module === 'object' && typeof module.exports === 'object') {
    module.exports = factory();
  } else {
    root.SharedConstants = factory();
  }
})(typeof window !== 'undefined' ? window : globalThis, function () {
  // Normalize legacy product names. The app is branded "Academy Exam App"
  // regardless of what an admin typed as a question set name. Specific
  // legacy tokens are mapped to a friendlier display label.
  function normalizeExamTitle(value) {
    var text = String(value == null ? '' : value).trim();
    if (!text) return 'Academy Exam App';
    if (/^ITIL\b/i.test(text)) return 'Academy Exam App';
    if (/\bSAP\s+Basis\s+Exam\b/i.test(text)) return 'Academy Exam Platform';
    if (/\bBasis\s+Exam\b/i.test(text)) return 'Academy Exam Platform';
    return text;
  }

  // Role/permission catalog. Keep this in lockstep with the operator
  // model documented in AGENTS.md. A permission is a colon-delimited
  // token. "*" matches everything for the admin role.
  var ROLE_PERMISSIONS = {
    admin: new Set(['*']),
    manager: new Set([
      'dashboard:read',
      'codes:read',
      'codes:generate',
      'codes:assign',
      'codes:note',
      'results:read',
      'results:export',
      'analytics:read',
      'audit:read',
      'notifications:read',
      'sessions:read'
    ]),
    reviewer: new Set([
      'dashboard:read',
      'codes:read',
      'results:read',
      'results:export',
      'analytics:read',
      'audit:read',
      'audit:export',
      'notifications:read',
      'compliance:read'
    ]),
    content_editor: new Set([
      'dashboard:read',
      'codes:read',
      'analytics:read',
      'notifications:read',
      'content:read',
      'content:write',
      'content:publish',
      'imports:write',
      'imports:rollback',
      'results:export'
    ])
  };

  function hasPermission(role, permission) {
    var set = ROLE_PERMISSIONS[role];
    if (!set) return false;
    return set.has('*') || set.has(permission);
  }

  return {
    normalizeExamTitle: normalizeExamTitle,
    ROLE_PERMISSIONS: ROLE_PERMISSIONS,
    hasPermission: hasPermission
  };
});
