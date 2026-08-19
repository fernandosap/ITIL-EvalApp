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

  // Canonical role names. Use ROLES.ADMIN etc. in code instead of
  // raw 'admin' literals. ROLE_LIST is the iterable form.
  var ROLES = {
    ADMIN: 'admin',
    MANAGER: 'manager',
    REVIEWER: 'reviewer',
    CONTENT_EDITOR: 'content_editor'
  };
  var ROLE_LIST = [ROLES.ADMIN, ROLES.MANAGER, ROLES.REVIEWER, ROLES.CONTENT_EDITOR];
  function isValidRole(role) { return ROLE_LIST.indexOf(role) !== -1; }

  // Access-code lifecycle. Drives /api/validate, /api/session/start, and
  // every "completed"/"unused" filter in the admin UI.
  var CODE_STATUS = {
    UNUSED: 'unused',
    ACTIVE: 'active',
    COMPLETED: 'completed',
    DELETED: 'deleted'
  };
  var CODE_STATUS_LIST = [CODE_STATUS.UNUSED, CODE_STATUS.ACTIVE, CODE_STATUS.COMPLETED, CODE_STATUS.DELETED];

  // Question-set lifecycle. Question sets move through these as admins
  // edit, publish, archive, and activate them. Activate is the act of
  // pointing exams at this set; that also demotes other sets in the same
  // version group to ARCHIVED.
  var QUESTION_SET_LIFECYCLE = {
    DRAFT: 'DRAFT',
    PUBLISHED: 'PUBLISHED',
    ARCHIVED: 'ARCHIVED'
  };

  // Exam mode on a question set. GRADED counts toward the candidate's
  // pass/fail analytics; PRACTICE is unscored and shows correct answers
  // on review. Both share the same per-session infra; the difference is
  // how results are recorded.
  var EXAM_MODE = {
    GRADED: 'GRADED',
    PRACTICE: 'PRACTICE'
  };

  // Audit-log action names. Keep in lockstep with the strings passed
  // to tryWriteAdminAudit() across server.js. Adding a new one here is
  // a one-line change instead of grepping the file.
  var AUDIT_ACTION = {
    LOGIN_SUCCESS: 'admin_login_success',
    LOGIN_FAILED: 'admin_login_failed',
    LOGOUT: 'admin_logout',
    SESSIONS_REVOKED: 'admin_sessions_revoked',
    CODE_DELETE: 'code_delete',
    CODE_BULK_DELETE: 'code_bulk_delete',
    CODE_GENERATE: 'code_generate',
    CODE_RESET: 'code_reset',
    CODE_NOTE: 'code_note',
    CODE_ASSIGN: 'code_assign',
    EXAM_AVAILABILITY: 'exam_availability',
    CLEAR_STALE_SESSIONS: 'clear_stale_sessions',
    REPAIR_SUMMARIES: 'repair_summaries',
    CLEAR_SUMMARIES: 'clear_summaries',
    QUESTION_SET_CREATE: 'question_set_create',
    QUESTION_SET_CLONE: 'question_set_clone',
    QUESTION_SET_PUBLISH: 'question_set_publish',
    QUESTION_SET_ARCHIVE: 'question_set_archive',
    QUESTION_SET_ACTIVATE: 'question_set_activate',
    QUESTION_SET_CONFIG: 'question_set_config',
    QUESTION_SET_DELETE: 'question_set_delete',
    QUESTION_SET_ROLLBACK_IMPORT: 'question_set_rollback_import',
    QUESTION_SET_UPLOAD: 'question_set_upload',
    QUESTION_SET_QUESTION_CREATE: 'question_set_question_create',
    QUESTION_SET_QUESTION_DELETE: 'question_set_question_delete',
    QUESTION_SET_SECTION_CREATE: 'question_set_section_create',
    QUESTION_SET_SECTION_DELETE: 'question_set_section_delete'
  };

  function hasPermission(role, permission) {
    var set = ROLE_PERMISSIONS[role];
    if (!set) return false;
    return set.has('*') || set.has(permission);
  }

  return {
    normalizeExamTitle: normalizeExamTitle,
    ROLE_PERMISSIONS: ROLE_PERMISSIONS,
    ROLES: ROLES,
    ROLE_LIST: ROLE_LIST,
    isValidRole: isValidRole,
    CODE_STATUS: CODE_STATUS,
    CODE_STATUS_LIST: CODE_STATUS_LIST,
    QUESTION_SET_LIFECYCLE: QUESTION_SET_LIFECYCLE,
    EXAM_MODE: EXAM_MODE,
    AUDIT_ACTION: AUDIT_ACTION,
    hasPermission: hasPermission
  };
});
