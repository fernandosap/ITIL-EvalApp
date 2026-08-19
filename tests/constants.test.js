'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeExamTitle,
  ROLES,
  ROLE_LIST,
  isValidRole,
  CODE_STATUS,
  CODE_STATUS_LIST,
  QUESTION_SET_LIFECYCLE,
  EXAM_MODE,
  AUDIT_ACTION,
  hasPermission,
  ROLE_PERMISSIONS
} = require('../shared/constants.js');

test('ROLES: exposes the four canonical names', () => {
  assert.equal(ROLES.ADMIN, 'admin');
  assert.equal(ROLES.MANAGER, 'manager');
  assert.equal(ROLES.REVIEWER, 'reviewer');
  assert.equal(ROLES.CONTENT_EDITOR, 'content_editor');
});

test('ROLE_LIST: contains exactly the four roles in priority order', () => {
  assert.deepEqual(ROLE_LIST, ['admin', 'manager', 'reviewer', 'content_editor']);
});

test('isValidRole: accepts valid roles, rejects anything else', () => {
  assert.equal(isValidRole('admin'), true);
  assert.equal(isValidRole('manager'), true);
  assert.equal(isValidRole('reviewer'), true);
  assert.equal(isValidRole('content_editor'), true);
  assert.equal(isValidRole('superadmin'), false);
  assert.equal(isValidRole(''), false);
  assert.equal(isValidRole(null), false);
  assert.equal(isValidRole(undefined), false);
});

test('CODE_STATUS: exposes the four lifecycle values', () => {
  assert.equal(CODE_STATUS.UNUSED, 'unused');
  assert.equal(CODE_STATUS.ACTIVE, 'active');
  assert.equal(CODE_STATUS.COMPLETED, 'completed');
  assert.equal(CODE_STATUS.DELETED, 'deleted');
});

test('CODE_STATUS_LIST: contains exactly the four statuses', () => {
  assert.deepEqual(CODE_STATUS_LIST, ['unused', 'active', 'completed', 'deleted']);
});

test('QUESTION_SET_LIFECYCLE: DRAFT / PUBLISHED / ARCHIVED', () => {
  assert.equal(QUESTION_SET_LIFECYCLE.DRAFT, 'DRAFT');
  assert.equal(QUESTION_SET_LIFECYCLE.PUBLISHED, 'PUBLISHED');
  assert.equal(QUESTION_SET_LIFECYCLE.ARCHIVED, 'ARCHIVED');
});

test('EXAM_MODE: GRADED / PRACTICE', () => {
  assert.equal(EXAM_MODE.GRADED, 'GRADED');
  assert.equal(EXAM_MODE.PRACTICE, 'PRACTICE');
});

test('AUDIT_ACTION: every entry uses the admin_* prefix convention', () => {
  for (const key of Object.keys(AUDIT_ACTION)) {
    const value = AUDIT_ACTION[key];
    assert.equal(typeof value, 'string', `${key} must be a string`);
    assert.match(value, /^(admin_|code_|exam_|clear_|question_set_|repair_)/,
      `${key} value ${value} should follow the audit action prefix convention`);
  }
});

test('AUDIT_ACTION: covers the most-used login/logout events', () => {
  assert.equal(AUDIT_ACTION.LOGIN_SUCCESS, 'admin_login_success');
  assert.equal(AUDIT_ACTION.LOGIN_FAILED, 'admin_login_failed');
  assert.equal(AUDIT_ACTION.LOGOUT, 'admin_logout');
  assert.equal(AUDIT_ACTION.SESSIONS_REVOKED, 'admin_sessions_revoked');
});

test('hasPermission: admin has all permissions via wildcard', () => {
  assert.equal(hasPermission('admin', 'codes:read'), true);
  assert.equal(hasPermission('admin', 'results:export'), true);
  assert.equal(hasPermission('admin', 'anything:weird'), true);
});

test('hasPermission: manager has codes:read but not content:write', () => {
  assert.equal(hasPermission('manager', 'codes:read'), true);
  assert.equal(hasPermission('manager', 'codes:generate'), true);
  assert.equal(hasPermission('manager', 'content:write'), false);
  assert.equal(hasPermission('manager', 'content:publish'), false);
});

test('hasPermission: reviewer can read but not write codes', () => {
  assert.equal(hasPermission('reviewer', 'results:read'), true);
  assert.equal(hasPermission('reviewer', 'compliance:read'), true);
  assert.equal(hasPermission('reviewer', 'codes:generate'), false);
  assert.equal(hasPermission('reviewer', 'content:write'), false);
});

test('hasPermission: content_editor can write content but not manage codes', () => {
  assert.equal(hasPermission('content_editor', 'content:write'), true);
  assert.equal(hasPermission('content_editor', 'content:publish'), true);
  assert.equal(hasPermission('content_editor', 'codes:generate'), false);
  assert.equal(hasPermission('content_editor', 'codes:delete'), false);
});

test('hasPermission: unknown role has no permissions', () => {
  assert.equal(hasPermission('hacker', 'codes:read'), false);
  assert.equal(hasPermission('', 'codes:read'), false);
  assert.equal(hasPermission(null, 'codes:read'), false);
});

test('ROLE_PERMISSIONS: admin is the only wildcard role', () => {
  assert.equal(ROLE_PERMISSIONS.admin.has('*'), true);
  for (const role of ['manager', 'reviewer', 'content_editor']) {
    assert.equal(ROLE_PERMISSIONS[role].has('*'), false,
      `${role} must NOT have the wildcard`);
  }
});

test('normalizeExamTitle: still works after the refactor', () => {
  assert.equal(normalizeExamTitle('ITIL Foundation V4'), 'Academy Exam App');
  assert.equal(normalizeExamTitle('SAP Basis Exam Q1'), 'Academy Exam Platform');
  assert.equal(normalizeExamTitle('Custom Quiz Name'), 'Custom Quiz Name');
  assert.equal(normalizeExamTitle(''), 'Academy Exam App');
  assert.equal(normalizeExamTitle(null), 'Academy Exam App');
});
