'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const serverFeedback = require('../lib/core/admin-feedback.js');
const clientFeedback = require('../client/admin-feedback.js');

function dbRow(overrides = {}) {
  return {
    STATUS: 'completed',
    ASSIGNED_QUESTION_SET_ID: 99,
    ASSIGNED_QUESTION_SET_NAME: 'Current Default',
    ASSIGNED_QUESTION_SET_VERSION: 9,
    ASSIGNED_EXAM_MODE: 'GRADED',
    ATTEMPT_QUESTION_SET_ID: null,
    ATTEMPT_QUESTION_SET_NAME: null,
    ATTEMPT_QUESTION_SET_VERSION: null,
    RESULT_JSON: null,
    SESSION_JSON: null,
    ...overrides
  };
}

test('completed attempts use immutable snapshot instead of current assignment', () => {
  const resolved = serverFeedback.resolveAttemptSnapshot(dbRow({
    ATTEMPT_QUESTION_SET_ID: 7,
    ATTEMPT_QUESTION_SET_NAME: 'Exam A',
    ATTEMPT_QUESTION_SET_VERSION: 3,
    RESULT_JSON: JSON.stringify({ questionSetId: 7, questionSetName: 'Exam A', examMode: 'GRADED' })
  }));
  assert.equal(resolved.questionSetId, 7);
  assert.equal(resolved.questionSetName, 'Exam A');
  assert.equal(resolved.questionSetVersion, 3);
  assert.notEqual(resolved.questionSetId, 99);
});

test('legacy completed attempts recover historical exam from stored result JSON', () => {
  const resolved = serverFeedback.resolveAttemptSnapshot(dbRow({
    RESULT_JSON: JSON.stringify({ questionSetId: 4, questionSetName: 'Historic Exam', questionSetVersion: 2, examMode: 'PRACTICE' })
  }));
  assert.equal(resolved.questionSetId, 4);
  assert.equal(resolved.questionSetName, 'Historic Exam');
  assert.equal(resolved.questionSetVersion, 2);
  assert.equal(resolved.examMode, 'PRACTICE');
});

test('active attempts recover the set identity from saved session instead of active default', () => {
  const resolved = serverFeedback.resolveAttemptSnapshot(dbRow({
    STATUS: 'active',
    SESSION_JSON: JSON.stringify({ session: { questionSetId: 12, questionSetName: 'In Progress Exam', questionSetVersion: 5, examMode: 'GRADED' } })
  }));
  assert.equal(resolved.questionSetId, 12);
  assert.equal(resolved.questionSetName, 'In Progress Exam');
  assert.equal(resolved.questionSetVersion, 5);
});

test('unused codes continue to follow their explicit assignment without creating history early', () => {
  const resolved = serverFeedback.resolveAttemptSnapshot(dbRow({ STATUS: 'unused' }));
  assert.equal(resolved.questionSetId, 99);
  assert.equal(resolved.questionSetName, 'Current Default');
  assert.equal(resolved.questionSetVersion, 9);
});

test('unknown historical attempts never fall back to the current active exam name', () => {
  const resolved = serverFeedback.resolveAttemptSnapshot(dbRow({
    ASSIGNED_QUESTION_SET_ID: null,
    ASSIGNED_QUESTION_SET_NAME: '',
    ASSIGNED_QUESTION_SET_VERSION: null
  }));
  assert.equal(resolved.questionSetId, null);
  assert.equal(resolved.questionSetName, 'Legacy / unknown exam');
});

test('server filters support seat ranges, archive state, exam set, mode, status and dates', () => {
  const row = {
    label: 'Seat 24', archived: false, questionSetId: 7, status: 'completed', examMode: 'GRADED', submittedAt: '2026-08-31T20:00:00.000Z'
  };
  const filters = serverFeedback.normalizeFilters({
    seatFrom: '20', seatTo: '30', archive: 'current', questionSetId: '7', status: 'completed', mode: 'GRADED', dateFrom: '2026-08-30', dateTo: '2026-09-01'
  });
  assert.equal(serverFeedback.rowMatchesFilters(row, filters), true);
  assert.equal(serverFeedback.rowMatchesFilters(row, { ...filters, seatFrom: 25 }), false);
  assert.equal(serverFeedback.rowMatchesFilters({ ...row, archived: true }, filters), false);
});

test('client filters mirror server behavior and treat unassigned unused codes as current default only', () => {
  const activeSet = { id: 3, examMode: 'GRADED' };
  const unused = { label: 'Seat 8', status: 'unused', questionSetId: null, examMode: '', archived: false };
  assert.equal(clientFeedback.rowMatchesFilters(unused, { questionSetId: '3', archive: 'current' }, activeSet), true);
  const completedUnknown = { label: 'Seat 8', status: 'completed', questionSetId: null, examMode: 'GRADED', archived: false };
  assert.equal(clientFeedback.rowMatchesFilters(completedUnknown, { questionSetId: '3', archive: 'current' }, activeSet), false);
});

test('archived records are hidden by default and explicitly recoverable', () => {
  const row = { label: 'Seat 1', status: 'completed', archived: true, questionSetId: 1, examMode: 'GRADED' };
  assert.equal(clientFeedback.rowMatchesFilters(row, { archive: 'current' }), false);
  assert.equal(clientFeedback.rowMatchesFilters(row, { archive: 'archived' }), true);
  assert.equal(clientFeedback.rowMatchesFilters(row, { archive: 'all' }), true);
});

test('codes response enrichment replaces mutable question-set identity with historical identity', () => {
  const body = { codes: [{ code: 'ABC234', status: 'completed', questionSetId: 99, questionSetName: 'Current Default' }] };
  const merged = serverFeedback.mergeHistoricalRows(body, [{
    code: 'ABC234', questionSetId: 7, questionSetName: 'Exam A', questionSetVersion: 3,
    examMode: 'GRADED', isPractice: false, countsTowardResults: true, archivedAt: null, archived: false
  }]);
  assert.equal(merged.codes[0].questionSetId, 7);
  assert.equal(merged.codes[0].questionSetName, 'Exam A');
  assert.equal(merged.codes[0].questionSetVersion, 3);
});

test('runtime schema and route wiring include immutable attempt snapshots and reversible archive APIs', () => {
  const db = fs.readFileSync(path.join(__dirname, '..', 'lib', 'core', 'db.js'), 'utf8');
  const adminRoutes = fs.readFileSync(path.join(__dirname, '..', 'lib', 'routes', 'admin.js'), 'utf8');
  const examRoutes = fs.readFileSync(path.join(__dirname, '..', 'lib', 'routes', 'exam.js'), 'utf8');
  for (const column of ['ATTEMPT_QUESTION_SET_ID', 'ATTEMPT_QUESTION_SET_NAME', 'ATTEMPT_QUESTION_SET_VERSION', 'ARCHIVED_AT']) {
    assert.match(db, new RegExp(column));
  }
  assert.match(adminRoutes, /\/api\/admin\/codes\/archive/);
  assert.match(adminRoutes, /\/api\/admin\/codes\/unarchive/);
  assert.match(adminRoutes, /replaceLast\(exportCsv, adminFeedback\.exportHandler\)/);
  assert.match(examRoutes, /freezeAttemptOnStart/);
});

test('schema migration remains schema-agnostic', () => {
  const sql = fs.readFileSync(path.join(__dirname, '..', 'migrations', '2026-08-31_admin_history_archive.sql'), 'utf8');
  assert.match(sql, /CURRENT_SCHEMA/);
  assert.doesNotMatch(sql, /"ITIL_EXAM"/);
});
