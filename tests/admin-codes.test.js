'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

test('showAdmin: renders code rows with the exam status chip', async () => {
  let rendered = '';
  const responses = {
    '/api/admin/codes': {
      role: 'admin',
      codes: [{ code: 'ABC2DE', status: 'unused', label: 'Seat 1' }],
      questionSets: []
    },
    '/api/admin/system-status': { warnings: [], staleSessions: [], examEnabled: true },
    '/api/admin/audit?limit=12': { entries: [] },
    '/api/admin/notifications': { notifications: [] },
    '/api/admin/analytics/overview?days=30': { ok: true, summary: {} }
  };
  const root = {
    S: {},
    _adminRows: [],
    _selectedCodes: new Set(),
    _exportFilters: { questionSetId: '', status: '', mode: '', dateFrom: '', dateTo: '' },
    IE: {
      util: {
        $: () => null,
        render: (html) => { rendered = html; },
        modal: () => {},
        _esc: (value) => String(value == null ? '' : value),
        apiFetch: async () => null,
        apiJson: async (url) => responses[url],
        durationLabel: () => '0m 00s',
        normalizeExamTitle: (value) => String(value || ''),
        roleCan: () => true
      },
      exam: {
        statusChip: (row) => `<span>${row.status}</span>`
      }
    }
  };
  const sandbox = {
    window: root,
    globalThis: root,
    document: { body: { classList: { remove() {} } } },
    fetch: async () => ({ ok: false }),
    console
  };
  vm.createContext(sandbox);
  vm.runInContext(
    fs.readFileSync(path.join(__dirname, '..', 'client', 'admin-codes.js'), 'utf8'),
    sandbox
  );

  await root.IE.admin.showAdmin();

  assert.match(rendered, /ABC2DE/);
  assert.match(rendered, /<span>unused<\/span>/);
});
