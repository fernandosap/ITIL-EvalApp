/* eslint-disable no-console */
'use strict';

(function (root) {
  function seatNumber(row) {
    const label = typeof row === 'string' ? row : row?.label;
    const match = String(label || '').match(/(\d+)/);
    return match ? Number(match[1]) : null;
  }

  function normalizedFilters(filters = {}) {
    return {
      questionSetId: String(filters.questionSetId || ''),
      status: String(filters.status || '').toLowerCase(),
      mode: String(filters.mode || '').toUpperCase(),
      dateFrom: String(filters.dateFrom || ''),
      dateTo: String(filters.dateTo || ''),
      seatFrom: String(filters.seatFrom || ''),
      seatTo: String(filters.seatTo || ''),
      archive: ['current', 'archived', 'all'].includes(String(filters.archive || '').toLowerCase())
        ? String(filters.archive).toLowerCase() : 'current'
    };
  }

  function rowMatchesFilters(row, filters = {}, activeSet = null) {
    const f = normalizedFilters(filters);
    if (f.archive === 'current' && row?.archived) return false;
    if (f.archive === 'archived' && !row?.archived) return false;
    if (f.status && String(row?.status || '').toLowerCase() !== f.status) return false;

    const effectiveSetId = row?.questionSetId != null
      ? Number(row.questionSetId)
      : (String(row?.status || '').toLowerCase() === 'unused' ? Number(activeSet?.id) : null);
    if (f.questionSetId && String(effectiveSetId || '') !== f.questionSetId) return false;

    const effectiveMode = String(row?.examMode || (String(row?.status || '').toLowerCase() === 'unused' ? activeSet?.examMode : '') || '').toUpperCase();
    if (f.mode && effectiveMode !== f.mode) return false;

    const seat = seatNumber(row);
    const seatFrom = f.seatFrom === '' ? null : Number(f.seatFrom);
    const seatTo = f.seatTo === '' ? null : Number(f.seatTo);
    if (Number.isFinite(seatFrom) && (seat == null || seat < seatFrom)) return false;
    if (Number.isFinite(seatTo) && (seat == null || seat > seatTo)) return false;

    const day = row?.submittedAt ? String(row.submittedAt).slice(0, 10) : '';
    if (f.dateFrom && (!day || day < f.dateFrom)) return false;
    if (f.dateTo && (!day || day > f.dateTo)) return false;
    return true;
  }

  function findCard(prefix) {
    const wrap = document.querySelector('.admin-wrap');
    if (!wrap) return null;
    return [...wrap.children].find((el) => el.classList?.contains('card') && String(el.textContent || '').trim().startsWith(prefix)) || null;
  }

  function rosterTable() {
    return [...document.querySelectorAll('.admin-table')].find((table) => {
      const headers = [...table.querySelectorAll('thead th')].map((th) => String(th.textContent || '').trim());
      return headers.includes('Code') && headers.includes('Seat') && headers.includes('Exam Set');
    }) || null;
  }

  function rowMap() {
    return new Map((root._adminRows || []).map((row) => [String(row.code || '').toUpperCase(), row]));
  }

  function visibleCodes() {
    const table = rosterTable();
    if (!table) return [];
    return [...table.querySelectorAll('tbody tr')]
      .filter((tr) => !tr.hidden)
      .map((tr) => String(tr.children?.[1]?.textContent || '').trim().toUpperCase())
      .filter(Boolean);
  }

  function updateSelectedCount() {
    const el = document.getElementById('selected-code-count');
    if (el) el.textContent = String(root._selectedCodes?.size || 0);
  }

  function applyRosterFilters() {
    const table = rosterTable();
    if (!table) return 0;
    const byCode = rowMap();
    const filters = normalizedFilters(root._exportFilters || {});
    let visible = 0;
    for (const tr of table.querySelectorAll('tbody tr')) {
      const code = String(tr.children?.[1]?.textContent || '').trim().toUpperCase();
      const row = byCode.get(code);
      if (!row) continue;
      const show = rowMatchesFilters(row, filters, root._activeQuestionSet);
      tr.hidden = !show;
      if (show) visible += 1;
    }
    const summary = document.getElementById('admin-filter-summary');
    if (summary) summary.textContent = `Showing ${visible} of ${(root._adminRows || []).length} records`;
    return visible;
  }

  function setDashboardFilter(key, value) {
    root._exportFilters = root._exportFilters || {};
    root._exportFilters[key] = String(value ?? '');
    // Never carry a bulk selection invisibly across a filter change.
    root._selectedCodes?.clear?.();
    document.querySelectorAll('.code-select').forEach((el) => { el.checked = false; });
    updateSelectedCount();
    applyRosterFilters();
  }

  function clearDashboardFilters() {
    root._exportFilters = {
      questionSetId: '', status: '', mode: '', dateFrom: '', dateTo: '',
      seatFrom: '', seatTo: '', archive: 'current'
    };
    if (root.IE?.admin?.showAdmin) return root.IE.admin.showAdmin();
  }

  function selectVisibleCodes() {
    if (!root._selectedCodes) root._selectedCodes = new Set();
    const codes = new Set(visibleCodes());
    root._selectedCodes.clear();
    for (const code of codes) root._selectedCodes.add(code);
    document.querySelectorAll('.code-select').forEach((el) => {
      const code = String(el.closest('tr')?.children?.[1]?.textContent || '').trim().toUpperCase();
      el.checked = codes.has(code);
    });
    updateSelectedCount();
  }

  function compactSystemStatus() {
    const card = findCard('System status');
    if (!card || card.dataset.compacted === '1') return;
    card.dataset.compacted = '1';
    const flex = card.firstElementChild;
    if (!flex || flex.children.length < 2) return;
    const technical = flex.children[1];
    const details = document.createElement('details');
    details.style.cssText = 'font-size:12px;color:#56627a;min-width:180px;align-self:flex-start';
    const summary = document.createElement('summary');
    summary.textContent = 'Technical details';
    summary.style.cssText = 'cursor:pointer;font-weight:700;color:#1f3864;text-align:right';
    details.appendChild(summary);
    technical.style.cssText += ';text-align:left;margin-top:8px;padding:8px 10px;border:1px solid #d9e2ef;border-radius:8px;background:#fff;min-width:250px';
    details.appendChild(technical);
    flex.appendChild(details);

    const status = root._adminSystemStatus || {};
    const left = flex.firstElementChild;
    if (left && !left.querySelector('[data-admin-key-status]')) {
      const key = document.createElement('div');
      key.dataset.adminKeyStatus = '1';
      key.style.cssText = 'font-size:12px;color:#405778;margin-top:7px;font-weight:600';
      key.textContent = `Active exam: ${status.activeQuestionSet?.name || '—'} · Access: ${status.examEnabled === false ? 'Closed' : 'Open'} · Stale sessions: ${status.staleSessionCount || 0}`;
      left.appendChild(key);
    }
  }

  function notificationsExpanded(expanded) {
    root._adminNotificationsExpanded = Boolean(expanded);
    const card = findCard('Notifications');
    if (!card) return;
    const list = card.children[1];
    if (!list) return;
    [...list.children].forEach((node, idx) => { node.hidden = !root._adminNotificationsExpanded && idx >= 3; });
    const btn = card.querySelector('[data-admin-notification-toggle]');
    const extra = Math.max(0, list.children.length - 3);
    if (btn) {
      btn.textContent = root._adminNotificationsExpanded ? 'Show less' : `Show ${extra} more`;
      btn.hidden = extra <= 0;
    }
  }

  function toggleNotifications() {
    notificationsExpanded(!root._adminNotificationsExpanded);
  }

  function compactNotifications() {
    const card = findCard('Notifications');
    if (!card || card.dataset.compacted === '1') return;
    card.dataset.compacted = '1';
    const header = card.firstElementChild;
    if (header) {
      const btn = document.createElement('button');
      btn.className = 'btn btn-secondary btn-sm';
      btn.dataset.action = 'toggleNotifications';
      btn.dataset.adminNotificationToggle = '1';
      btn.style.marginLeft = 'auto';
      header.appendChild(btn);
    }
    notificationsExpanded(false);
  }

  function moveRecentActivityToBottom() {
    const card = findCard('Recent Admin Activity');
    const wrap = document.querySelector('.admin-wrap');
    if (card && wrap) wrap.appendChild(card);
  }

  function addFilterControls() {
    const table = rosterTable();
    if (!table) return;
    const card = table.closest('.card');
    if (!card) return;
    const grid = [...card.children].find((el) => el.style?.display === 'grid' && el.querySelector('select[data-action="setExportFilter"]'));
    if (!grid || grid.dataset.enhancedFilters === '1') return;
    grid.dataset.enhancedFilters = '1';

    const seatFrom = document.createElement('input');
    seatFrom.type = 'number'; seatFrom.min = '0'; seatFrom.placeholder = 'Seat from';
    seatFrom.value = root._exportFilters?.seatFrom || '';
    seatFrom.style.cssText = 'margin:0;font-size:12px;padding:8px 10px';
    seatFrom.dataset.action = 'setExportFilter'; seatFrom.dataset.args = 'seatFrom,__value__';
    grid.appendChild(seatFrom);

    const seatTo = document.createElement('input');
    seatTo.type = 'number'; seatTo.min = '0'; seatTo.placeholder = 'Seat to';
    seatTo.value = root._exportFilters?.seatTo || '';
    seatTo.style.cssText = 'margin:0;font-size:12px;padding:8px 10px';
    seatTo.dataset.action = 'setExportFilter'; seatTo.dataset.args = 'seatTo,__value__';
    grid.appendChild(seatTo);

    const archive = document.createElement('select');
    archive.style.cssText = 'margin:0;font-size:12px;padding:8px 10px';
    archive.dataset.action = 'setExportFilter'; archive.dataset.args = 'archive,__value__';
    archive.innerHTML = '<option value="current">Current records</option><option value="archived">Archived</option><option value="all">Current + archived</option>';
    archive.value = root._exportFilters?.archive || 'current';
    grid.appendChild(archive);

    const clear = document.createElement('button');
    clear.className = 'btn btn-secondary btn-sm'; clear.textContent = 'Clear Filters';
    clear.dataset.action = 'clearDashboardFilters';
    grid.appendChild(clear);

    const top = card.firstElementChild;
    if (top && !top.querySelector('#admin-filter-summary')) {
      const summary = document.createElement('span');
      summary.id = 'admin-filter-summary';
      summary.style.cssText = 'font-size:12px;color:#56627a;font-weight:700';
      top.insertBefore(summary, top.firstChild?.nextSibling || null);
    }
  }

  function annotateExamHistoryAndArchiveActions() {
    const table = rosterTable();
    if (!table) return;
    const byCode = rowMap();
    for (const tr of table.querySelectorAll('tbody tr')) {
      const code = String(tr.children?.[1]?.textContent || '').trim().toUpperCase();
      const row = byCode.get(code);
      if (!row) continue;
      const examCell = tr.children?.[3];
      if (examCell && row.questionSetVersion && !examCell.querySelector('[data-exam-version]')) {
        const version = document.createElement('span');
        version.dataset.examVersion = '1';
        version.style.cssText = 'display:block;font-size:10px;color:#66758d;margin-top:2px';
        version.textContent = `Version ${row.questionSetVersion}`;
        examCell.appendChild(version);
      }
      if (examCell && ['active', 'completed'].includes(String(row.status || '').toLowerCase()) && !row.questionSetName) {
        examCell.textContent = 'Legacy / unknown exam';
      }
      const actions = tr.lastElementChild;
      if (actions && String(row.status || '').toLowerCase() === 'completed' && !actions.querySelector('[data-archive-row-action]')) {
        const btn = document.createElement('button');
        btn.className = 'btn btn-secondary btn-sm';
        btn.dataset.archiveRowAction = '1';
        btn.dataset.action = row.archived ? 'unarchiveCode' : 'archiveCode';
        btn.dataset.args = code;
        btn.textContent = row.archived ? 'Restore' : 'Archive';
        actions.appendChild(document.createTextNode(' '));
        actions.appendChild(btn);
      }
    }
  }

  function addBulkArchiveControls() {
    const table = rosterTable();
    const card = table?.closest('.card');
    if (!card) return;
    const deleteButton = [...card.querySelectorAll('button')].find((btn) => /Delete Selected/.test(btn.textContent || ''));
    const controls = deleteButton?.parentElement;
    if (!controls || controls.querySelector('[data-bulk-archive]')) return;

    const archive = document.createElement('button');
    archive.className = 'btn btn-secondary btn-sm'; archive.textContent = 'Archive Selected';
    archive.dataset.action = 'archiveSelectedCodes'; archive.dataset.bulkArchive = '1';
    controls.insertBefore(archive, deleteButton);

    const restore = document.createElement('button');
    restore.className = 'btn btn-secondary btn-sm'; restore.textContent = 'Restore Selected';
    restore.dataset.action = 'unarchiveSelectedCodes'; restore.dataset.bulkArchive = '1';
    controls.insertBefore(restore, deleteButton);
  }

  async function updateArchive(codes, archived) {
    const util = root.IE?.util;
    const list = [...new Set((Array.isArray(codes) ? codes : [codes]).map((v) => String(v || '').trim().toUpperCase()).filter(Boolean))];
    if (!list.length) {
      util?.modal?.('ℹ️', 'No Codes Selected', 'Select one or more completed results first.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    if (archived) {
      const byCode = rowMap();
      const invalid = list.filter((code) => String(byCode.get(code)?.status || '').toLowerCase() !== 'completed');
      if (invalid.length) {
        util?.modal?.('ℹ️', 'Completed Results Only', 'Only completed exam results can be archived. Remove active or unused codes from the selection first.', [{ label: 'OK', cls: 'btn-primary' }]);
        return;
      }
    }
    try {
      await util.apiJson(`/api/admin/codes/${archived ? 'archive' : 'unarchive'}`, {
        method: 'POST', body: JSON.stringify({ codes: list })
      }, { timeoutMs: 15000, retries: 0 });
      root._selectedCodes?.clear?.();
      await root.IE.admin.showAdmin();
    } catch (_e) {
      util?.modal?.('❌', archived ? 'Archive Failed' : 'Restore Failed', archived ? 'The selected completed results could not be archived.' : 'The selected records could not be restored.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  function archiveCode(code) { return updateArchive([code], true); }
  function unarchiveCode(code) { return updateArchive([code], false); }
  function archiveSelectedCodes() { return updateArchive([...(root._selectedCodes || [])], true); }
  function unarchiveSelectedCodes() { return updateArchive([...(root._selectedCodes || [])], false); }

  function enhanceAdminDashboard() {
    compactSystemStatus();
    compactNotifications();
    addFilterControls();
    annotateExamHistoryAndArchiveActions();
    addBulkArchiveControls();
    applyRosterFilters();
    moveRecentActivityToBottom();
  }

  function install() {
    root.IE = root.IE || {};
    if (!root.IE.admin || root.IE.adminFeedback?.installed) return false;
    root._exportFilters = Object.assign({
      questionSetId: '', status: '', mode: '', dateFrom: '', dateTo: '', seatFrom: '', seatTo: '', archive: 'current'
    }, root._exportFilters || {});
    root._adminNotificationsExpanded = false;

    const originalShowAdmin = root.IE.admin.showAdmin.bind(root.IE.admin);
    root.IE.admin.showAdmin = async function enhancedShowAdmin() {
      const value = await originalShowAdmin();
      enhanceAdminDashboard();
      return value;
    };
    root.IE.admin.setExportFilter = setDashboardFilter;
    root.IE.admin.selectAllVisibleCodes = selectVisibleCodes;
    root.IE.admin.toggleNotifications = toggleNotifications;
    root.IE.admin.clearDashboardFilters = clearDashboardFilters;
    root.IE.admin.archiveCode = archiveCode;
    root.IE.admin.unarchiveCode = unarchiveCode;
    root.IE.admin.archiveSelectedCodes = archiveSelectedCodes;
    root.IE.admin.unarchiveSelectedCodes = unarchiveSelectedCodes;

    root.IE.adminFeedback = {
      installed: true,
      seatNumber,
      normalizedFilters,
      rowMatchesFilters,
      applyRosterFilters,
      compactSystemStatus,
      compactNotifications,
      moveRecentActivityToBottom,
      enhanceAdminDashboard,
      install
    };
    return true;
  }

  root.IE = root.IE || {};
  root.IE.adminFeedback = root.IE.adminFeedback || {
    installed: false,
    seatNumber,
    normalizedFilters,
    rowMatchesFilters,
    install
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { seatNumber, normalizedFilters, rowMatchesFilters };
  }
})(typeof window !== 'undefined' ? window : globalThis);
