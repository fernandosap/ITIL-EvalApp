/* eslint-disable no-console */
// client/admin-codes.js — the admin dashboard: code list, results, audit
// log, system status, analytics tiles, and the destructive operations
// (delete, reset, bulk-delete, repair/clear summaries).
//
// showAdmin() is the centerpiece — it loads 5 endpoints in parallel
// and renders the full admin console. The other functions in this
// module are the per-row/per-button handlers (delete, reset, flag
// viewer, exports, etc.).
//
// Public surface (attached to window.IE.admin):
//   showAdmin, reviewResult, summaryValue, sortAdminRows,
//   seatSortValue, flagsFor, clearStaleSessions, toggleExamAvailability,
//   repairResultSummaries, clearResultSummaries, deleteCode,
//   updateSelectedCodeCount, toggleCodeSelection, selectAllVisibleCodes,
//   clearCodeSelection, bulkDeleteCodes, saveNote, resetCode,
//   generateCodes, setExportFilter, downloadExport, downloadAuditExport,
//   downloadSignedResultSummary, auditActionLabel

(function (root) {
  const { $, render, modal, _esc, apiJson, apiFetch, durationLabel, normalizeExamTitle, roleCan } = root.IE.util;
  const { statusChip } = root.IE.exam;
  const S = root.S;

  // ---- Helpers ----
  function summaryValue(rows, status) {
    return rows.filter((r) => r.status === status).length;
  }
  function seatSortValue(row) {
    const label = String(row?.label || '');
    const match = label.match(/(\d+)/);
    if (match) return Number(match[1]);
    return Number.MAX_SAFE_INTEGER;
  }
  function sortAdminRows(rows) {
    return [...rows].sort((a, b) => {
      const seatDiff = seatSortValue(a) - seatSortValue(b);
      if (seatDiff !== 0) return seatDiff;
      return String(a.code || '').localeCompare(String(b.code || ''));
    });
  }
  function auditActionLabel(action) {
    switch (action) {
      case 'admin_login_success': return 'Login success';
      case 'admin_login_failed': return 'Login failed';
      case 'admin_logout': return 'Logout';
      case 'admin_sessions_revoked': return 'All admin sessions revoked';
      case 'admin_exam_availability_updated': return 'Exam availability changed';
      case 'admin_note_saved': return 'Note saved';
      case 'admin_code_reset': return 'Code reset';
      case 'admin_code_deleted': return 'Code deleted';
      case 'admin_codes_bulk_deleted': return 'Codes bulk deleted';
      case 'admin_codes_generated': return 'Codes generated';
      case 'admin_stale_sessions_cleared': return 'Stale sessions cleared';
      case 'admin_result_summaries_repaired': return 'Scores repaired';
      case 'admin_result_summaries_cleared': return 'Scores cleared';
      case 'admin_code_question_set_assigned': return 'Code exam assigned';
      case 'admin_question_set_created': return 'Exam created';
      case 'admin_question_set_uploaded': return 'Exam uploaded';
      case 'admin_question_set_config_updated': return 'Exam config updated';
      case 'admin_question_set_activated': return 'Exam set active';
      case 'admin_question_set_deleted': return 'Exam deleted';
      case 'admin_question_created': return 'Question created';
      case 'admin_question_updated': return 'Question updated';
      case 'admin_question_deleted': return 'Question deleted';
      case 'admin_section_created': return 'Section created';
      case 'admin_section_updated': return 'Section updated';
      case 'admin_section_deleted': return 'Section deleted';
      default: return action || 'Unknown action';
    }
  }

  // ---- showAdmin (the dashboard) ----
  async function showAdmin() {
    S.screen = 'admin';
    document.body.classList.remove('exam-bg');
    render('<div class="admin-wrap"><div style="padding:60px;text-align:center;color:white;font-size:18px">Loading admin data...</div></div>');
    let data, systemStatus, auditData, notificationData, overviewData, me;
    try {
      [me, data, systemStatus, auditData, notificationData, overviewData] = await Promise.all([
        // /api/admin/me is the canonical source for the current role —
        // it works regardless of which permission set the operator has
        // (e.g. reviewer without codes:read). Earlier we used `data.role`
        // from /api/admin/codes, which is fine for admins/managers but
        // fails open for everyone else. Keep the codes response as a
        // fallback in case /me 401s (e.g. session expired mid-load).
        fetch('/api/admin/me', { credentials: 'same-origin' }).then((r) => r.ok ? r.json() : null).catch(() => null),
        apiJson('/api/admin/codes', {}, { timeoutMs: 12000, retries: 1 }),
        apiJson('/api/admin/system-status', {}, { timeoutMs: 12000, retries: 1 }),
        apiJson('/api/admin/audit?limit=12', {}, { timeoutMs: 12000, retries: 1 }),
        apiJson('/api/admin/notifications', {}, { timeoutMs: 12000, retries: 1 }),
        apiJson('/api/admin/analytics/overview?days=30', {}, { timeoutMs: 16000, retries: 1 })
      ]);
    } catch (_e) {
      data = systemStatus = auditData = notificationData = overviewData = me = null;
    }
    if (!data || data.error) {
      modal('❌', 'Error', 'Could not load admin data from the server.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    root._adminSystemStatus = systemStatus;
    root._adminAuditEntries = Array.isArray(auditData?.entries) ? auditData.entries : [];
    root._adminNotifications = Array.isArray(notificationData?.notifications) ? notificationData.notifications : [];
    root._adminOverview = overviewData && overviewData.ok ? overviewData : null;
    // /me first, then /codes.role, then whatever we already had.
    root._adminRole = (me && me.ok && me.role) || (data && data.role) || root._adminRole || 'admin';
    if (me && me.authMethod) root._adminAuthMethod = me.authMethod;
    const canAdmin = roleCan('*');
    const canContentRead = roleCan('content:read');
    const canContentWrite = roleCan('content:write');
    const canContentPublish = roleCan('content:publish');
    const canAnalytics = roleCan('analytics:read');
    const canAuditExport = roleCan('audit:export');
    root._adminRows = sortAdminRows(data.codes || []);
    root._selectedCodes = new Set([...root._selectedCodes].filter((code) => root._adminRows.some((row) => row.code === code)));
    root._adminQuestionSets = Array.isArray(data.questionSets) ? data.questionSets : [];
    root._activeQuestionSet = root._adminQuestionSets.find((set) => set.isActive) || root._adminQuestionSets[0] || null;
    const unused = summaryValue(root._adminRows, 'unused');
    const active = summaryValue(root._adminRows, 'active');
    const completed = summaryValue(root._adminRows, 'completed');
    const warnings = Array.isArray(systemStatus?.warnings) ? systemStatus.warnings : [];
    const staleSessions = Array.isArray(systemStatus?.staleSessions) ? systemStatus.staleSessions : [];
    const examOpen = systemStatus?.examEnabled !== false;
    const metricTiles = root._adminOverview ? `
      <div class="card" style="margin-bottom:16px">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:12px">
          <div style="font-size:16px;font-weight:800;color:#1F3864">30-Day Analytics</div>
          <div style="font-size:12px;color:#666">${root._adminOverview.summary?.attempts || 0} attempts</div>
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:14px">
          <div class="metric-card"><div class="metric-label">Avg %</div><div class="metric-value">${root._adminOverview.summary?.averagePct ?? '—'}</div></div>
          <div class="metric-card"><div class="metric-label">Pass Rate</div><div class="metric-value">${root._adminOverview.summary?.passRate == null ? '—' : `${root._adminOverview.summary.passRate}%`}</div></div>
          <div class="metric-card"><div class="metric-label">Avg Duration</div><div class="metric-value">${root._adminOverview.summary?.averageDurationSecs ? durationLabel(root._adminOverview.summary.averageDurationSecs) : '—'}</div></div>
          <div class="metric-card"><div class="metric-label">Question Sets</div><div class="metric-value">${Array.isArray(root._adminOverview.byQuestionSet) ? root._adminOverview.byQuestionSet.length : 0}</div></div>
        </div>
        <div style="display:grid;grid-template-columns:1.2fr .8fr;gap:14px">
          <div style="overflow-x:auto">
            <table class="admin-table"><thead><tr><th>Day</th><th style="text-align:center">Attempts</th><th style="text-align:center">Avg %</th><th style="text-align:center">Pass Rate</th></tr></thead><tbody>${(root._adminOverview.trend || []).length ? root._adminOverview.trend.slice(-10).map((row) => `<tr><td>${_esc(row.day)}</td><td style="text-align:center">${row.attempts}</td><td style="text-align:center">${row.averagePct ?? '—'}</td><td style="text-align:center">${row.passRate == null ? '—' : `${row.passRate}%`}</td></tr>`).join('') : '<tr><td colspan="4" style="text-align:center;color:#888;padding:16px">No trend data</td></tr>'}</tbody></table>
          </div>
          <div style="overflow-x:auto">
            <table class="admin-table"><thead><tr><th>Weakest Section</th><th style="text-align:center">Avg %</th></tr></thead><tbody>${(root._adminOverview.weakestSections || []).length ? root._adminOverview.weakestSections.slice(0, 8).map((row) => `<tr><td>${_esc(row.questionSetName || '')} · ${_esc(row.name || 'Section')}</td><td style="text-align:center">${row.averagePct ?? '—'}</td></tr>`).join('') : '<tr><td colspan="2" style="text-align:center;color:#888;padding:16px">No section data</td></tr>'}</tbody></table>
          </div>
        </div>
      </div>` : '';
    const notificationsCard = `
      <div class="card" style="margin-bottom:16px">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:10px">
          <div style="font-size:16px;font-weight:800;color:#1F3864">Notifications</div>
          <div style="font-size:12px;color:#666">${root._adminNotifications.length} active</div>
        </div>
        <div style="display:grid;gap:8px">
          ${root._adminNotifications.length ? root._adminNotifications.map((item) => `<div style="padding:10px 12px;border-radius:12px;border:1px solid ${item.level === 'high' ? '#f1c0c0' : '#e7d6a2'};background:${item.level === 'high' ? '#fff7f7' : '#fffaf0'}"><div style="font-size:13px;font-weight:700;color:${item.level === 'high' ? '#9f2d22' : '#8a5b00'}">${_esc(item.message || '')}</div>${item.detail ? `<div style="font-size:11px;color:#666;margin-top:4px">${_esc(item.detail)}</div>` : ''}</div>`).join('') : '<div style="font-size:12px;color:#666">No active alerts.</div>'}
        </div>
      </div>`;
    const systemBanner = systemStatus ? `
      <div class="card" style="margin-bottom:16px;background:${systemStatus.ok ? 'rgba(238,247,242,.98)' : 'rgba(255,245,245,.98)'};border-left:6px solid ${systemStatus.ok ? '#2e7d32' : '#c0392b'}">
        <div style="display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:flex-start">
          <div>
            <div style="font-size:16px;font-weight:800;color:${systemStatus.ok ? '#1f5f2c' : '#9f2d22'}">
              ${systemStatus.ok ? 'System status healthy' : 'System status needs attention'}
            </div>
            <div style="font-size:13px;color:#555;margin-top:4px">
              ${systemStatus.questionCount} questions across ${systemStatus.questionSetCount || 0} exam set${systemStatus.questionSetCount === 1 ? '' : 's'} · ${systemStatus.accessCodeCount} codes · ${systemStatus.activeSessionCount} active sessions · ${systemStatus.resultCount} completed results
            </div>
          </div>
          <div style="font-size:12px;color:#666;text-align:right">
            <div>Version: ${_esc(systemStatus.appVersion || '—')}</div>
            <div>Revision: ${_esc(systemStatus.appRevision || '—')}</div>
            <div>Deployed: ${systemStatus.deployedAt ? _esc(new Date(systemStatus.deployedAt).toLocaleString()) : '—'}</div>
            <div>Schema: ${_esc(systemStatus.schema || '—')}</div>
            <div>Active exam: ${_esc(systemStatus.activeQuestionSet?.name || '—')}</div>
            <div>Exam access: ${examOpen ? 'open' : 'closed'}</div>
            <div>Notes: ${systemStatus.notesEnabled ? 'enabled' : 'missing'}</div>
            <div>Stale sessions: ${systemStatus.staleSessionCount || 0}</div>
            <div>Audit log: ${systemStatus.auditEnabled ? `${systemStatus.auditCount} entries` : 'missing'}</div>
            <div>Admin env: ${systemStatus.adminConfigured ? 'configured' : 'missing'}</div>
            <div>Manager login: ${systemStatus.managerConfigured ? 'configured' : 'not configured'}</div>
            <div>Reviewer login: ${systemStatus.reviewerConfigured ? 'configured' : 'not configured'}</div>
            <div>Content editor: ${systemStatus.contentEditorConfigured ? 'configured' : 'not configured'}</div>
            <div>Admin session reset: ${systemStatus.adminSessionRevokedAt ? _esc(new Date(systemStatus.adminSessionRevokedAt).toLocaleString()) : 'never'}</div>
          </div>
        </div>
        ${staleSessions.length ? `<div style="margin-top:10px;padding:10px 12px;background:rgba(255,248,230,.9);border-radius:10px;color:#8a5b00;font-size:13px">
          <strong>Stale active sessions (${systemStatus.staleSessionMinutes}+ min):</strong><br>
          ${staleSessions.map((s) => `${_esc(s.code)} · last save ${s.updatedAt ? _esc(new Date(s.updatedAt).toLocaleString()) : 'unknown'}`).join('<br>')}
          ${canAdmin ? '<div style="margin-top:10px"><button class="btn btn-danger btn-sm" data-action="clearStaleSessions">Clear Stale Sessions</button></div>' : ''}
        </div>` : ''}
        ${warnings.length ? `<div style="margin-top:10px;font-size:13px;color:#7a251d">${warnings.map((w) => `• ${_esc(w)}`).join('<br>')}</div>` : ''}
      </div>` : '';

    const setRows = root._adminQuestionSets.map((set) => `
      <tr style="background:${set.isActive ? 'rgba(236,247,239,.9)' : 'white'}">
        <td>
          <strong>${_esc(set.name)}</strong>${set.isActive ? ' <span style="color:#1a5c1a;font-size:11px;font-weight:700">● DEFAULT</span>' : ''}
          ${set.description ? `<div style="font-size:12px;color:#777;margin-top:3px">${_esc(set.description)}</div>` : ''}
        </td>
        <td style="text-align:center">${set.questionCount || 0}</td>
        <td style="text-align:center">v${set.versionNumber || 1}</td>
        <td style="text-align:center">${_esc(set.lifecycleStatus || 'PUBLISHED')}</td>
        <td style="text-align:center">${set.numQuestions ? `${set.numQuestions} of ${set.questionCount || 0}` : `All ${set.questionCount || 0}`}</td>
        <td style="text-align:center">${set.durationMinutes || 45}m</td>
        <td style="text-align:center">${set.passPct || 80}%</td>
        <td style="text-align:center">${set.examMode === 'PRACTICE' ? '<span class="chip chip-pass">Practice</span>' : '<span class="chip chip-active">Graded</span>'}</td>
        <td style="text-align:center">${set.proctorEnabled !== false ? 'On' : 'Off'}</td>
        <td style="text-align:center;white-space:nowrap">
          ${canAnalytics ? `<button class="btn btn-secondary btn-sm" data-action="showQuestionSetAnalytics" data-args="${set.id}">Analytics</button>` : ''}
          ${canContentRead ? `<button class="btn btn-secondary btn-sm" data-action="openQuestionSet" data-args="${set.id},${_esc(set.name)}">Manage</button>` : ''}
          ${roleCan('results:export') ? `<button class="btn btn-secondary btn-sm" data-action="exportQuestionSet" data-args="${set.id}">Export</button>` : ''}
          ${canContentWrite ? `<button class="btn btn-secondary btn-sm" data-action="cloneQuestionSet" data-args="${set.id},${_esc(set.name)}">Clone</button>` : ''}
          ${canContentWrite ? `<button class="btn btn-secondary btn-sm" data-action="configQuestionSet" data-args="${set.id},${set.durationMinutes || 45},${set.passPct || 80},${set.proctorEnabled !== false},${set.numQuestions == null ? 'null' : set.numQuestions},${set.questionCount || 0}">Config</button>` : ''}
          ${canContentPublish && !set.isActive ? `<button class="btn btn-primary btn-sm" data-action="activateQuestionSet" data-args="${set.id}">Set Default</button>` : ''}
          ${canContentPublish && String(set.lifecycleStatus || '') !== 'PUBLISHED' ? `<button class="btn btn-secondary btn-sm" data-action="publishQuestionSet" data-args="${set.id}">Publish</button>` : ''}
          ${canContentPublish && !set.isActive && String(set.lifecycleStatus || '') !== 'ARCHIVED' ? `<button class="btn btn-secondary btn-sm" data-action="archiveQuestionSet" data-args="${set.id}">Archive</button>` : ''}
          ${roleCan('imports:rollback') && set.importSource === 'csv_upload' && !set.isActive ? `<button class="btn btn-danger btn-sm" data-action="rollbackImportedSet" data-args="${set.id}">Rollback</button>` : ''}
          ${canContentPublish && !set.isActive ? `<button class="btn btn-danger btn-sm" data-action="deleteQuestionSet" data-args="${set.id},${_esc(set.name)}">Delete</button>` : ''}
        </td>
      </tr>`).join('');

    const rows = root._adminRows.map((row) => `
      <tr>
        <td style="text-align:center">${canAdmin ? `<input type="checkbox" class="code-select" ${root._selectedCodes.has(row.code) ? 'checked' : ''} data-action="toggleCodeSelection" data-args="${row.code},__checked__">` : ''}</td>
        <td style="font-family:monospace;font-weight:700">${_esc(row.code)}</td>
        <td>${_esc(row.label || '')}</td>
        <td>
          ${row.status === 'unused'
            ? `<select style="margin:0;width:220px;font-size:12px;padding:6px 8px" data-action="assignQuestionSet" data-args="${row.code},__value__">
                <option value="" ${row.questionSetId == null ? 'selected' : ''}>${root._activeQuestionSet ? `${_esc(root._activeQuestionSet.name)} (default)` : 'Default active set'}</option>
                ${root._adminQuestionSets.map((set) => `<option value="${set.id}" ${row.questionSetId === set.id ? 'selected' : ''}>${_esc(set.name)}${set.isActive ? ' ⭐' : ''}</option>`).join('')}
              </select>`
            : `<span style="font-size:12px;color:#555">${_esc(normalizeExamTitle(row.questionSetName || root._activeQuestionSet?.name || 'Default active set'))}</span>`}
        </td>
        <td><input type="text" value="${_esc(row.notes || '')}" style="margin:0;width:220px;font-size:12px;padding:6px 8px" data-blur-action="saveNote" data-args="${row.code},__value__"></td>
        <td>${statusChip(row)}</td>
        <td style="text-align:center">${row.score == null ? '—' : row.score}</td>
        <td style="text-align:center">${row.pct == null ? '—' : `${row.pct}%`}</td>
        <td style="text-align:center">${row.durationSecs == null ? '—' : durationLabel(row.durationSecs)}</td>
        <td style="text-align:center">${row.tabSwitches || 0}</td>
        <td style="text-align:center">${row.incidentCount ? `<button class="btn btn-secondary btn-sm" data-action="flagsFor" data-args="${row.code}">${row.incidentCount}</button>` : '0'}</td>
        <td style="text-align:center">${row.submittedAt ? new Date(row.submittedAt).toLocaleString() : '—'}</td>
        <td style="text-align:center;white-space:nowrap">
          ${row.status === 'completed' ? `<button class="btn btn-secondary btn-sm" data-action="reviewResult" data-args="${row.code}">Review</button>` : ''}
          ${canAdmin ? `<button class="btn btn-danger btn-sm" data-action="resetCode" data-args="${row.code}">Reset</button>` : ''}
          ${canAdmin ? `<button class="btn btn-danger btn-sm" data-action="deleteCode" data-args="${row.code},${row.status}">Delete</button>` : ''}
        </td>
      </tr>`).join('');

    render(`<div class="admin-wrap">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:16px">
        <div>
          <div style="font-size:22px;font-weight:800;color:white">Admin Console</div>
          <div style="font-size:13px;color:rgba(255,255,255,.75)">${unused} unused · ${active} active · ${completed} completed · ${root._adminQuestionSets.length} exam set${root._adminQuestionSets.length === 1 ? '' : 's'} · Role: ${root._adminRole === 'admin' ? 'Admin' : root._adminRole === 'manager' ? 'Manager' : root._adminRole === 'reviewer' ? 'Reviewer' : 'Content Editor'}</div>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn btn-primary btn-sm" data-action="generateCodes">+ Generate Codes</button>
          ${canContentWrite ? '<button class="btn btn-primary btn-sm" data-action="createQuestionSet">+ New Exam Set</button>' : ''}
          ${roleCan('imports:write') ? '<button class="btn btn-secondary btn-sm" data-action="showUploadQuestionSet">Upload Exam CSV</button>' : ''}
          ${canAdmin ? `<button class="btn btn-secondary btn-sm" data-action="toggleExamAvailability" data-args="${examOpen ? 'false' : 'true'}">${examOpen ? 'Close Exams' : 'Open Exams'}</button>` : ''}
          ${canAdmin ? '<button class="btn btn-secondary btn-sm" data-action="revokeAdminSessions">Revoke Sessions</button>' : ''}
          ${canAdmin ? '<button class="btn btn-secondary btn-sm" data-action="repairResultSummaries">Repair Scores</button>' : ''}
          ${canAdmin ? '<button class="btn btn-secondary btn-sm" data-action="clearResultSummaries">Clear Scores</button>' : ''}
          <button class="btn btn-secondary btn-sm" data-action="downloadExport">Export CSV</button>
          ${canAuditExport ? '<button class="btn btn-secondary btn-sm" data-action="downloadAuditExport">Export Audit JSON</button>' : ''}
          <button class="btn btn-secondary btn-sm" data-action="logoutAdmin">Logout</button>
          <button class="btn btn-secondary btn-sm" data-action="showAdmin">↻ Refresh</button>
        </div>
      </div>
      ${systemBanner}
      ${notificationsCard}
      ${metricTiles}
      <div class="card" style="margin-bottom:16px">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:10px">
          <div>
            <div style="font-size:16px;font-weight:800;color:#1F3864">Exam Sets</div>
          <div style="font-size:12px;color:#666">${root._activeQuestionSet ? `Default exam: ${root._activeQuestionSet.name}` : 'No default exam set configured yet'}</div>
        </div>
        <div style="font-size:12px;color:#666">Manage exams, upload new banks, and assign a set per code.</div>
        </div>
        <div style="overflow-x:auto">
          <table class="admin-table">
            <thead>
              <tr>
                <th>Name</th><th style="text-align:center">Questions</th><th style="text-align:center">Version</th><th style="text-align:center">Status</th><th style="text-align:center">Delivered</th><th style="text-align:center">Duration</th><th style="text-align:center">Pass</th><th style="text-align:center">Mode</th><th style="text-align:center">Proctor</th><th style="text-align:center">Actions</th>
              </tr>
            </thead>
            <tbody>${setRows || '<tr><td colspan="10" style="text-align:center;color:#888;padding:18px">No exam sets found</td></tr>'}</tbody>
          </table>
        </div>
      </div>
      <div class="card" style="margin-bottom:16px">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:10px">
          <div style="font-size:16px;font-weight:800;color:#1F3864">Recent Admin Activity</div>
          <div style="font-size:12px;color:#666">${root._adminAuditEntries.length ? `${root._adminAuditEntries.length} latest events` : 'No audit events yet'}</div>
        </div>
        <div style="overflow-x:auto">
          <table class="admin-table">
            <thead>
              <tr>
                <th>Time</th><th>Action</th><th>Target</th><th>IP</th><th>Details</th>
              </tr>
            </thead>
            <tbody>${
              root._adminAuditEntries.length
                ? root._adminAuditEntries.map((entry) => `
                  <tr>
                    <td style="white-space:nowrap">${entry.createdAt ? _esc(new Date(entry.createdAt).toLocaleString()) : '—'}</td>
                    <td>${_esc(auditActionLabel(entry.action))}</td>
                    <td style="font-family:monospace">${_esc(entry.targetCode || '—')}</td>
                    <td style="font-family:monospace">${_esc(entry.clientIp || '—')}</td>
                    <td>${_esc(entry.details ? JSON.stringify(entry.details) : '—')}</td>
                  </tr>`).join('')
                : '<tr><td colspan="5" style="text-align:center;color:#888;padding:16px">No admin audit activity recorded yet</td></tr>'
            }</tbody>
          </table>
        </div>
      </div>
      <div class="card" style="padding:0;overflow-x:auto">
        <div style="padding:14px 16px 0;font-size:12px;color:#666;display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap">
          <span>Sorted by seat number to make the roster easier to scan.</span>
          ${canAdmin ? `<span style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <button class="btn btn-secondary btn-sm" data-action="selectAllVisibleCodes">Select Visible</button>
            <button class="btn btn-secondary btn-sm" data-action="clearCodeSelection">Clear Selection</button>
            <button class="btn btn-danger btn-sm" data-action="bulkDeleteCodes">Delete Selected (<span id="selected-code-count">${root._selectedCodes.size}</span>)</button>
          </span>` : ''}
        </div>
        <div style="padding:12px 16px 10px;display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">
          <select style="margin:0;font-size:12px;padding:8px 10px" data-action="setExportFilter" data-args="questionSetId,__value__">
            <option value="">All exam sets</option>
            ${root._adminQuestionSets.map((set) => `<option value="${set.id}" ${String(root._exportFilters.questionSetId) === String(set.id) ? 'selected' : ''}>${_esc(set.name)}</option>`).join('')}
          </select>
          <select style="margin:0;font-size:12px;padding:8px 10px" data-action="setExportFilter" data-args="status,__value__">
            <option value="" ${!root._exportFilters.status ? 'selected' : ''}>All statuses</option>
            <option value="unused" ${root._exportFilters.status === 'unused' ? 'selected' : ''}>Unused</option>
            <option value="active" ${root._exportFilters.status === 'active' ? 'selected' : ''}>Active</option>
            <option value="completed" ${root._exportFilters.status === 'completed' ? 'selected' : ''}>Completed</option>
          </select>
          <select style="margin:0;font-size:12px;padding:8px 10px" data-action="setExportFilter" data-args="mode,__value__">
            <option value="" ${!root._exportFilters.mode ? 'selected' : ''}>All modes</option>
            <option value="GRADED" ${root._exportFilters.mode === 'GRADED' ? 'selected' : ''}>Graded only</option>
            <option value="PRACTICE" ${root._exportFilters.mode === 'PRACTICE' ? 'selected' : ''}>Practice only</option>
          </select>
          <input type="date" value="${_esc(root._exportFilters.dateFrom || '')}" style="margin:0;font-size:12px;padding:8px 10px" data-action="setExportFilter" data-args="dateFrom,__value__">
          <input type="date" value="${_esc(root._exportFilters.dateTo || '')}" style="margin:0;font-size:12px;padding:8px 10px" data-action="setExportFilter" data-args="dateTo,__value__">
        </div>
        <table class="admin-table">
          <thead>
            <tr>
              <th style="text-align:center">${canAdmin ? '<input type="checkbox" data-action="toggleAllVisibleCodes" data-args="__checked__">' : ''}</th><th>Code</th><th>Seat</th><th>Exam Set</th><th>Notes</th><th>Status</th><th style="text-align:center">Score</th><th style="text-align:center">Pct</th><th style="text-align:center">Duration</th><th style="text-align:center">Tabs</th><th style="text-align:center">Flags</th><th style="text-align:center">Submitted</th><th style="text-align:center">Actions</th>
            </tr>
          </thead>
          <tbody>${rows || '<tr><td colspan="13" style="text-align:center;color:#888;padding:20px">No access codes found</td></tr>'}</tbody>
        </table>
      </div>
    </div>`);
  }

  // ---- Per-action helpers ----
  function flagsFor(code) {
    const row = root._adminRows.find((r) => r.code === code);
    if (!row || !row.incidents || !row.incidents.length) {
      modal('ℹ️', 'No Flags', 'No incidents are recorded for this access code.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    const body = row.incidents.map((i) => `• ${i.time || ''} ${i.type || ''}${i.detail ? ` — ${i.detail}` : ''}`).join('\n');
    modal('🚨', `Flags for ${code}`, body, [{ label: 'Close', cls: 'btn-primary' }]);
  }

  async function clearStaleSessions() {
    modal('⚠️', 'Clear Stale Sessions', `Clear all stale active sessions older than ${root._adminSystemStatus?.staleSessionMinutes || 30} minutes? This will remove saved in-progress state for those stale entries only.`, [
      { label: 'Clear Stale Sessions', cls: 'btn-danger', action: async () => {
        try {
          const resp = await apiJson('/api/admin/clear-stale-sessions', { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 15000, retries: 0 });
          if (!resp || !resp.ok) throw new Error('clear_failed');
          modal('✅', 'Stale Sessions Cleared', `${resp.clearedCount} stale session(s) were cleared.`, [{ label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }]);
        } catch (_e) {
          modal('❌', 'Clear Failed', 'Could not clear stale sessions.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  async function toggleExamAvailability(enabled) {
    const title = enabled ? 'Open Exams' : 'Close Exams';
    const body = enabled
      ? 'Candidates will be able to enter access codes again.'
      : 'Candidates will be blocked at the access-code screen until exams are turned back on.';
    modal(enabled ? '🟢' : '⛔', title, body, [
      {
        label: enabled ? 'Open Exams' : 'Close Exams',
        cls: enabled ? 'btn-primary' : 'btn-danger',
        action: async () => {
          try {
            await apiJson('/api/admin/exam-availability', {
              method: 'POST',
              body: JSON.stringify({ enabled })
            }, { timeoutMs: 10000, retries: 0 });
            showAdmin();
          } catch (_e) {
            modal('❌', 'Update Failed', 'Could not update exam availability.', [{ label: 'OK', cls: 'btn-primary' }]);
          }
        }
      },
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  async function repairResultSummaries() {
    modal('🛠️', 'Repair Scores', 'This will refill missing score and percentage values from saved result records wherever possible.', [
      {
        label: 'Repair Scores',
        cls: 'btn-primary',
        action: async () => {
          try {
            const resp = await apiJson('/api/admin/results/repair-summaries', {
              method: 'POST',
              body: JSON.stringify({})
            }, { timeoutMs: 20000, retries: 0 });
            modal('✅', 'Repair Complete', `${resp.repaired || 0} completed row(s) were repaired.${resp.skipped ? ` ${resp.skipped} row(s) could not be repaired from historical data.` : ''}`, [
              { label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }
            ]);
          } catch (_e) {
            modal('❌', 'Repair Failed', 'Could not repair score summaries.', [{ label: 'OK', cls: 'btn-primary' }]);
          }
        }
      },
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  async function clearResultSummaries() {
    modal('⚠️', 'Clear All Scores', 'This will blank the score and percentage summary columns for all access codes and completed results. The underlying result JSON remains, but the overview table will show blank scores until repaired.', [
      {
        label: 'Clear All Scores',
        cls: 'btn-danger',
        action: async () => {
          try {
            await apiJson('/api/admin/results/clear-summaries', {
              method: 'POST',
              body: JSON.stringify({})
            }, { timeoutMs: 20000, retries: 0 });
            modal('✅', 'Scores Cleared', 'All summary score fields were cleared.', [
              { label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }
            ]);
          } catch (_e) {
            modal('❌', 'Clear Failed', 'Could not clear the score summaries.', [{ label: 'OK', cls: 'btn-primary' }]);
          }
        }
      },
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  async function deleteCode(code, status) {
    const detail = status === 'completed'
      ? 'This will permanently remove the code, its saved result, and any stored progress.'
      : status === 'active'
        ? 'This will permanently remove the code and any in-progress session.'
        : 'This will permanently remove the unused code.';
    modal('⚠️', 'Delete Access Code', `${detail}\n\nCode: ${code}`, [
      {
        label: 'Delete Code',
        cls: 'btn-danger',
        action: async () => {
          try {
            await apiJson(`/api/admin/codes/${encodeURIComponent(code)}`, { method: 'DELETE' }, { timeoutMs: 12000, retries: 0 });
            showAdmin();
          } catch (_e) {
            modal('❌', 'Delete Failed', 'The access code could not be deleted.', [{ label: 'OK', cls: 'btn-primary' }]);
          }
        }
      },
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  function updateSelectedCodeCount() {
    const el = $('selected-code-count');
    if (el) el.textContent = String(root._selectedCodes.size);
  }
  function toggleCodeSelection(code, checked) {
    if (checked) root._selectedCodes.add(code);
    else root._selectedCodes.delete(code);
    updateSelectedCodeCount();
  }
  function toggleAllVisibleCodes(checked) {
    for (const row of root._adminRows) {
      if (checked) root._selectedCodes.add(row.code);
      else root._selectedCodes.delete(row.code);
    }
    document.querySelectorAll('.code-select').forEach((el) => { el.checked = checked; });
    updateSelectedCodeCount();
  }
  function selectAllVisibleCodes() { toggleAllVisibleCodes(true); }
  function clearCodeSelection() {
    root._selectedCodes.clear();
    document.querySelectorAll('.code-select').forEach((el) => { el.checked = false; });
    updateSelectedCodeCount();
  }
  async function bulkDeleteCodes() {
    const codes = [...root._selectedCodes];
    if (!codes.length) {
      modal('ℹ️', 'No Codes Selected', 'Select one or more access codes first.', [{ label: 'OK', cls: 'btn-primary' }]);
      return;
    }
    const selectedRows = root._adminRows.filter((row) => root._selectedCodes.has(row.code));
    const statusSummary = ['unused', 'active', 'completed'].map((status) => `${selectedRows.filter((row) => row.status === status).length} ${status}`).join('\n');
    modal('⚠️', 'Delete Selected Codes', `Delete ${codes.length} selected access code(s)?\n\n${statusSummary}\n\nThe codes will be removed from the normal admin view and can no longer be used. Historical result records are preserved when the database migration is installed.`, [
      {
        label: 'Delete Selected',
        cls: 'btn-danger',
        action: async () => {
          try {
            const resp = await apiJson('/api/admin/codes/bulk-delete', {
              method: 'POST',
              body: JSON.stringify({ codes })
            }, { timeoutMs: 30000, retries: 0 });
            root._selectedCodes.clear();
            modal('✅', 'Codes Deleted', `${resp.deletedCount || 0} access code(s) were deleted.`, [{ label: 'Refresh', cls: 'btn-primary', action: () => showAdmin() }]);
          } catch (_e) {
            modal('❌', 'Bulk Delete Failed', 'The selected codes could not be deleted.', [{ label: 'OK', cls: 'btn-primary' }]);
          }
        }
      },
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  async function reviewResult(code) {
    render('<div class="admin-wrap"><div style="padding:60px;text-align:center;color:white;font-size:18px">Loading candidate answers...</div></div>');
    try {
      const resp = await apiJson(`/api/admin/results/${encodeURIComponent(code)}/review`, {}, { timeoutMs: 12000, retries: 1 });
      if (!resp || !resp.ok) throw new Error('review_failed');
      if (!resp.reviewAvailable) {
        modal('ℹ️', 'Review Not Available', 'This completed exam does not include per-question answer detail. It was likely submitted before answer review was added.', [
          { label: 'Back to Admin', cls: 'btn-primary', action: () => showAdmin() }
        ]);
        return;
      }
      const result = resp.result || {};
      const questionResults = Array.isArray(result.questionResults) ? result.questionResults : [];
      const rows = questionResults.map((item, idx) => {
        const opts = Array.isArray(item.opts) ? item.opts : [];
        const formatIndexes = (indexes) => {
          if (!Array.isArray(indexes) || !indexes.length) return 'No answer selected';
          return indexes.map((originalIdx) => `${originalIdx + 1}. ${opts[originalIdx] || `Option ${originalIdx + 1}`}`).join('<br>');
        };
        return `
          <tr>
            <td style="text-align:center">${idx + 1}</td>
            <td>
              <div style="font-weight:700;color:#1F3864">${_esc(item.stem || 'Question')}</div>
              ${item.note ? `<div style="font-size:12px;color:#666;margin-top:4px">${_esc(item.note)}</div>` : ''}
              ${item.sectionName ? `<div style="font-size:11px;color:#7a8ca8;margin-top:6px;text-transform:uppercase;letter-spacing:.04em">${_esc(item.sectionName)}</div>` : ''}
            </td>
            <td style="font-size:12px;line-height:1.6">${formatIndexes(item.given)}</td>
            <td style="font-size:12px;line-height:1.6">${formatIndexes(item.expected)}</td>
            <td style="text-align:center">${item.correct ? '<span class="chip chip-pass">Correct</span>' : '<span class="chip chip-fail">Wrong</span>'}</td>
          </tr>`;
      }).join('');

      render(`<div class="admin-wrap">
        <div class="card" style="margin-bottom:16px">
          <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap">
            <div>
              <div style="font-size:22px;font-weight:800;color:#1F3864">Answer Review</div>
              <div style="font-size:13px;color:#666;margin-top:4px">${_esc(resp.label || code)} · ${_esc(normalizeExamTitle(result.questionSetName || 'Exam'))}</div>
              <div style="font-size:12px;color:#777;margin-top:6px">${result.score ?? '—'} / ${result.total ?? '—'} · ${result.pct == null ? '—' : `${result.pct}%`} · ${result.pass ? 'Passed' : 'Did not pass'}</div>
            </div>
            <div style="display:flex;gap:8px;flex-wrap:wrap">
              ${roleCan('compliance:read') ? `<button class="btn btn-secondary btn-sm" data-action="downloadSignedResultSummary" data-args="${code}">Signed Summary</button>` : ''}
              <button class="btn btn-secondary btn-sm" data-action="showAdmin">← Back to Admin</button>
            </div>
          </div>
        </div>
        <div class="card">
          <div style="overflow-x:auto">
            <table class="admin-table">
              <thead>
                <tr><th style="text-align:center">#</th><th>Question</th><th>Your Answer</th><th>Correct Answer</th><th style="text-align:center">Result</th></tr>
              </thead>
              <tbody>${rows || '<tr><td colspan="5" style="text-align:center;color:#888;padding:18px">No answer detail available</td></tr>'}</tbody>
            </table>
          </div>
        </div>
      </div>`);
    } catch (_e) {
      modal('❌', 'Review Failed', 'Could not load the answer review for that exam.', [{ label: 'Back to Admin', cls: 'btn-primary', action: () => showAdmin() }]);
    }
  }

  async function saveNote(code, val) {
    try {
      await apiJson('/api/admin/note', { method: 'POST', body: JSON.stringify({ code, notes: String(val || '').trim() }) }, { timeoutMs: 8000, retries: 0 });
    } catch (_e) {
      // quiet failure; refresh will show truth
    }
  }

  async function resetCode(code) {
    modal('⚠️', 'Reset Access Code', `Reset code ${code}? This will delete saved progress and any submitted result for that candidate.`, [
      { label: 'Reset Code', cls: 'btn-danger', action: async () => {
        try {
          await apiJson('/api/admin/reset', { method: 'POST', body: JSON.stringify({ code }) }, { timeoutMs: 10000, retries: 0 });
          showAdmin();
        } catch (_e) {
          modal('❌', 'Reset Failed', 'The code could not be reset.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  async function generateCodes() {
    const raw = window.prompt('How many new access codes should be generated?', '10');
    if (!raw) return;
    const count = Number(raw);
    if (!Number.isInteger(count) || count < 1) return;
    try {
      const resp = await apiJson('/api/admin/generate', { method: 'POST', body: JSON.stringify({ count }) }, { timeoutMs: 12000, retries: 0 });
      if (!resp || !resp.ok) throw new Error('generate_failed');
      showAdmin();
    } catch (_e) {
      modal('❌', 'Generate Failed', 'New access codes could not be generated.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  function setExportFilter(key, value) {
    root._exportFilters[key] = String(value || '');
  }

  async function downloadExport() {
    try {
      const params = new URLSearchParams();
      for (const [key, value] of Object.entries(root._exportFilters)) {
        if (value != null && String(value).trim() !== '') params.set(key, String(value).trim());
      }
      const suffix = params.toString() ? `?${params}` : '';
      const resp = await apiFetch(`/api/admin/export.csv${suffix}`, {}, { timeoutMs: 12000, retries: 1 });
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'Academy_Exam_App_Results.csv';
      a.click();
      URL.revokeObjectURL(url);
    } catch (_e) {
      modal('❌', 'Export Failed', 'Could not download the CSV export.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  async function downloadAuditExport() {
    try {
      const resp = await apiFetch('/api/admin/audit/export.json?limit=1000', {}, { timeoutMs: 12000, retries: 1 });
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'academy_exam_audit_export.json';
      a.click();
      URL.revokeObjectURL(url);
    } catch (_e) {
      modal('❌', 'Audit Export Failed', 'Could not download the signed audit export.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  async function downloadSignedResultSummary(code) {
    try {
      const resp = await apiFetch(`/api/admin/results/${encodeURIComponent(code)}/signed-summary`, {}, { timeoutMs: 12000, retries: 1 });
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `signed_result_${code}.json`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (_e) {
      modal('❌', 'Signed Summary Failed', 'Could not download the signed result summary.', [{ label: 'OK', cls: 'btn-primary' }]);
    }
  }

  root.IE = root.IE || {};
  root.IE.admin = {
    showAdmin: showAdmin,
    reviewResult: reviewResult,
    summaryValue: summaryValue,
    sortAdminRows: sortAdminRows,
    seatSortValue: seatSortValue,
    flagsFor: flagsFor,
    clearStaleSessions: clearStaleSessions,
    toggleExamAvailability: toggleExamAvailability,
    repairResultSummaries: repairResultSummaries,
    clearResultSummaries: clearResultSummaries,
    deleteCode: deleteCode,
    updateSelectedCodeCount: updateSelectedCodeCount,
    toggleCodeSelection: toggleCodeSelection,
    selectAllVisibleCodes: selectAllVisibleCodes,
    clearCodeSelection: clearCodeSelection,
    bulkDeleteCodes: bulkDeleteCodes,
    saveNote: saveNote,
    resetCode: resetCode,
    generateCodes: generateCodes,
    setExportFilter: setExportFilter,
    downloadExport: downloadExport,
    downloadAuditExport: downloadAuditExport,
    downloadSignedResultSummary: downloadSignedResultSummary,
    auditActionLabel: auditActionLabel
  };
})(typeof window !== 'undefined' ? window : globalThis);
