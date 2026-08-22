/* eslint-disable no-console */
// client/util.js — pure DOM and API helpers.

(function (root) {
  function $(id) { return document.getElementById(id); }
  function render(html) { $('app').innerHTML = html; }
  function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
  function _esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }
  function fmt(secs) {
    const s = Math.max(0, secs);
    return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
  }
  function durationLabel(totalSecs) {
    const m = Math.floor(totalSecs / 60);
    const s = totalSecs % 60;
    return `${m}m ${String(s).padStart(2, '0')}s`;
  }
  function brandLockup(title, subtitle) {
    if (subtitle == null) subtitle = 'Academy assessment workspace';
    return `<div class="brand-hero">
      <div class="brand-lockup">
        <div class="brand-grid" aria-hidden="true">
          <span class="brand-grid-mark"></span>
          <span class="brand-grid-mark"></span>
          <span class="brand-grid-mark"></span>
          <span class="brand-grid-mark"></span>
        </div>
        <div class="brand-copy">
          <div class="brand-eyebrow">Academy Exam App</div>
          <div class="brand-title">${_esc(title)}</div>
          <div class="brand-subtitle">${_esc(subtitle)}</div>
        </div>
      </div>
    </div>`;
  }

  function connectivityBanner() {
    if (typeof navigator === 'undefined' || navigator.onLine !== false) return '';
    return `<div role="status" aria-live="polite" style="background:#fff3cd;color:#664d03;border-bottom:1px solid #ffecb5;padding:8px 16px;text-align:center;font-size:12px;font-weight:700">You are offline. Your answers stay in this browser and will retry when the connection returns.</div>`;
  }

  function normalizeExamTitle(value) { return window.SharedConstants.normalizeExamTitle(value); }
  function roleCan(permission) { return window.SharedConstants.hasPermission(String(window._adminRole || 'admin'), permission); }

  async function _sha256(str) {
    const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
    return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, '0')).join('');
  }

  function errorMessage(err, fallback) {
    if (!err) return fallback;
    if (err.name === 'AbortError') return 'Request timed out';
    if (err.message) return err.message;
    return fallback;
  }

  function setSavePill(cls, text) {
    root.SAVE_UI = root.SAVE_UI || { cls: 'save-pill', text: 'Saved' };
    root.SAVE_UI.cls = cls;
    root.SAVE_UI.text = text;
    const el = $('save-pill');
    if (el) { el.className = cls; el.textContent = text; }
  }
  function markSaveStart() { setSavePill('save-pill saving', 'Saving...'); }
  function markRetry(attempt) { setSavePill('save-pill retry', `Retry ${attempt}`); }
  function markSaveDone(ok) { setSavePill(ok ? 'save-pill' : 'save-pill error', ok ? 'Saved' : 'Save failed'); }
  function markOfflineSave() { setSavePill('save-pill error', 'Offline'); }

  root._questionSetVersions = root._questionSetVersions || {};

  function questionSetId(url) {
    const match = String(url || '').match(/^\/api\/admin\/question-sets\/(\d+)(?:\/|$)/);
    return match ? match[1] : null;
  }

  function rememberVersion(url, resp) {
    const id = questionSetId(url);
    if (!id || !resp || !resp.headers) return;
    const raw = resp.headers.get('x-resource-version') || resp.headers.get('etag') || '';
    const version = String(raw).trim().replace(/^W\//, '').replace(/^"|"$/g, '');
    if (version) root._questionSetVersions[id] = version;
  }

  async function hydrateQuestionSetVersion(id, headers, signal) {
    if (!id || root._questionSetVersions[id]) return root._questionSetVersions[id] || null;
    try {
      const probeHeaders = Object.assign({}, headers || {});
      delete probeHeaders['If-Match'];
      const resp = await fetch(`/api/admin/question-sets/${id}/readiness`, { method: 'GET', headers: probeHeaders, signal: signal });
      if (resp.ok) rememberVersion(`/api/admin/question-sets/${id}/readiness`, resp);
    } catch (_e) { /* the mutation will return a useful 428 if probing failed */ }
    return root._questionSetVersions[id] || null;
  }

  async function apiFetch(url, opts, cfg) {
    opts = opts || {};
    cfg = cfg || {};
    const method = String(opts.method || 'GET').toUpperCase();
    const retries = Number.isInteger(cfg.retries) ? cfg.retries : 1;
    const timeoutMs = cfg.timeoutMs || (root.API_TIMEOUT_MS || 12000);
    const baseRetryMs = root.API_BASE_RETRY_MS || 600;
    const isSave = !!cfg.isSave;

    if (isSave) markSaveStart();
    let lastErr = null;
    for (let attempt = 0; attempt <= retries; attempt++) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const headers = Object.assign({}, opts.headers || {});
        if (root._adminToken) headers['X-Admin-Token'] = root._adminToken;
        if (root.S && root.S.examToken) headers['X-Exam-Token'] = root.S.examToken;

        const setId = questionSetId(url);
        if (setId && ['POST', 'PUT', 'PATCH', 'DELETE'].includes(method) && !String(url).endsWith('/clone')) {
          const version = await hydrateQuestionSetVersion(setId, headers, controller.signal);
          if (version) headers['If-Match'] = `"${version}"`;
        }

        const resp = await fetch(url, Object.assign({}, opts, { headers, signal: controller.signal }));
        clearTimeout(timer);
        rememberVersion(url, resp);
        if (!resp.ok) {
          let detail = `HTTP ${resp.status}`;
          try {
            const ct = (resp.headers.get('content-type') || '').toLowerCase();
            if (ct.includes('application/json')) {
              const body = await resp.json();
              detail = body?.message
                || (Array.isArray(body?.errors) && body.errors.length ? body.errors[0] : null)
                || body?.error
                || detail;
            } else {
              const text = await resp.text();
              if (text) detail = text;
            }
          } catch (_e) { /* ignore parse failure */ }
          if (setId && (resp.status === 412 || resp.status === 428)) delete root._questionSetVersions[setId];
          const e = new Error(detail);
          e.status = resp.status;
          throw e;
        }
        if (isSave) markSaveDone(true);
        return resp;
      } catch (err) {
        clearTimeout(timer);
        lastErr = err;
        const status = err && err.status ? err.status : 0;
        const retryable = err.name === 'AbortError' || status === 0 || status === 429 || status >= 500;
        if (attempt < retries && retryable) {
          if (isSave) markRetry(attempt + 1);
          await sleep(baseRetryMs * Math.pow(2, attempt));
          continue;
        }
        if (isSave) markSaveDone(false);
        throw new Error(errorMessage(err, 'API request failed'));
      }
    }
    if (isSave) markSaveDone(false);
    throw new Error(errorMessage(lastErr, 'API request failed'));
  }

  async function apiJson(url, opts, cfg) {
    opts = opts || {};
    const headers = Object.assign({}, opts.headers || {});
    if (opts.body && !headers['Content-Type']) headers['Content-Type'] = 'application/json';
    const resp = await apiFetch(url, Object.assign({}, opts, { headers }), cfg);
    const ct = (resp.headers.get('content-type') || '').toLowerCase();
    if (!ct.includes('application/json')) return null;
    return resp.json();
  }

  function modal(icon, title, body, btns) {
    $('m-icon').textContent = icon;
    $('m-title').textContent = title;
    $('m-body').textContent = body;
    $('m-btns').innerHTML = btns.map((b, i) =>
      `<button class="btn ${b.cls || 'btn-primary'}" id="mb-${i}">${_esc(b.label)}</button>`
    ).join('');
    btns.forEach((b, i) => {
      const el = $(`mb-${i}`);
      if (!el) return;
      el.onclick = () => {
        $('modal').classList.remove('show');
        if (b.action) b.action();
      };
    });
    $('modal').classList.add('show');
  }

  function reportClientError(type, info) {
    if (root.IE && root.IE.reporter && typeof root.IE.reporter.report === 'function') {
      return root.IE.reporter.report(type, info);
    }
    return false;
  }

  root.IE = root.IE || {};
  root.IE.util = {
    $,
    render,
    sleep,
    _esc,
    fmt,
    durationLabel,
    brandLockup,
    connectivityBanner,
    normalizeExamTitle,
    roleCan,
    _sha256,
    errorMessage,
    setSavePill,
    markSaveStart,
    markRetry,
    markSaveDone,
    markOfflineSave,
    apiFetch,
    apiJson,
    modal,
    reportClientError,
    questionSetId,
    rememberVersion
  };
})(typeof window !== 'undefined' ? window : globalThis);