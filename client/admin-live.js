/* eslint-disable no-console */
(function (root) {
  let panel = null;

  function esc(value) {
    if (root.IE?.util?._esc) return root.IE.util._esc(String(value ?? ''));
    return String(value ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function api(path) {
    if (root.IE?.util?.apiJson) return root.IE.util.apiJson(path);
    return fetch(path, { credentials: 'same-origin' }).then(async (r) => {
      const body = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(body.error || `HTTP ${r.status}`);
      return body;
    });
  }

  function ensureButton() {
    const existing = document.getElementById('admin-live-launcher');
    // Never expose operations affordances on the unauthenticated login page.
    if (!root.S || root.S.screen !== 'admin') {
      if (existing) existing.remove();
      return;
    }
    if (existing) return;
    const btn = document.createElement('button');
    btn.id = 'admin-live-launcher';
    btn.type = 'button';
    btn.textContent = 'Live Sessions';
    btn.style.cssText = 'position:fixed;right:20px;bottom:20px;z-index:9000;padding:10px 16px;border-radius:999px;border:0;background:#0a6ed1;color:white;font-weight:700;box-shadow:0 4px 18px rgba(0,0,0,.2);cursor:pointer';
    btn.addEventListener('click', openPanel);
    document.body.appendChild(btn);
  }

  function closePanel() {
    if (panel) panel.remove();
    panel = null;
  }

  function shell(title) {
    closePanel();
    panel = document.createElement('div');
    panel.style.cssText = 'position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.35);display:flex;align-items:center;justify-content:center;padding:24px';
    panel.innerHTML = `<div style="width:min(980px,96vw);max-height:88vh;overflow:auto;background:white;border-radius:16px;padding:22px;box-shadow:0 24px 70px rgba(0,0,0,.25)"><div style="display:flex;justify-content:space-between;gap:16px;align-items:center;margin-bottom:16px"><h2 style="margin:0;color:#1d2d3e">${esc(title)}</h2><button id="admin-live-close" class="btn">Close</button></div><div id="admin-live-body">Loading…</div></div>`;
    document.body.appendChild(panel);
    panel.querySelector('#admin-live-close').addEventListener('click', closePanel);
    panel.addEventListener('click', (e) => { if (e.target === panel) closePanel(); });
    return panel.querySelector('#admin-live-body');
  }

  async function showTimeline(code) {
    const body = shell(`Proctor Timeline · ${code}`);
    try {
      const data = await api(`/api/admin/proctor/incidents/${encodeURIComponent(code)}`);
      if (!data.incidents?.length) {
        body.innerHTML = '<p>No server-side incidents recorded for this exam.</p>';
        return;
      }
      body.innerHTML = `<div style="display:grid;gap:10px">${data.incidents.map((i) => `<div style="border:1px solid #d9e2ec;border-radius:10px;padding:12px"><div style="font-weight:700">${esc(i.type)}</div><div style="color:#52606d;margin-top:4px">${esc(i.detail || '')}</div><div style="font-size:12px;color:#7b8794;margin-top:6px">${esc(i.serverTime || i.clientTime || '')}</div></div>`).join('')}</div>`;
    } catch (err) {
      body.innerHTML = `<p>Could not load timeline: ${esc(err.message)}</p>`;
    }
  }

  async function openPanel() {
    if (!root.S || root.S.screen !== 'admin') return;
    const body = shell('Live Exam Sessions');
    try {
      const data = await api('/api/admin/live-sessions');
      if (!data.sessions?.length) {
        body.innerHTML = '<p>No candidates are currently in an active exam session.</p>';
        return;
      }
      body.innerHTML = `<div style="overflow:auto"><table style="width:100%;border-collapse:collapse"><thead><tr><th style="text-align:left;padding:8px">Seat</th><th style="text-align:left;padding:8px">Code</th><th style="text-align:right;padding:8px">Elapsed</th><th style="text-align:right;padding:8px">Tab switches</th><th style="text-align:right;padding:8px">Incidents</th><th style="text-align:left;padding:8px">Last save</th><th></th></tr></thead><tbody>${data.sessions.map((s) => `<tr style="border-top:1px solid #e5eaf0"><td style="padding:8px">${esc(s.label || '—')}</td><td style="padding:8px;font-family:monospace">${esc(s.code)}</td><td style="padding:8px;text-align:right">${Math.floor(Number(s.elapsedMs || 0) / 60000)}m</td><td style="padding:8px;text-align:right">${Number(s.tabSwitches || 0)}</td><td style="padding:8px;text-align:right">${Number(s.incidentCount || 0)}</td><td style="padding:8px">${esc(s.lastSaveAt || '—')}</td><td style="padding:8px"><button class="btn admin-live-timeline" data-code="${esc(s.code)}">Timeline</button></td></tr>`).join('')}</tbody></table></div>`;
      body.querySelectorAll('.admin-live-timeline').forEach((btn) => btn.addEventListener('click', () => showTimeline(btn.dataset.code)));
    } catch (err) {
      body.innerHTML = `<p>Could not load live sessions: ${esc(err.message)}</p>`;
    }
  }

  const observer = new MutationObserver(ensureButton);
  observer.observe(document.documentElement, { childList: true, subtree: true });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', ensureButton);
  else ensureButton();

  root.IE = root.IE || {};
  root.IE.adminLive = { openPanel, showTimeline, ensureButton };
})(typeof window !== 'undefined' ? window : globalThis);
