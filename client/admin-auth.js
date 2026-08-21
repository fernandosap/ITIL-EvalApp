/* eslint-disable no-console */
// client/admin-auth.js — admin login / logout / session-revoke and
// the cookie-based XSUAA bootstrap. Reads from window._adminToken and
// window._adminRole; mutates them on success.
//
// Public surface (attached to window.IE.adminAuth):
//   showAdminLogin, doLogin, tryBootstrapFromCookie, logoutAdmin,
//   revokeAdminSessions

(function (root) {
  const { $, render, modal, _sha256, apiJson } = root.IE.util;
  const S = root.S;

  async function showAdminLogin() {
    S.screen = 'admin-login';
    document.body.classList.remove('exam-bg');
    // Probe the server to learn which auth methods are available. XSUAA
    // takes precedence when bound; the password form is shown as a
    // fallback only when it's also enabled.
    let methods = { password: false, xsuaa: { enabled: false } };
    try {
      const r = await fetch('/api/admin/auth-methods', { credentials: 'same-origin' });
      if (r.ok) methods = await r.json();
    } catch (_e) {
      // If we can't reach the server, fall through to the password form.
    }
    // XSUAA button shows a spinner on click because the redirect to the
    // IdP is server-side (302) and can take a few seconds. Without the
    // spinner the user thinks the click did nothing and clicks again,
    // which sometimes starts a second OAuth flow that races the first.
    const xsuaaBtn = methods.xsuaa && methods.xsuaa.enabled
      ? `<a class="btn btn-primary btn-full" id="xsuaa-login" href="${methods.xsuaa.authorizeUrl || '/oauth/login'}" data-action="startXsuaaLogin" data-args="__el__">Sign in with SAP</a>
         <div style="text-align:center;color:#888;font-size:12px;margin:14px 0">or</div>`
      : '';
    const pwdBlock = methods.password
      ? `<label class="label">Password</label>
         <input type="password" id="pwd" placeholder="Admin password" autocomplete="off" data-enter-action="doLogin">
         <button class="btn btn-secondary btn-full" data-action="doLogin">Access Console</button>`
      : (!methods.xsuaa || !methods.xsuaa.enabled
          ? `<p style="color:#a00;font-size:13px">No admin authentication is configured on this server. Set ADMIN_HASH or bind XSUAA.</p>`
          : '');
    render(`<div class="screen" style="max-width:380px">
      <div class="card" style="margin-top:80px">
        <h2>Admin Access</h2>
        <p style="margin-bottom:18px;color:#666;font-size:14px">Proctor console — restricted access.</p>
        ${xsuaaBtn}
        ${pwdBlock}
        <button class="btn btn-secondary btn-full" style="margin-top:18px" data-action="showCodeEntry">← Back</button>
      </div>
    </div>`);
  }

  async function doLogin() {
    const entered = String($('pwd')?.value || '');
    const hash = await _sha256(entered);
    let resp;
    try {
      resp = await apiJson('/api/admin/login', { method: 'POST', body: JSON.stringify({ hash }) }, { timeoutMs: 10000, retries: 0 });
    } catch (_e) {
      resp = null;
    }
    if (!resp || !resp.ok || !resp.token) {
      modal('❌', 'Incorrect Password', 'The password you entered is incorrect, or the server could not verify it.', [{ label: 'Try Again', cls: 'btn-primary' }]);
      return;
    }
    root._adminToken = resp.token;
    root._adminAuthMethod = 'token';
    root._adminRole = resp.role || 'admin';
    if (root.IE.admin && typeof root.IE.admin.showAdmin === 'function') {
      root.IE.admin.showAdmin();
    }
  }

  // Show a spinner inside the "Sign in with SAP" button while the
  // 302 to the XSUAA /oauth/authorize endpoint is in flight. The
  // browser will navigate away within ~1s on a healthy network, but
  // on a slow link or a transient 5xx the user could otherwise click
  // the button again, starting a second OAuth flow. After ~6s we
  // re-enable the button so a hard failure (IdP down) doesn't leave
  // the user stuck.
  function startXsuaaLogin(anchor) {
    if (!anchor) return;
    if (anchor.dataset.loading === '1') return;  // already in flight
    anchor.dataset.loading = '1';
    const original = anchor.innerHTML;
    anchor.innerHTML = '<span class="spinner" aria-hidden="true"></span> Redirecting to SAP...';
    anchor.style.pointerEvents = 'none';
    setTimeout(() => {
      // If we're still here after 6s, the navigation didn't happen.
      // Restore the button so the user can try again.
      if (anchor.dataset.loading === '1') {
        anchor.innerHTML = original;
        anchor.style.pointerEvents = '';
        delete anchor.dataset.loading;
      }
    }, 6000);
  }

  // Bootstrap an admin session from the XSUAA cookie, if one is set.
  // The /oauth/callback redirect lands the browser back at the SPA
  // with the xsuaa_jwt cookie set — we call /api/admin/me to learn
  // the role and jump straight into the admin console. Returns true
  // if a session was detected.
  async function tryBootstrapFromCookie() {
    try {
      const r = await fetch('/api/admin/me', { credentials: 'same-origin' });
      if (!r.ok) return false;
      const data = await r.json();
      if (!data || !data.ok) return false;
      root._adminRole = data.role || 'admin';
      root._adminAuthMethod = data.authMethod || 'token';
      root._adminToken = null;  // rely on cookie
      return true;
    } catch (_e) {
      return false;
    }
  }

  function logoutAdmin() {
    modal('↩️', 'Logout', 'End this admin session on this browser?', [
      { label: 'Logout', cls: 'btn-primary', action: async () => {
        try {
          await apiJson('/api/admin/logout', { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 8000, retries: 0 });
        } catch (_e) {
          // local token clear still happens
        }
        root._adminToken = null;
        root._adminAuthMethod = 'token';
        root._adminRole = 'admin';
        showAdminLogin();
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  function revokeAdminSessions() {
    modal('⚠️', 'Revoke All Sessions', 'Force every existing admin or manager token to expire now? Your current browser will also need a fresh login.', [
      { label: 'Revoke All', cls: 'btn-danger', action: async () => {
        try {
          await apiJson('/api/admin/sessions/revoke-all', { method: 'POST', body: JSON.stringify({}) }, { timeoutMs: 10000, retries: 0 });
          modal('✅', 'Sessions Revoked', 'All existing admin/manager tokens are now invalid.', [{ label: 'OK', cls: 'btn-primary', action: () => {
            root._adminToken = null;
            root._adminAuthMethod = 'token';
            showAdminLogin();
          }}]);
        } catch (_e) {
          modal('❌', 'Revoke Failed', 'Could not revoke all admin sessions.', [{ label: 'OK', cls: 'btn-primary' }]);
        }
      }},
      { label: 'Cancel', cls: 'btn-secondary' }
    ]);
  }

  root.IE = root.IE || {};
  root.IE.adminAuth = {
    showAdminLogin: showAdminLogin,
    doLogin: doLogin,
    startXsuaaLogin: startXsuaaLogin,
    tryBootstrapFromCookie: tryBootstrapFromCookie,
    logoutAdmin: logoutAdmin,
    revokeAdminSessions: revokeAdminSessions
  };
})(typeof window !== 'undefined' ? window : globalThis);
