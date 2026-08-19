'use strict';

// Admin auth middlewares. Factored out of server.js so the auth path
// (tryXsuaaAuth → parseAdminToken → DB-backed revocation check) is
// testable in isolation and doesn't drag in 3000+ lines of route code.
//
// Returns:
//   - requireAdmin           — accepts XSUAA Bearer / xsuaa_jwt cookie
//                              OR the legacy SHA-256 X-Admin-Token.
//   - requireAdminRole(role) — only pass through if req.adminRole === role.
//                              Stricter than requirePermission: this is
//                              role identity, not capability.
//   - requirePermission(p)   — capability check via shared/constants.js
//                              ROLE_PERMISSIONS table.

function createAuthMiddleware(deps) {
  const {
    tryXsuaaAuth,
    parseAdminToken,
    getAdminTokenNotBefore,
    withDb,
    hasDbConfig,
    getXsuaaConfig,
    hasPermission,
    log
  } = deps;

  if (typeof tryXsuaaAuth !== 'function') throw new Error('middleware: tryXsuaaAuth is required');
  if (typeof parseAdminToken !== 'function') throw new Error('middleware: parseAdminToken is required');
  if (typeof hasPermission !== 'function') throw new Error('middleware: hasPermission is required');

  async function requireAdmin(req, res, next) {
    // XSUAA Bearer token (when VCAP_SERVICES has the xsuaa binding)
    const xsuaaAuth = tryXsuaaAuth(req);
    if (xsuaaAuth) {
      req.adminRole = xsuaaAuth.role;
      req.adminSubject = xsuaaAuth.sub;
      req.authMethod = 'xsuaa';
      return next();
    }

    // Legacy SHA-256 token path (local dev or until XSUAA is rolled out to all admins)
    const token = String(req.headers['x-admin-token'] || '').trim();
    const parsed = parseAdminToken(token);
    if (!parsed) {
      return res.status(401).json({
        error: 'unauthorized',
        hint: getXsuaaConfig && getXsuaaConfig()
          ? 'Provide an Authorization: Bearer <jwt> from the bound XSUAA service.'
          : 'Provide X-Admin-Token (SHA-256 role token) or bind XSUAA.'
      });
    }
    try {
      const revokedBefore = hasDbConfig
        ? await withDb(async (conn) => getAdminTokenNotBefore(conn))
        : 0;
      if (parsed.issuedAt && revokedBefore && parsed.issuedAt < revokedBefore) {
        return res.status(401).json({ error: 'session_revoked' });
      }
      req.adminRole = parsed.role;
      req.authMethod = 'token';
      next();
    } catch (err) {
      if (typeof log === 'function') {
        log('error', 'admin_auth_failed', { requestId: req.requestId, message: err.message });
      }
      res.status(500).json({ error: 'admin_auth_failed' });
    }
  }

  function requireAdminRole(role) {
    return (req, res, next) => {
      if (role === 'admin' && req.adminRole !== 'admin') {
        return res.status(403).json({ error: 'admin_role_required' });
      }
      next();
    };
  }

  function requirePermission(permission) {
    return (req, res, next) => {
      if (!hasPermission(req.adminRole, permission)) {
        return res.status(403).json({ error: 'forbidden', permission });
      }
      next();
    };
  }

  return { requireAdmin, requireAdminRole, requirePermission };
}

module.exports = { createAuthMiddleware };
