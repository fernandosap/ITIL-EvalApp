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
//   - requireAdminWriteRate(opts) — per-(role+IP) rate limit on
//                              state-changing admin requests.
//                              Defense in depth against a leaked token
//                              (or a fat-fingered loop in the admin
//                              console) hammering the DB.

function createAuthMiddleware(deps) {
  const {
    tryXsuaaAuth,
    readXsuaaSession,
    parseAdminToken,
    getAdminTokenNotBefore,
    withDb,
    hasDbConfig,
    getXsuaaConfig,
    hasPermission,
    log,
    checkRateLimit,
    getClientIp
  } = deps;

  if (typeof tryXsuaaAuth !== 'function') throw new Error('middleware: tryXsuaaAuth is required');
  if (typeof parseAdminToken !== 'function') throw new Error('middleware: parseAdminToken is required');
  if (typeof hasPermission !== 'function') throw new Error('middleware: hasPermission is required');

  async function restoreXsuaaSessionAuth(req) {
    if (req.xsuaaSessionAuth?.token && req.xsuaaSessionAuth?.role) return req.xsuaaSessionAuth;
    if (typeof readXsuaaSession !== 'function') return null;
    const cookieHeader = String(req.headers?.cookie || '');
    const match = cookieHeader.match(/(?:^|;\s*)xsuaa_session=([^;]+)/);
    const sessionId = match?.[1] ? decodeURIComponent(match[1]) : '';
    if (!sessionId) return null;
    try {
      const session = await readXsuaaSession(sessionId);
      if (!session?.token || !session?.role) {
        if (typeof log === 'function') {
          log('warn', 'xsuaa_session_restore_miss', {
            requestId: req.requestId,
            path: req.path,
            hasCookie: true
          });
        }
        return null;
      }
      req.xsuaaSessionAuth = session;
      req.headers.authorization = `Bearer ${session.token}`;
      return session;
    } catch (err) {
      if (typeof log === 'function') {
        log('error', 'xsuaa_session_restore_failed', {
          requestId: req.requestId,
          path: req.path,
          message: err.message
        });
      }
      return null;
    }
  }

  async function requireAdmin(req, res, next) {
    // XSUAA Bearer token (when VCAP_SERVICES has the xsuaa binding)
    let xsuaaAuth = tryXsuaaAuth(req);
    if (!xsuaaAuth) {
      await restoreXsuaaSessionAuth(req);
      xsuaaAuth = tryXsuaaAuth(req);
    }
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
    // Strict role gate: only requests whose req.adminRole matches `role`
    // get through. This used to be a no-op for any role other than
    // 'admin' (silently allowed everyone), which is a footgun: someone
    // calling requireAdminRole('manager') expecting "only managers" was
    // actually getting "everyone". Now it does what the name says.
    return (req, res, next) => {
      if (req.adminRole !== role) {
        return res.status(403).json({ error: 'role_required', requiredRole: role });
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

  // Per-IP rate limit on state-changing admin requests.
  // Use AFTER requireAdmin so req.adminRole is set. Defaults:
  // 60 requests / 60s window. GETs should NOT use this — it's
  // for writes (POST/PUT/DELETE) where a runaway loop in the
  // console or a leaked token could hammer HANA.
  //
  // Bucket is 'admin_write', keyed by client IP only. The
  // intent is to bound backend load, so two admins at the
  // same source IP share a budget (deliberate — see AGENTS.md
  // gotcha #20). In BTP production this means all admin
  // console users coming from a single corporate egress IP
  // share the 60/min budget, which is the intended blast-
  // radius control.
  function requireAdminWriteRate(opts) {
    const max = (opts && Number.isInteger(opts.max)) ? opts.max : 60;
    const windowMs = (opts && Number.isInteger(opts.windowMs)) ? opts.windowMs : 60 * 1000;
    if (typeof checkRateLimit !== 'function') {
      throw new Error('middleware: checkRateLimit dependency is required for requireAdminWriteRate');
    }
    return (req, res, next) => {
      const ip = typeof getClientIp === 'function' ? getClientIp(req) : (req.ip || 'unknown');
      if (!checkRateLimit('admin_write', String(ip), max, windowMs)) {
        if (typeof log === 'function') {
          log('warn', 'admin_write_rate_limited', {
            requestId: req.requestId,
            role: req.adminRole,
            clientIp: ip,
            path: req.path,
            method: req.method
          });
        }
        res.setHeader('Retry-After', Math.ceil(windowMs / 1000));
        return res.status(429).json({ error: 'too_many_writes' });
      }
      next();
    };
  }

  return { requireAdmin, requireAdminRole, requirePermission, requireAdminWriteRate };
}

module.exports = { createAuthMiddleware };
