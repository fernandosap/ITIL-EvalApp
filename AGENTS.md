# AGENTS.md — ITIL-EvalApp

> Living document for AI agents and humans working on this app.
> Last inspected: 2026-08-18 against HANA prod `be84eee8-9540-4517-be90-a3267f32084a.hna1.prod-us10.hanacloud.ondemand.com` (schema `ITIL_EXAM`).

## What this is

**Academy Exam App** — a SAP BTP / Cloud Foundry application that delivers a proctored
ITIL 4–style exam to candidates and lets admins manage question banks, generate access
codes, and review results.

- **Live route**: `https://academycd-evalapp.cfapps.us10.hana.ondemand.com`
- **Public hostname** is hard-coded in `manifest.yml` and `deploy_btp.sh` (override via `ROUTE_HOST`)
- **App name in CF**: `itil4-evalapp`
- **Production revision pinned in `.env`**: `a0dae34-dirty` (April 11, 2026 — note: `.env` is local dev only; CF env vars are authoritative)
- **HEAD commit at inspection time**: `651dc02` — "fix(server): serve /client/*.js modules for the refactored SPA"

## Stack

- Node 20 (engines pinned in `package.json`)
- Express 4.21
- `@sap/hana-client` 2.28 (lockfile target 2.25 — minor drift, OK)
- Vanilla JS frontend (no bundler, no framework) — see the
  `client/` directory for the SPA, served as plain `<script>` tags
  from `index.html`
- SAP HANA Cloud, schema `ITIL_EXAM`
- Anthropic API for proctor image analysis (`claude-sonnet-4-20250514`)

## File map

### Server + shared + lib (Node)

| File | Lines | Role |
|---|---:|---|
| `server.js` | ~3,720 | API + admin + HANA queries + token signing + metrics |
| `lib/audit.js` | ~150 | `tryWriteAdminAudit()` + `getMetrics()` for compliance visibility (counts attempts, writes, failures, last failure message) |
| `lib/middleware.js` | ~95 | `createAuthMiddleware({...})` factory → `requireAdmin` / `requireAdminRole` / `requirePermission` |
| `lib/rate-limit.js` | ~55 | In-memory sliding-window `checkRateLimit(bucket, key, max, windowMs)` + `peekRateLimit` (no-op increment) |
| `lib/responses.js` | ~95 | `jsonError` / `jsonOk` / `toCsvCell` / `toCsvRow` / `parseJsonOrNull` / `parseAnthropicText` / `buildSignedEnvelope` / `verifySignedEnvelope` (HMAC-SHA256) |
| `shared/constants.js` | ~135 | UMD — `normalizeExamTitle` + `ROLE_PERMISSIONS` + `hasPermission` + `ROLES` + `CODE_STATUS` + `QUESTION_SET_LIFECYCLE` + `EXAM_MODE` + `AUDIT_ACTION` |
| `shared/scoring.js` | 120 | `makePRNG`, `seededShuffle`, `buildOrdering`, `pickQuestionsForSession`, `gradeExamFromSession` |
| `shared/xsuaa.js` | ~270 | XSUAA helpers: VCAP parsing, JWT verify (RS256, no `@sap/xssec`), scope→role, `buildAuthorizeUrl`, `exchangeCodeForToken`, `parseCookieHeader` |
| `shared/db-pool.js` | ~120 | Opt-in HANA connection pool via `HANA_POOL_SIZE` |
| `index.html` | ~410 | Shell, all CSS inline. Loads `client/*.js` in strict order. |
| `migrations/*.sql` | 369 total | 7 idempotent migrations, all currently applied in prod |
| `scripts/check-env.mjs` | 65 | Validates required env vars before deploy |
| `scripts/smoke-test.mjs` | 299 | Boots server, hits HANA real, exercises admin login + code generation |
| `scripts/inspect-hana.mjs` | ~175 | Read-only schema/data dump (overridable via `HANA_PASSWORD=`) |
| `tests/*.test.js` | ~3,200 | 211 unit tests, runs in ~700ms |
| `deploy_btp.sh` | 278 | `cf push` with env-file + SSO support (currently bypassed — see Deployment status) |
| `manifest.yml` | 577 | CF app manifest with `((...))` placeholders for env vars |
| `favicon.svg` | 26 | Brand mark (blue/teal gradient) |
| `.cfignore` | 41 | Excludes `.git/`, `.gitignore`, `.DS_Store`, `node_modules/` |
| `.githooks/pre-commit` | ~110 | Replaces CI: `npm test` + secret scan + `node --check` on every JS file |

### Client SPA (browser)

The SPA is split into 9 modules. Each is a UMD IIFE that attaches to
`window.IE.<name>`. They share state via `window.S`, `window._adminToken`,
etc. (declared in `client/state.js`).

| File | Bytes | Role |
|---|---:|---|
| `client/util.js` | 8K | Pure DOM/API helpers: `$`, `render`, `modal`, `apiJson`, `_esc`, `fmt`, `_sha256`, `setSavePill`, `normalizeExamTitle`, `roleCan` |
| `client/state.js` | 8.5K | Globals (`S`, `_adminToken`, `_adminRows`, etc.) + state mutators: `saveProgress`, `queueProgressSave`, `replayPendingActions`, `fetchStatus`, `logIncident`, `isOnline`, `connectivityBanner`, `proctorEnabled` |
| `client/code-entry.js` | 16K | Pre-exam flow: `showCodeEntry`, `handleCodeSubmit`, `showConsent`, `handleConsentNext`, `showTechCheck`, `reqWebcam`, `reqScreen`, `startExam` |
| `client/exam.js` | 23K | In-exam: `renderQ`, `goToQ`, `prevQ`, `nextQ`, `pick`, `trySubmit`, `startTimer`, `updateTimer`, `submitExam`, `showResultsFromRecord`, `downloadResultSummary`, `statusChip`. Also registers global keydown/contextmenu/selectstart security listeners. |
| `client/proctor.js` | 5K | Proctoring: `setupSecurity`, `teardownSecurity`, `onBlur`, `onFocus`, `onVisChange`, `startProctor`, `proctor`, `refreshConnectivityState` |
| `client/admin-auth.js` | 6K | Login/logout: `showAdminLogin`, `doLogin`, `tryBootstrapFromCookie`, `logoutAdmin`, `revokeAdminSessions` |
| `client/admin-codes.js` | 44K | Admin dashboard: `showAdmin` (the big one — loads 5 endpoints, renders the full console) + per-row actions (`deleteCode`, `bulkDeleteCodes`, `saveNote`, `resetCode`, `generateCodes`, etc.) + exports (`downloadExport`, `downloadAuditExport`, `downloadSignedResultSummary`) |
| `client/admin-question-sets.js` | 53K | Exam set management: `createQuestionSet`, `configQuestionSet`, `activateQuestionSet`, `publishQuestionSet`, `archiveQuestionSet`, `deleteQuestionSet`, `exportQuestionSet`, `cloneQuestionSet`, `rollbackImportedSet` + CSV upload (`parseQuestionCsv`, `analyzeQuestionUpload`, `previewUploadedQuestionSet`, `submitUploadedQuestionSet`) + question/section editor (`openQuestionSet`, `showQuestionEditor`, `editSectionPrompt`, etc.) + per-set analytics (`showQuestionSetAnalytics`) |
| `client/dispatcher.js` | 6K | Event delegation: one click listener + one change listener on `document` route `data-action` → `window.IE.*.<fn>(data-args...)`. Type-coerces args; resolves `__value__`, `__checked__`, and `__el__` sentinels from the element. |
| `client/main.js` | 3K | Entry point: registers `beforeunload` / `online` / `offline` listeners and runs the `DOMContentLoaded` bootstrap (handles `?admin=1` + XSUAA `?auth=ok` cookie bootstrap). |

**Load order** (in `index.html`, strict):

```
shared/constants.js → client/util.js → client/state.js →
client/code-entry.js → client/exam.js → client/proctor.js →
client/admin-auth.js → client/admin-codes.js →
client/admin-question-sets.js → client/dispatcher.js → client/main.js
```

`client/state.js` must run before the others because it defines the
shared globals. `client/dispatcher.js` must run before `client/main.js`
so the click/change listeners are wired up before the DOMContentLoaded
bootstrap fires. `client/main.js` runs last because it depends on every
`window.IE.*` namespace being populated.

**Event delegation (data-action pattern):**

Buttons, selects, checkboxes, and file inputs use `data-action` +
`data-args` instead of inline `onclick="X()"` / `onchange="X()"`.

```html
<button data-action="doLogin">Login</button>
<button data-action="deleteCode" data-args="ABC123,completed">Delete</button>
<select data-action="setExportFilter" data-args="status,__value__">…</select>
<input type="checkbox" data-action="toggleAllVisibleCodes" data-args="__checked__">
```

`data-args` is a CSV. Each cell is type-coerced by
`client/dispatcher.js` `coerce()`:
- `"0"` → `0` (number)
- `"true"` / `"false"` → boolean
- `"null"` → `null`
- everything else → string

Two sentinels read from the element itself at dispatch time:
- `__value__` → `el.value`
- `__checked__` → `el.checked`
- `__el__` → the element itself (e.g. to update its innerHTML while a 302 is in flight, as `startXsuaaLogin` does for the "Sign in with SAP" button)

Click and change listeners are installed once on `document` (capture
phase) in `client/dispatcher.js`. The dispatcher walks `window.IE.*`
and dispatches to the first matching function name. Adding a new
action is just: write the function on a module + use `data-action`.

`onkeydown="if(event.key==='Enter') X()"` stays inline in
`client/code-entry.js` (only 2 sites, no clean declarative form).

## HANA schema (actual, post-migration)

All 7 migrations applied. All runtime-detected columns present.

```
APP_SETTINGS (SETTING_KEY PK, SETTING_VALUE, UPDATED_AT)
ACCESS_CODES (ACCESS_CODE PK(6), LABEL, NOTES, STATUS, SCORE, PCT, PASS,
              QUESTION_SET_ID FK, DELETED_AT, DELETED_BY, CREATED_AT, UPDATED_AT)
EXAM_SESSIONS (ACCESS_CODE PK, SESSION_JSON NCLOB, ELAPSED_MS, TAB_SWITCHES, UPDATED_AT)
EXAM_RESULTS (ACCESS_CODE PK, SCORE, TOTAL, PCT, PASS BOOLEAN, AUTO_SUBMIT,
              DURATION_SECS, TAB_SWITCHES, INCIDENT_COUNT, RESULT_JSON NCLOB, SUBMITTED_AT)
EXAM_QUESTIONS (legacy, see findings) (QUESTION_INDEX PK, STEM, NOTE, OPTS_JSON, ANSWER_JSON, MULTI, UPDATED_AT)
EXAM_INCIDENTS (orphan, see findings) (ID PK, ACCESS_CODE, EVENT_TIME, EVENT_TYPE, DETAIL)
QUESTION_SETS (QUESTION_SET_ID PK, NAME, DESCRIPTION, IS_ACTIVE, DURATION_MINUTES,
              PASS_PCT, PROCTOR_ENABLED, EXAM_MODE, SHOW_CORRECT_ANSWERS,
              COUNTS_TOWARD_RESULTS, NUM_QUESTIONS, VERSION_GROUP_ID, VERSION_NUMBER,
              LIFECYCLE_STATUS, PARENT_QUESTION_SET_ID, IMPORT_SOURCE, CREATED_AT, UPDATED_AT)
QUESTION_SECTIONS (SECTION_ID PK, QUESTION_SET_ID FK CASCADE, NAME, DESCRIPTION, DISPLAY_ORDER, DRAW_COUNT, ...)
QUESTION_SET_QUESTIONS (QUESTION_ID PK, QUESTION_SET_ID FK CASCADE, SECTION_ID FK SET NULL,
                        QUESTION_INDEX, STEM, NOTE, OPTS_JSON NCLOB, ANSWER_JSON NCLOB, MULTI, ...)
ADMIN_AUDIT_LOG (AUDIT_ID PK, ACTION, TARGET_CODE, DETAILS_JSON NCLOB, ACTOR, CLIENT_IP, CREATED_AT)
```

Unique index: `UX_QITEMS_SET_QINDEX` on `QUESTION_SET_QUESTIONS(QUESTION_SET_ID, QUESTION_INDEX)`.

## Current data state (as of 2026-08-18, post-cleanup)

| Table | Rows | Notes |
|---|---:|---|
| `ACCESS_CODES` | 274 | 242 completed, 17 deleted, 15 unused, **0 active** |
| `EXAM_RESULTS` | 242 | 168 PASS (avg 87.07%) + 74 FAIL (avg 64.73%); **all GRADED, zero PRACTICE** |
| `EXAM_SESSIONS` | 0 | clean — stale 7WGME9 was deleted (see cleanup log) |
| `EXAM_QUESTIONS` (legacy) | — | **DROPPED 2026-08-18** (was 30 rows, all migrated to active set) |
| `EXAM_INCIDENTS` | — | **DROPPED 2026-08-18** (was 0 rows, no code path used it) |
| `QUESTION_SETS` | 4 | 1 active (PUBLISHED), 3 ARCHIVED (cleanup normalized drift) |
| `QUESTION_SET_QUESTIONS` | 192 | ~48 per set average |
| `QUESTION_SECTIONS` | 15 | — |
| `ADMIN_AUDIT_LOG` | 1,295 | Last entry: **2026-07-23** (silent in August — see findings) |
| `APP_SETTINGS` | 2 | `EXAMS_ENABLED` (set Mar 31), `ADMIN_TOKEN_NOT_BEFORE` (set May 25) |

## API surface (60+ routes; 4-role auth + XSUAA OAuth)

### Candidate (no auth)
- `POST /api/validate` — validate 6-char access code
- `POST /api/session/start` — start/resume an exam session
- `GET /api/question/:displayIdx` — fetch one question (requires exam token)
- `POST /api/progress` — autosave progress
- `POST /api/submit` — grade and store result
- `POST /api/proctor/check` — send webcam JPEG to Anthropic (rate-limited)
- `GET /api/health`, `GET /api/status` — public health/config
- `GET /api/bootstrap` — **410 Gone by design** (intentionally disabled for security)
- `GET /`, `/client/*.js`, `/shared/constants.js`, `/favicon.svg` — static shell (regex route on `/^\/client\/[A-Za-z0-9_-]+\.js$/`, with path-traversal check)

### Admin (4 roles: admin / manager / reviewer / content_editor)
- `POST /api/admin/login`, `/logout`, `/sessions/revoke-all`
- `GET /api/admin/auth-methods` — public; returns `{ password, xsuaa: { enabled, authorizeUrl, xsappname } }`
- `GET /api/admin/me` — auth; returns `{ ok, role, sub, authMethod }` (used by SPA to bootstrap from cookie)
- `GET /oauth/login` — public; 302 to XSUAA authorize URL, sets `xsuaa_state` cookie
- `GET /oauth/callback` — public; exchanges code, sets `xsuaa_jwt` cookie, 302s to `/?auth=ok`
- `GET /api/admin/system-status`, `/metrics`, `/notifications`, `/audit`, `/audit/export.json`
- `GET /api/admin/codes` (paginated codes + joins)
- `POST /api/admin/generate` (create codes), `POST /api/admin/reset`, `POST /api/admin/note`
- `DELETE /api/admin/codes/:code`, `POST /api/admin/codes/bulk-delete`
- `POST /api/admin/codes/:code/question-set` (assign per code)
- `POST /api/admin/exam-availability` (global on/off)
- `POST /api/admin/clear-stale-sessions`
- `GET /api/admin/sweeper-status` — read-only sweeper state (last tick, stuck flag, total cleared)
- `GET /api/admin/results/:code/review`, `GET /api/admin/results/:code/signed-summary`, `POST /api/admin/results/verify-signature`
- `POST /api/admin/results/repair-summaries`, `POST /api/admin/results/clear-summaries`
- `GET /api/admin/analytics/overview`, `GET /api/admin/question-sets/:id/analytics`
- `GET /api/admin/question-sets`, `POST /api/admin/question-sets` (create), `/clone`, `/publish`, `/archive`, `/activate`, `/config`, `/rollback-import`
- `DELETE /api/admin/question-sets/:id`
- `GET /api/admin/question-sets/:id/questions`, `POST .../questions`, `DELETE .../questions/:qid`
- `GET /api/admin/question-sets/:setId/sections`, `POST .../sections`, `DELETE .../sections/:sid`
- `POST /api/admin/question-sets/upload/preview`, `POST .../upload`
- `GET /api/admin/question-sets/:id/export.json`, `GET /api/admin/export.csv`
- `GET /api/admin/result/:code` (single result fetch)

## Auth model

Two parallel paths for the admin console; the password path is the
fallback when XSUAA is not bound.

### Password path (legacy / local dev)

- **Admin login**: client-side `SHA-256(plaintext)` via `crypto.subtle.digest`, sends hash. Server compares against `ADMIN_HASH`/`MANAGER_HASH`/`REVIEWER_HASH`/`CONTENT_EDITOR_HASH` env vars.
- **Admin token**: HMAC-SHA256 over `expiry:nonce:role[:issuedAt]`, base64url-encoded, sent in `X-Admin-Token` header. **TTL: 8h.**
- **Session revocation**: `APP_SETTINGS.ADMIN_TOKEN_NOT_BEFORE` is checked on every admin request; bumping it invalidates all tokens issued before that timestamp.

### XSUAA / OAuth 2.0 path (BTP)

- **Service instance**: `itil-evalapp-xsuaa` (plan `application`), bound to the app. Config in `xs-security.json` declares 4 scopes (`$XSAPPNAME.{admin,manager,reviewer,content_editor}`).
- **Browser flow**: SPA calls `GET /api/admin/auth-methods` → if XSUAA is bound, shows a "Sign in with SAP" button. Clicking it navigates to `GET /oauth/login` → 302 to `https://<xsuaa>/oauth/authorize?response_type=code&client_id=...&redirect_uri=...&state=<rand>`. The user authenticates at SAP, which redirects to `GET /oauth/callback?code=...&state=...`. The server validates state (cookie `xsuaa_state`, httpOnly, 10 min), exchanges the code for an access token at `https://<xsuaa>/oauth/token` (HTTP Basic auth with clientid:clientsecret), sets the JWT in cookie `xsuaa_jwt` (httpOnly, SameSite=Lax, Secure, Max-Age = `expires_in`), and 302s to `/?auth=ok`.
- **API clients**: send `Authorization: Bearer <jwt>` directly (RS256, signed by XSUAA's verification key).
- **JWT validation** (`shared/xsuaa.js` `verifyXsuaaJwt`): manual RS256 verification using the verification key from `VCAP_SERVICES.xsuaa[0].credentials.verificationkey`. Validates `alg=RS256`, signature, `exp`, `nbf`, `aud == xsappname` (or `*`).
- **Role mapping**: highest-priority scope wins. `admin` > `manager` > `reviewer` > `content_editor`.
- **Logout**: `POST /api/admin/logout` sets `xsuaa_jwt=; Max-Age=0` to expire the cookie. Does NOT invalidate the IdP session (XSUAA session is separate and would need `/oauth/logout` against XSUAA — not implemented; not needed for this app).
- **No `@sap/xssec` dependency** — manual JWT verification keeps the dep tree small (~140 LOC in `shared/xsuaa.js`).

### Exam token

- HMAC-SHA256 over `CODE:expiry:nonce`, sent in `X-Exam-Token` header. TTL: `max(90min, exam_duration + 30min)`.

### Common

- **Rate limits**: validate 10/10min/IP, admin login 8/15min/IP, proctor 90/min/code-IP.
- **Permissions** are per-role, per-action (`codes:read`, `content:write`, etc.). Full list in `ROLE_PERMISSIONS` in `shared/constants.js` (single source of truth, mirrored in `server.js` and `client/util.js`).

## Env vars

| Var | Required | Notes |
|---|---|---|
| `HANA_HOST`, `HANA_PORT`, `HANA_USER`, `HANA_PASSWORD`, `HANA_SCHEMA` | ✅ | Prod schema is `ITIL_EXAM`. |
| `HANA_ENCRYPT` | optional | `true` by default |
| `HANA_SSL_VALIDATE_CERTIFICATE` | optional | **`true` is the default** (and what we run in BTP). The HANA Cloud cert is signed by DigiCert G5 which is in the Node.js default trust store, so no custom `sslTrustStore` is needed. Set to `false` only as a last-resort workaround for a misconfigured CA. |
| `ADMIN_HASH`, `MANAGER_HASH`, `REVIEWER_HASH`, `CONTENT_EDITOR_HASH` | optional | 64-char SHA-256 hex of role password. If absent, that role's login is disabled. |
| `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`, `ANTHROPIC_VERSION` | optional | If absent, proctor endpoint returns `{ enabled: false }` |
| `EXAM_NAME`, `EXAM_DURATION_SECS`, `EXAM_PASS_PCT`, `EXAM_ACTIVE`, `PROCTOR_ENABLED` | optional | Defaults: 45min, 80%, true, true |
| `STALE_SESSION_MINUTES` | optional | Default 30. Used by sweeper and admin status. |
| `AUTO_CLEAR_STALE_SESSIONS`, `STALE_SESSION_SWEEP_MINUTES` | optional | Default true / 10 |
| `HANA_POOL_SIZE`, `HANA_POOL_PING_CHECK`, `HANA_POOL_TTL_SECONDS` | optional | Defaults 0 / true / 300. When `HANA_POOL_SIZE > 0`, `withDb` uses a pool. See gotcha #3. |
| `RESULT_SIGNING_KEY_ID` | recommended (with `RESULT_SIGNING_KEY`) | Stable identifier for the active signing key (e.g. `"v2"`, `"prod-2026-08"`). 1-64 chars. The value of this env var is what shows up in the `kid` field of every new signed envelope. **Do not change it during a rotation** — pick a new value for the new key instead. |
| `RESULT_SIGNING_KEY` | recommended | 32+ char secret for the active signing key. When set with `RESULT_SIGNING_KEY_ID`, new envelopes use that kid. When unset, falls back to the legacy derived secret (logs a warning). |
| `RESULT_SIGNING_KEY_PREVIOUS_ID` | optional | Stable identifier for the previous signing key during rotation. Must keep the SAME value the previous deployment used for that key. |
| `RESULT_SIGNING_KEY_PREVIOUS` | optional | Previous secret. Kept during rotation so old envelopes still verify. |
| `SLOW_QUERY_MS`, `SLOW_REQUEST_MS` | optional | Default 400 / 1200. Logs slow ops. |
| `STARTUP_STRICT` | optional | `true` in current `.env`. If true, missing required config throws on boot. |
| `APP_REVISION`, `APP_DEPLOYED_AT` | build | Set by `deploy_btp.sh` from git + date. |

### ⚠️ Password handling (current state)

- The `.env` file in this repo is **stale** (the original `DBADMIN_DEV_PASSWORD` value, last deploy April 11). The literal value is not reproduced here — refer to `OPEN_SECRETS.md` for context.
- HANA's DBADMIN password was rotated on 2026-08-18. The new password is held by the operator (not committed in this repo).
- The **live BTP app's `HANA_PASSWORD` env var is now mismatched** and the app is in a degraded state until `cf set-env` is run.
- **Out of scope for this project to touch**: BTP env var updates, secret rotation policy, credential store migration.

## Deploy

```bash
./deploy_btp.sh --api https://api.cf.us10-001.hana.ondemand.com \
                --org <org> --space <space> \
                --domain cfapps.us10-001.hana.ondemand.com
```

Reads HANA vars from `.env` or `--env-file`. Sets `APP_REVISION` from `git rev-parse --short HEAD` (suffix `-dirty` if working tree is dirty). Stops the previous instance and restages if `ANTHROPIC_API_KEY` changes.

## Gotchas (read these before changing anything)

1. **`normalizeExamTitle` and `roleCan` live in `shared/constants.js`** (UMD — used as CommonJS on the server and as `window.SharedConstants` in the browser). `server.js` requires them directly; `client/util.js` defines thin delegates (`function normalizeExamTitle(v) { return window.SharedConstants.normalizeExamTitle(v); }`) so existing call sites keep working. Change in **one place only**: `shared/constants.js`. If you add a new helper, put it there too.
2. **Result JSON contains everything**: stem, options, given, expected — per question, per result. NCLOB. With many results, this is the largest table by storage.
3. **HANA connection pool is opt-in via `HANA_POOL_SIZE`** — set to `0` (or unset, default) for the original open/close behavior. When `> 0`, `shared/db-pool.js` lazy-creates a `hana.createPool(connOpts, poolOpts)` and `withDb` acquires from it. **Use the callback form of `pool.getConnection(cb)`** — the sync form throws "maxConnectedOrPool limit has been reached" on burst. When the pool IS exhausted (internal queue depth limit), `withDb` falls back to opening a fresh non-pooled conn so the request still succeeds (logged as `pool_exhausted_fallback`). Speedup observed: cold 157ms → warm 37ms on `/api/health` with `HANA_POOL_SIZE=5`. 100 concurrent on a 5-slot pool = 100/100 OK, max 2.4s.
4. **`_questionSetCache` is per-process** — if you ever scale to `instances: > 1` in `manifest.yml`, writes from instance A won't invalidate the cache on instance B.
5. **Anthropic proctoring sends raw webcam JPEGs to Anthropic** — see the [Proctoring data flow](#proctoring-data-flow) section for the full chain. Make sure your data processing agreement covers this. Image is in `image/jpeg` at quality 0.65, dimensions ≤ 320×240, 12s timeout.
6. **`/api/bootstrap` returns 410** by design. Don't add code that depends on it.
7. **Code format is `[A-Z2-9]{6}`** — no `I`, `L`, `O`, `0`, `1` to avoid visual confusion. Keep that in mind for any code generation.
8. **Multi-select grading is exact-match**: must select every correct option, no partial credit. Document this if the user asks.
9. **`REQUEST.body.code` is checked against `req.examSession.code`** in progress/submit endpoints. If they don't match, returns 403. Don't break this.
10. **`requireAdminRole('admin')`** is stricter than `requireAdmin` + `requirePermission(...)`. Use it for destructive ops (delete, reset, revoke).
11. **`STARTUP_STRICT=true`** means missing config throws on boot. Good for prod, annoying for local. Use it deliberately.
12. **Question set activation** demotes other sets in the same `VERSION_GROUP_ID` to `ARCHIVED`. Currently the 3 inactive sets are all in `PUBLISHED` — drift to fix.
13. **`getQuestionSetRows` is the single source of truth** for listing sets. Always go through it, never query `QUESTION_SETS` directly in new code.
14. **Admin login is rate-limited TWICE** — once by IP (`admin_login:<ip>`, 8/15min) and once by SHA-256 of the password itself (`admin_login_hash:<hash>`, 20/15min). The hash bucket exists to stop a password-spray where the attacker rotates IPs to dodge the per-IP limit. **Never log the hash** (it's the password, just hashed). The 429 response is identical for both — no oracle to tell the attacker which limit they hit.
15. **`/api/admin/me` is the canonical role probe** for the client. It works for any role (admin, manager, reviewer, content_editor) because it doesn't require a specific permission, only valid auth. Don't fall back to reading `data.role` from `/api/admin/codes` for the cookie-based XSUAA flow — codes requires `codes:read` which reviewers don't have.
16. **`lib/audit.js` is a self-contained module** that owns the audit log + its metrics. Server.js just calls `tryWriteAdminAudit()` and exposes `audit.getMetrics()` in `/api/admin/metrics`. The metrics track `attempts / writes / skippedNoTable / skippedNoDb / failures / lastFailureAt / lastFailureMessage / auditTablePresent` so a quietly broken audit log (table dropped, HANA outage) is visible in metrics instead of being silently swallowed.
17. **String literals for roles / code statuses / lifecycle / exam modes / audit actions** are in `shared/constants.js` (`ROLES`, `CODE_STATUS`, `QUESTION_SET_LIFECYCLE`, `EXAM_MODE`, `AUDIT_ACTION`). Use the constants, not raw strings — `grep " 'admin'"` should ideally be empty in the codebase.
18. **Result-signing key is now versioned with STABLE key IDs** — `getSigningKeyMap()` returns `{ current, keys }` where `current` is the operator-supplied `RESULT_SIGNING_KEY_ID` (e.g. `"v2"`) and `keys` is a map of `{ <id>: <secret>, ..., legacy: <derived> }`. **Key IDs are stable across rotations** — pick a new id for a new key, never re-use an old id for a new secret. The kid field on every envelope is the id, and the verifier looks up `keys[envelope.kid]`. Rotation procedure:
    1. Deploy 1: set `RESULT_SIGNING_KEY_ID=v1`, `RESULT_SIGNING_KEY=<secret1>`. New envelopes get `kid=v1`.
    2. Rotate: set `RESULT_SIGNING_KEY_ID=v2`, `RESULT_SIGNING_KEY=<secret2>`, `RESULT_SIGNING_KEY_PREVIOUS_ID=v1`, `RESULT_SIGNING_KEY_PREVIOUS=<secret1>`. New envelopes get `kid=v2`; old envelopes (`kid=v1`) still verify because `keys["v1"]` still points to `<secret1>`.
    3. Drop the old key: remove the `*_PREVIOUS*` env vars. `keys["v1"]` is gone; old `kid=v1` envelopes now fail.
    The earlier implementation hard-coded `current → v1` and `previous → v2`, which broke rotation in the same way. The fix and the e2e test are in `tests/signing-key-rotation.test.js`.

## HANA findings (Fase 0, 2026-08-18)

1. **`EXAM_INCIDENTS` is orphan** — 0 rows, no code path touches it. **DROPPED 2026-08-18** ✅
2. **`EXAM_QUESTIONS` (legacy) is dead** — 30 rows, code only reads from `QUESTION_SET_QUESTIONS`. Migration already moved data. **DROPPED 2026-08-18** ✅ (snapshot of the 30 rows saved at `/tmp/exam_questions_snapshot.json` for recovery).
3. **1 stale active session** — 7WGME9, last save 2026-07-16. **Cleaned 2026-08-18** ✅. Sweeper investigation pending — see open items.
4. **All inactive sets in `LIFECYCLE_STATUS=PUBLISHED`** — should be `ARCHIVED`. **Fixed 2026-08-18** ✅ — IDs 1, 2, 5 are now ARCHIVED.
5. **Zero PRACTICE results in 242** — feature implemented but never used. Confirm with stakeholders if it's still in scope, otherwise document as "deferred".
6. **Audit log silent in August** — last entry 2026-07-23, zero events in August. No `admin_login_failed` either, so the BTP app may not be reachable (possibly due to DBADMIN password rotation). Cross-check with `cf logs itil4-evalapp --recent` once env is fixed.
7. **DBADMIN password rotated 2026-08-18** — `.env` in this repo is **stale** (still has the original `DBADMIN_DEV_PASSWORD` value, which is no longer valid). Live BTP app status is **unknown** and out of scope to fix from this repo. The new password is held by the operator — do not commit it. The literal value is referenced symbolically only; see `OPEN_SECRETS.md`.

## Proctoring data flow

When `PROCTOR_ENABLED=true` on a question set, candidates go through a tech
check (`getUserMedia` for webcam, `getDisplayMedia` for screen) and then
are continuously monitored while the exam is in progress.

### What is captured (locally in the browser)

- **Webcam video stream** — `MediaStream` from `getUserMedia({ video: { width: 320, height: 240 } })`. Never uploaded as a stream.
- **Screen share video stream** — `MediaStream` from `getDisplayMedia({ video: { displaySurface: 'monitor' } })`. Never uploaded as a stream.
- **Per-proctor-check JPEG** — every 28s (`CFG.webcamIntervalS`), one frame is drawn to a hidden canvas, encoded as `image/jpeg` at quality `0.65`, and ≤ 320×240 px.

### What leaves the browser

Only the per-check JPEG and only to the server endpoint
`POST /api/proctor/check`. The server immediately forwards the JPEG to
Anthropic's `messages` API with a strict prompt asking for a JSON
`{flag, reason}` verdict (no face, second person, phone/notes, looking
away). The flag and reason return to the client; the JPEG is not
persisted server-side.

### Anthropic's data exposure

- The webcam JPEG is sent **with the candidate's face and any PII visible
  on their desk** to Anthropic under whatever data-processing agreement
  the operator has with Anthropic.
- API key is `ANTHROPIC_API_KEY` env var; never in code or logs.
- 12-second timeout per call. 90 calls/min/code-IP rate limit.
- If `ANTHROPIC_API_KEY` is missing, the endpoint returns
  `{ enabled: false, flag: false, reason: null }` and no image is sent.

### What is persisted

- **Incidents** appended to `S.incidents` in browser state and serialized
  into `EXAM_SESSIONS.SESSION_JSON` via `/api/progress`. Each entry is
  `{ time, type, detail }`. Types: `tab_switch`, `focus_lost`,
  `focus_returned`, `possible_screenshot`, `screenshot_attempt`,
  `right_click`, `shortcut`, `ai_flag`, `screen_stopped`.
- **Final incident count** ends up in `EXAM_RESULTS.INCIDENT_COUNT` and
  in `RESULT_JSON.incidentCount` / `RESULT_JSON.incidents[]`.
- **No image** is ever stored in HANA. Only the boolean flag and short
  reason text from Anthropic are kept.

### Compliance notes

- The webcam JPEG route is opt-in: a question set can disable proctoring
  entirely with `PROCTOR_ENABLED=false` (`PATCH /api/admin/question-sets/:id/config`).
- Candidates see a consent screen with the bullet "Webcam images are
  analysed for proctoring only. No answer key is stored in the browser"
  before enabling monitoring.
- For a copy of the Anthropic prompt and the exact server code, see
  `server.js` around line 1586 (`/api/proctor/check`) and `client/proctor.js`
  (`startProctor`, `proctor`).


## Cleanup log (2026-08-18)

Operations executed in prod HANA (`be84eee8-9540-4517-be90-a3267f32084a.hna1.prod-us10.hanacloud.ondemand.com`):

| # | Op | Result | Notes |
|---|---|---|---|
| 1 | `DROP TABLE EXAM_INCIDENTS` | ✅ 0 rows dropped, table gone | orphan, no code refs |
| 2 | Snapshot 30 rows + `DROP TABLE EXAM_QUESTIONS` | ✅ snapshot at `/tmp/exam_questions_snapshot.json`, table gone | legacy, data in active set |
| 3 | `UPDATE QUESTION_SETS SET LIFECYCLE_STATUS='ARCHIVED' WHERE IS_ACTIVE=FALSE AND LIFECYCLE_STATUS='PUBLISHED'` | ✅ 3 rows affected (IDs 1, 2, 5) | normalizes drift |
| 4 | `DELETE FROM EXAM_SESSIONS WHERE ACCESS_CODE='7WGME9'` + `UPDATE ACCESS_CODES SET STATUS='unused' WHERE ACCESS_CODE='7WGME9' AND STATUS='active'` | ✅ 1 session deleted, 1 access code returned to `unused` pool | stale 33 days, sweeper had not run |

## Deployment status (as of 2026-08-18)

**BTP is now running `e1165e6` (HEAD, deployed today 2026-08-18 21:04 PDT).**

The deployment drift documented earlier in this file has been resolved:
all 8 commits between the previous deployed version (`a0dae34`) and HEAD
are now live, including the stale-session sweeper, the admin console,
question set versioning, audit log, and the proctoring endpoint.

| Metric | Was deployed (`a0dae34`) | Now live (HEAD) |
|---|---:|---:|
| Commits | 1 | 8 |
| `server.js` lines | 2,142 | 3,520 |
| API routes | 40 | 58 |
| Stale-session sweeper | ❌ missing | ✅ running every 10 min |

**BTP env vars confirmed after deploy:**
- HANA: `ITIL_EXAM_ADMIN` (password in BTP credential store, **not committed**)
- `ADMIN_HASH`, `MANAGER_HASH` set
- `REVIEWER_HASH`, `CONTENT_EDITOR_HASH` not set (login disabled for those roles)
- `STARTUP_STRICT=true`, `AUTO_CLEAR_STALE_SESSIONS=true`, `STALE_SESSION_SWEEP_MINUTES=10` (new in this deploy)

> **Note**: an earlier version of this section (commit `b06518d`) included the
> HANA password in plain text. That commit was already pushed to the public
> GitHub remote. **The HANA password must be considered compromised and
> rotated in BTP.** The password was removed from the source on
> 2026-08-19; historic commits still contain it. See `OPEN_SECRETS.md` for
> the full incident note.
- Note: `APP_REVISION` and `APP_DEPLOYED_AT` still show the April values because they are only updated by `deploy_btp.sh`. The live code is the HEAD version.

**Buildpack pin** (added in deploy commit, see git log):
The manifest now pins the buildpack to `nodejs_buildpack#v1.9.1` (via the
git URI form). BTP's default `nodejs_buildpack` resolves to 1.9.2 which
fails in the python bootstrap step on `cflinuxfs4`. Pinning to 1.9.1
matches the version that worked for the April 11 deploy. When SAP
deprecates 1.9.1, this pin needs to be updated.

### Deploy procedure used (documented for next time)

```bash
# 1. Get current BTP env vars (especially HANA_*)
cf env itil4-evalapp

# 2. Build a vars file from the current env (do NOT use the .env; it
#    has the stale DBADMIN dev credentials).
cat > /tmp/cf-vars.yml <<EOF
route_host: academycd-evalapp
default_domain: cfapps.us10.hana.ondemand.com
hana_host: <from cf env>
hana_port: "443"
hana_user: ITIL_EXAM_ADMIN
hana_password: <from cf env>
hana_schema: ITIL_EXAM
hana_encrypt: "true"
hana_ssl_validate_certificate: "false"
EOF

# 3. Push
cf push itil4-evalapp --vars-file /tmp/cf-vars.yml

# 4. Verify health
cf app itil4-evalapp
cf logs itil4-evalapp --recent

# 5. Clean up: rm /tmp/cf-vars.yml
```

**Why not `deploy_btp.sh`**: that script reads HANA vars from `.env` and
overrides BTP env vars. Since the local `.env` has the stale DBADMIN dev
credentials (and the user explicitly excluded password rotation from
this session), the right approach is to use the values that are already
working in BTP, not the local dev defaults.

## Authentication

The app supports two admin auth mechanisms. The server picks one based on
what's configured at startup.

### Mechanism 1: SHA-256 role hashes (default / local dev)

`ADMIN_HASH`, `MANAGER_HASH`, `REVIEWER_HASH`, `CONTENT_EDITOR_HASH` env vars
each hold a 64-char SHA-256 hex of the role's password. Login at
`POST /api/admin/login` sends the SHA-256 hash; server compares. Disabled
when the corresponding env var is empty. This is what runs in local dev
(no XSUAA bound).

### Mechanism 2: XSUAA OAuth (BTP / prod)

The BTP XSUAA service `itil-evalapp-xsuaa` is bound to the app. The
config in `xs-security.json` declares 4 scopes
(`$XSAPPNAME.admin`, `.manager`, `.reviewer`, `.content_editor`) and a
role template that puts those scopes into the access token. Admin
endpoints accept a Bearer token (Authorization: Bearer <jwt>); the
server validates against the XSUAA `verificationkey` from
`VCAP_SERVICES` and maps the granted scopes to the internal role.

The two mechanisms coexist: if `process.env.VCAP_SERVICES` contains an
xsuaa binding, the server uses Bearer auth; otherwise it falls back to
the SHA-256 hash. This means local dev (no XSUAA bound) keeps working
without any extra config.

## CSRF analysis

**Threat model**: an attacker hosts a page on `evil.com`. The victim's
browser holds a session cookie or storage state for our app. The
attacker tricks the victim's browser into issuing a state-changing
request (POST/DELETE/PUT) to our origin, with the victim's auth attached.

**Why this app is mostly not vulnerable**:

1. **No cookie-based auth on the candidate or admin side.** Both auth
   mechanisms (SHA-256 token, XSUAA JWT) are sent in **headers** that
   the attacker cannot set from a cross-origin page:
   - `X-Admin-Token: <base64url>` — set as a custom header, not
     auto-attached by the browser.
   - `Authorization: Bearer <jwt>` — same.
   - `X-Exam-Token: <base64url>` — same.
   Browsers DO NOT send custom headers cross-origin without an explicit
   CORS preflight, and a `evil.com` script cannot trigger that preflight
   (no `Content-Type: application/json` would force it; even if it did,
   the server's CORS policy would reject it).

2. **`xsuaa_jwt` cookie** is the one token that IS auto-attached. It
   is set with `SameSite=Lax; HttpOnly; Secure`. Lax means it is
   **not** sent on cross-site sub-requests (XHR / fetch / form POSTs
   from another origin), only on top-level navigations (the user
   typing the URL, following a link, or hitting back). A `evil.com`
   `<form action="https://academycd-evalapp.../api/admin/codes" method="POST">`
   would NOT include the `xsuaa_jwt` cookie because Lax blocks cookies
   on cross-site POSTs. So even the cookie-based path is safe.

3. **No GET-as-state-change endpoints.** Every state-changing endpoint
   requires POST / DELETE / PUT. Express routes are explicit per
   method. (See API surface section — there are no GETs that mutate.)

4. **CORS is not configured for cross-origin admin access.** A
   `evil.com` script doing `fetch('https://academycd-evalapp.../api/admin/...')`
   would have to handle CORS preflight (`OPTIONS`), and the server
   doesn't reply with permissive `Access-Control-Allow-Origin` for any
   non-self origin. So even with the cookie, the attacker cannot read
   the response.

5. **Rate limits + audit** mean a successful CSRF would be visible in
   `ADMIN_AUDIT_LOG` within seconds. The IP-keyed login rate limit
   stops brute-force auth attempts; the new hash-keyed limit stops
   password-spray across IPs.

**What an attacker CAN do**:

- Force a top-level navigation (the Lax exception). E.g.
  `<a href="https://academycd-evalapp.../api/admin/codes/ABC123/reset">`
  via a phishing link. The server will not reset anything because
  `POST /api/admin/codes/:code/reset` requires a POST — a GET
  navigation just hits the SPA shell.
- Read the SPA shell HTML (no auth) and link the user to the public
  landing. Already public, no information disclosure.
- Phish the admin's password via a fake login page that submits to
  `evil.com`. This is the standard phishing threat, not a CSRF
  bypass. Mitigations: bind XSUAA so admins never see a password
  prompt; ensure the password page is served over HTTPS with HSTS
  (CF router sets HSTS by default).

**Conclusion**: the CSRF attack surface is minimal. No mitigation
needed beyond the existing SameSite=Lax + header-based tokens + CORS
defaults. **Do not** add CSRF tokens unless the auth model changes
(e.g. switching to cookie-based sessions with `SameSite=None` for
cross-origin SSO, or adding a webhook-style endpoint callable from
another origin).

## Open items / TODO

- [x] Investigate stale-session sweeper health → **NOT a code bug, deployment drift.** BTP runs `a0dae34` (2026-03-31) but the sweeper was added in `e843506` (2026-06-06). Fix = deploy HEAD once `HANA_PASSWORD` is rotated in BTP. Also hardened: per-tick log, `GET /api/admin/sweeper-status` endpoint, `isStuck` flag for silent crashes.
- [x] Decide on PRACTICE feature scope (0 uses in 242 results) → **Keep.** The feature is fully implemented (EXAM_MODE column, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS, dedicated UI affordances in `client/admin-question-sets.js`, admin config dialog, results filtering, analytics split). Zero uses means admins haven't created a PRACTICE question set yet, not that the feature is dead. Documented in the config dialog (`client/admin-question-sets.js` `configQuestionSet`), the server config endpoint, and the HANA schema.
- [x] Pool HANA connections → opt-in via `HANA_POOL_SIZE`, see gotcha #3.
- [x] Dedupe `normalizeExamTitle` and `roleCan` → now in `shared/constants.js` (UMD), used by both server and client. See gotcha #1.
- [x] Document Anthropic proctoring data flow → see "Proctoring data flow" section above.
- [x] Investigate audit log silence in August → **NOT a bug, just no real admin activity.** Queried HANA directly: the last `admin_login_success` is `2026-07-23 12:35:30` (Fernando). Between that and 2026-08-19 (today), the only audit rows are 2 `admin_login_failed` from MY own password-login tests — internal CF IPs, no real session. The audit log is working; the silence reflects the operator not actively using the admin console.
- [ ] Update BTP env var `HANA_PASSWORD` to match the new prod password (out of scope for this repo)
- [ ] Move BTP creds out of `cf set-env` into a credential store (out of scope for this repo)
- [x] Convert `onclick="X()"` handlers to `data-action="X"` event delegation → done in commit `021c3c4`. `client/dispatcher.js` is the single listener. `client/main.js` no longer carries the `window.X = X` re-export list.
- [x] `lib/` extraction: `audit.js` (compliance metrics), `middleware.js` (auth middlewares via factory), `responses.js` (jsonError/jsonOk/csvCell/signed-envelope), `rate-limit.js` (sliding window). `server.js` shrunk from ~3830 → ~3720 LOC.
- [x] `tryWriteAdminAudit` visibility: failure counters (attempts, writes, skippedNoTable, skippedNoDb, failures, lastFailureAt, lastFailureMessage, auditTablePresent) exposed via `audit.getMetrics()` → `/api/admin/metrics.audit`.
- [x] Admin-login rate limit by SHA-256 hash (in addition to IP) → stops password-spray across IPs. 20 attempts per 15min per hash. Hash never logged.
- [x] String literal centralization: `ROLES`, `CODE_STATUS`, `QUESTION_SET_LIFECYCLE`, `EXAM_MODE`, `AUDIT_ACTION` in `shared/constants.js`. 16 unit tests cover the catalog.
- [x] `/api/admin/me` as canonical for client role lookup → cookie-based XSUAA flow + reviewers (no `codes:read`) both work.
- [x] "Sign in with SAP" loading state (spinner during 302 to IdP) → prevents double-click that races the OAuth flow.
- [x] Inactivity banner on candidate landing → shows exam duration + pass mark + "Session secure" indicator.
- [x] CSRF analysis writeup → minimal attack surface (header-based tokens, SameSite=Lax, no CORS, no GET-as-state-change).
- [x] `__el__` sentinel in event delegation → allows the XSUAA login button to pass its own element to `startXsuaaLogin(el)` for in-place spinner swap.
- [ ] Sweeper end-to-end test (real HANA stale session cleared by sweeper) — deferred to next session, requires CI infra.
- [ ] JSDOM-light tests for client renderers (`renderQ`, `showAdmin`, `showResultsFromRecord`) — deferred, no test deps added.
- [ ] Route split (deferred — multi-day effort, deserves its own session).
- [ ] **Branch protection on `main`** (operator action, GitHub-side): after the secret-leak incident in commit `b06518d`, the next operator should enable at least "Require pull request before merging" + "Do not allow force pushes" on `main` (Settings → Branches → main → Branch protection rules). The pre-commit hook catches secrets locally, but a server-side gate is the second line of defense. GitHub also offers free secret scanning + push protection for public repos — worth turning on. The repo is currently `protected: false` (`gh api repos/fernandosap/ITIL-EvalApp/branches/main/protection` returns 404).

## Open security debt

These are the remaining security follow-ups. Ordered by recommended
sequence — rotate the leaked password FIRST, then run the platform
upgrades, then tighten the operational defaults.

### 1. Rotate `HANA_ITIL_EXAM_ADMIN_PASSWORD` in BTP (P0)

The password was committed in `b06518d` (Aug 2026) and briefly
re-published in `OPEN_SECRETS.md` in `13e156b`. Both values are
public. The literal value is **not** reproduced in this file or in
`OPEN_SECRETS.md` — see `OPEN_SECRETS.md` for the identifier and
the action plan. Operationally: rotate via BTP Cockpit → HANA
Cloud → "Reset Administrator Password" for `ITIL_EXAM_ADMIN`,
store the new value in the BTP credential store, and audit the
HANA audit trail (NOT `cf logs`) for any connections from
non-CF source IPs in the window between 2026-08-18 and the
rotation time.

### 2. Migrate to Node 22 LTS ✅ DONE (commit `28c77f5`); cflinuxfs5 ⏳ PENDING

Node 22 LTS (`22.22.2`) is now live in BTP. `engines.node` is
`22.x`, buildpack pin is `nodejs_buildpack#v1.9.2` (which
removes EOL Node 20 and ships 22.22.2). **The buildpack is
still on `cflinuxfs4`**; the cflinuxfs5 default kicks in
Feb 2027 per SAP. The stack migration is a separate piece of
work — needs a newer buildpack that supports cflinuxfs5 (not
yet in the v1.9.x line as of this writing) and a `cf push
--stack cflinuxfs5` re-deploy. Will revisit when SAP publishes
a cflinuxfs5-capable buildpack version.

### 3. HANA TLS certificate validation ✅ DONE (commit `9bdfe27`)

`HANA_SSL_VALIDATE_CERTIFICATE` is now `true` by default in both
code and BTP. Verified that the HANA Cloud cert chain
(`SAP SE → DigiCert G5 TLS RSA4096 SHA384 2021 CA1`) is in
Node's default trust store, so no `sslTrustStore` is needed.
`/api/health` returns 200 with `db: connected` end-to-end
through the validated TLS connection.

### 4. Versioned result-signing key ✅ DONE (commit `5014841`, **hardened** in current commit)

`getSigningKeyMap()` returns `{ current, keys }` where `current`
is the operator-supplied `RESULT_SIGNING_KEY_ID` and `keys`
maps each id to its secret. **Key IDs are stable across
rotations** — pick a new id for a new key, never re-use an
old id for a new secret (the earlier `current → v1` /
`previous → v2` design was buggy and is documented in gotcha
#18). Envelope format includes `kid`; the verifier looks up
`keys[envelope.kid]`. Without any `RESULT_SIGNING_KEY*` env
vars set, the app falls back to the legacy derived secret
under the `legacy` kid and logs a warning at boot. Rotation
procedure and env-var names are documented in the env-var
table above and in gotcha #18. The `tests/signing-key-rotation.test.js`
e2e test simulates two deployments via real env-var changes
and verifies that an envelope signed in deploy-1 still
verifies in deploy-2.

### 5. Replace custom XSUAA JWT verifier with `@sap/xssec` — Investigated, deferred (P2)

**Status**: investigated 2026-08-19, **not migrated**.

**Reason**: `@sap/xssec` 4.x is designed around `createSecurityContext`
which always goes through an HTTP call to the XSUAA JWKS endpoint
(`<xsuaa-url>/token_keys`) to look up the signing key by the JWT
header's `kid`. That model assumes tokens are issued by the real
XSUAA service and discoverable via JWKS. Our current verifier
in `shared/xsuaa.js` validates the signature directly against the
`verificationkey` from the service binding, which is a different
operating model (offline, no JWKS roundtrip, works with any RS256
token whose signing key we know).

Trying to swap to xssec:
- Breaks the 44 existing `tests/xsuaa.test.js` (they generate
  local RSA-2048 keypairs, sign with them, and pass the public
  key as `verificationkey`. xssec's `createSecurityContext`
  refuses these because the `kid` is not in a JWKS it can fetch).
- Adds operational coupling: every running instance now needs
  network access to the XSUAA `/token_keys` endpoint to validate
  a single token. That endpoint is sometimes behind the same
  auth boundary the token itself is trying to satisfy, creating
  a chicken-and-egg problem during a misconfiguration.
- Adds 2 transitive deps (`debug`, `jwt-decode`) for a feature
  we already have.

**Functional parity** between our custom verifier and xssec:
- ✅ RS256 signature (both)
- ✅ `exp` / `nbf` (both — we just added explicit `nbf` handling
  in commit `f1f4b34`/the session before; xssec always handled it)
- ✅ `aud` matching the binding's `xsappname` (both, required
  in our verifier since session hardening)
- ✅ `iss` matching the binding's tenant URL (both, optional
  in xssec but enforced by us)
- ❌ Proof token validation (xssec only, not used by us)
- ❌ JWKS-based key rotation (xssec only, our `verificationkey`
  is static for the lifetime of the binding)

**When this becomes worth re-evaluating**:
- XSUAA rolls out a token-format change we don't know about (e.g.
  a new mandatory claim or a new `alg`). xssec would track it;
  our custom verifier would silently reject everything.
- We need proof-token validation (currently not used; would
  matter if XSUAA started emitting proof tokens by default).
- The XSUAA service starts refusing tokens that don't go through
  JWKS lookup (currently it doesn't, but this is SAP's direction).

Until one of those triggers, the custom verifier is a smaller
attack surface than the official lib for our specific use case
(single tenant, static verification key, no proof tokens). If
you need to revisit, the investigation notes above should make
the migration plan clear.

## Inspecting HANA (read-only)

Use the `scripts/inspect-hana.mjs` tool. It requires DBADMIN creds, runs only
SELECT statements, and dumps the full schema + aggregates to JSON. Re-run after
any schema change to detect drift.

```bash
# Use the .env password (default for local dev)
npm run inspect:hana

# Override the password (e.g. after a rotation)
HANA_PASSWORD='<new-password>' npm run inspect:hana
```

The tool emits JSON with: tables, columns per table, optional-column presence
checks, row counts, APP_SETTINGS keys, QUESTION_SETS aggregate, EXAM_RESULTS
aggregate by mode/PASS, ACCESS_CODES by status, EXAM_SESSIONS stale count, and
ADMIN_AUDIT_LOG by month. Never log or print row-level candidate data.
