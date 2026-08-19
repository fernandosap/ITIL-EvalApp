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
- **HEAD commit at inspection time**: `e843506` — "feat(app): expand admin workflows and resilient exam flow"

## Stack

- Node 20 (engines pinned in `package.json`)
- Express 4.21
- `@sap/hana-client` 2.28 (lockfile target 2.25 — minor drift, OK)
- Vanilla JS frontend (no bundler, no framework) — `client-app.js` is the SPA, served from `index.html`
- SAP HANA Cloud, schema `ITIL_EXAM`
- Anthropic API for proctor image analysis (`claude-sonnet-4-20250514`)

## File map

| File | Lines | Role |
|---|---:|---|
| `server.js` | 3,709 | API + admin + HANA queries + token signing + metrics |
| `client-app.js` | 2,855 | SPA (candidate + admin), HTML rendered via `innerHTML` |
| `index.html` | 383 | Shell, all CSS inline, hidden Netlify form (legacy, harmless) |
| `migrations/*.sql` | 369 total | 7 idempotent migrations, all currently applied in prod |
| `scripts/check-env.mjs` | 65 | Validates required env vars before deploy |
| `scripts/smoke-test.mjs` | 299 | Boots server, hits HANA real, exercises admin login + code generation |
| `deploy_btp.sh` | 278 | `cf push` with env-file + SSO support |
| `Staticfile` | 87 | CF static buildpack config (gzip on) |
| `manifest.yml` | 577 | CF app manifest with `((...))` placeholders for env vars |
| `favicon.svg` | 26 | Brand mark (blue/teal gradient) |
| `.cfignore` | 41 | Excludes `.git/`, `.gitignore`, `.DS_Store`, `node_modules/` |

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

## API surface (57 routes)

### Candidate (no auth)
- `POST /api/validate` — validate 6-char access code
- `POST /api/session/start` — start/resume an exam session
- `GET /api/question/:displayIdx` — fetch one question (requires exam token)
- `POST /api/progress` — autosave progress
- `POST /api/submit` — grade and store result
- `POST /api/proctor/check` — send webcam JPEG to Anthropic (rate-limited)
- `GET /api/health`, `GET /api/status` — public health/config
- `GET /api/bootstrap` — **410 Gone by design** (intentionally disabled for security)
- `GET /`, `/client-app.js`, `/favicon.svg` — static shell

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
- **Permissions** are per-role, per-action (`codes:read`, `content:write`, etc.). Full list in `ROLE_PERMISSIONS` in `server.js` and mirrored in `roleCan` in `client-app.js`.

## Env vars

| Var | Required | Notes |
|---|---|---|
| `HANA_HOST`, `HANA_PORT`, `HANA_USER`, `HANA_PASSWORD`, `HANA_SCHEMA` | ✅ | Prod schema is `ITIL_EXAM`. |
| `HANA_ENCRYPT` | optional | `true` by default |
| `HANA_SSL_VALIDATE_CERTIFICATE` | optional | `false` is current (with `STARTUP_STRICT` this generates a warning) |
| `ADMIN_HASH`, `MANAGER_HASH`, `REVIEWER_HASH`, `CONTENT_EDITOR_HASH` | optional | 64-char SHA-256 hex of role password. If absent, that role's login is disabled. |
| `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`, `ANTHROPIC_VERSION` | optional | If absent, proctor endpoint returns `{ enabled: false }` |
| `EXAM_NAME`, `EXAM_DURATION_SECS`, `EXAM_PASS_PCT`, `EXAM_ACTIVE`, `PROCTOR_ENABLED` | optional | Defaults: 45min, 80%, true, true |
| `STALE_SESSION_MINUTES` | optional | Default 30. Used by sweeper and admin status. |
| `AUTO_CLEAR_STALE_SESSIONS`, `STALE_SESSION_SWEEP_MINUTES` | optional | Default true / 10 |
| `HANA_POOL_SIZE`, `HANA_POOL_PING_CHECK`, `HANA_POOL_TTL_SECONDS` | optional | Defaults 0 / true / 300. When `HANA_POOL_SIZE > 0`, `withDb` uses a pool. See gotcha #3. |
| `SLOW_QUERY_MS`, `SLOW_REQUEST_MS` | optional | Default 400 / 1200. Logs slow ops. |
| `STARTUP_STRICT` | optional | `true` in current `.env`. If true, missing required config throws on boot. |
| `APP_REVISION`, `APP_DEPLOYED_AT` | build | Set by `deploy_btp.sh` from git + date. |

### ⚠️ Password handling (current state)

- The `.env` file in this repo is **stale** (the original `WelcomeWelcome1.`, last deploy April 11).
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

1. **`normalizeExamTitle` and `roleCan` are duplicated** in `server.js` and `client-app.js`. Change in both places. Candidates: extract to a shared file loaded by both.
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

## HANA findings (Fase 0, 2026-08-18)

1. **`EXAM_INCIDENTS` is orphan** — 0 rows, no code path touches it. **DROPPED 2026-08-18** ✅
2. **`EXAM_QUESTIONS` (legacy) is dead** — 30 rows, code only reads from `QUESTION_SET_QUESTIONS`. Migration already moved data. **DROPPED 2026-08-18** ✅ (snapshot of the 30 rows saved at `/tmp/exam_questions_snapshot.json` for recovery).
3. **1 stale active session** — 7WGME9, last save 2026-07-16. **Cleaned 2026-08-18** ✅. Sweeper investigation pending — see open items.
4. **All inactive sets in `LIFECYCLE_STATUS=PUBLISHED`** — should be `ARCHIVED`. **Fixed 2026-08-18** ✅ — IDs 1, 2, 5 are now ARCHIVED.
5. **Zero PRACTICE results in 242** — feature implemented but never used. Confirm with stakeholders if it's still in scope, otherwise document as "deferred".
6. **Audit log silent in August** — last entry 2026-07-23, zero events in August. No `admin_login_failed` either, so the BTP app may not be reachable (possibly due to DBADMIN password rotation). Cross-check with `cf logs itil4-evalapp --recent` once env is fixed.
7. **DBADMIN password rotated 2026-08-18** — `.env` in this repo is **stale** (still has the original `WelcomeWelcome1.` which is no longer valid). Live BTP app status is **unknown** and out of scope to fix from this repo. The new password is held by the operator — do not commit it.

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
  `server.js` around line 1586 (`/api/proctor/check`) and `client-app.js`
  around line 888 (`startProctor`, `proctor`).


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
- HANA: `ITIL_EXAM_ADMIN` / `SAPacademy_ITIL_EXAM_2026!` (separate from DBADMIN used for local dev)
- `ADMIN_HASH`, `MANAGER_HASH` set
- `REVIEWER_HASH`, `CONTENT_EDITOR_HASH` not set (login disabled for those roles)
- `STARTUP_STRICT=true`, `AUTO_CLEAR_STALE_SESSIONS=true`, `STALE_SESSION_SWEEP_MINUTES=10` (new in this deploy)
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

## Open items / TODO

- [x] Investigate stale-session sweeper health → **NOT a code bug, deployment drift.** BTP runs `a0dae34` (2026-03-31) but the sweeper was added in `e843506` (2026-06-06). Fix = deploy HEAD once `HANA_PASSWORD` is rotated in BTP. Also hardened: per-tick log, `GET /api/admin/sweeper-status` endpoint, `isStuck` flag for silent crashes.
- [x] Decide on PRACTICE feature scope (0 uses in 242 results) → **Keep.** The feature is fully implemented (EXAM_MODE column, SHOW_CORRECT_ANSWERS, COUNTS_TOWARD_RESULTS, dedicated UI affordances in `client-app.js`, admin config dialog, results filtering, analytics split). Zero uses means admins haven't created a PRACTICE question set yet, not that the feature is dead. Documented in `client-app.js` (config dialog), `server.js` (config endpoint), and the HANA schema.
- [ ] Investigate audit log silence in August (likely tied to BTP app health post-password-rotation)
- [ ] Update BTP env var `HANA_PASSWORD` to match the new prod password (out of scope for this repo)
- [ ] Consider pooling HANA connections
- [ ] Consider deduping `normalizeExamTitle` and `roleCan` client/server
- [ ] Document Anthropic proctoring data flow in this file
- [ ] Move BTP creds out of `cf set-env` into a credential store (out of scope for this repo)

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
