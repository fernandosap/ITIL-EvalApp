# Contributing to ITIL-EvalApp

This project runs on SAP BTP (Cloud Foundry) and persists to HANA Cloud.
There is no hosted CI (the org's GitHub billing does not allow Actions),
so the workflow below is the substitute.

## Local setup

```bash
node --version  # 20.x
npm ci          # install exact deps from lockfile
cp .env.example .env  # (one day) fill in HANA_*, role hashes, etc.
```

The `.env` in this repo is committed as a sample of the local dev
shape; the actual HANA creds for prod live in `cf env` and are not in
git (see `.gitignore`).

## Before every commit

```bash
npm test
```

This runs `node --test tests/*.test.js` and currently executes 98 unit
tests. The whole suite finishes in under 1 second and covers:
- `shared/scoring.js` (grade, pick, build ordering, PRNG)
- `shared/xsuaa.js` (JWT validation, VCAP parsing, scope mapping)
- `shared/constants.js` (brand normalization, role permissions)
- `shared/db-pool.js` (pool lifecycle, env-driven config, fake hana)
- `validateQuestionUploadEntries` (CSV bulk import)
- `getSweeperStatus` lifecycle
- `buildSignedEnvelope` / `verifySignedEnvelope` (result tamper-evidence)
- HANA cleanup (smoke test in `scripts/smoke-test.mjs` if HANA is
  reachable)

Tests are pure (no HANA required). They use real RSA-2048 keys generated
in-process for the XSUAA JWT tests, so no secrets are hardcoded.

## Pre-commit hook (substitutes for CI)

A pre-commit hook lives in `.githooks/pre-commit`. It runs the full test
suite and a secret scan over the staged diff (catches `WelcomeWelcome*.`,
`SAPacademy_*_2026!`, AWS/GitHub/OpenAI/Slack tokens, private keys,
embedded creds in connection strings). To activate it (one-time):

```bash
git config core.hooksPath .githooks
```

The hook is **not** auto-installed by `npm install` because git's
`core.hooksPath` is per-clone, not per-repo. To enforce the gate
project-wide without depending on individual setup, add the line above
to onboarding docs or your team's bootstrap script.

Run the gate manually without committing:

```bash
npm run precommit
```

Skip the gate in an emergency:

```bash
git commit --no-verify
```

Add `# secretscan: ok` on a line to silence a false-positive secret hit
(re-check the value first).

## Before every push

If your change touches any of:
- `server.js`
- `client-app.js`
- `shared/scoring.js`
- `shared/xsuaa.js`
- `shared/constants.js`

Then in addition to `npm test`, run a quick `node --check` on the
modified file:

```bash
node --check server.js
```

The smoke test (`npm run test:smoke`) boots the server against the real
HANA instance and exercises login + code generation. **Only run it if
you have working HANA creds** (it will fail loudly with a clear error
otherwise, so it's safe to attempt).

## Branch + commit

- Branch off `main` for non-trivial work: `git checkout -b <slug>`
- Keep commits atomic. Subject line in Conventional Commits style
  (`feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `ops:`, `chore:`).
- Body explains **why**, not what. The diff shows what.
- One concern per commit. Mixed-concern commits are hard to revert.

## Deploy to BTP

This project does not have a CI pipeline, so deploys are explicit and
operator-driven. The procedure is documented in `AGENTS.md` under
"Deployment status". Short version:

```bash
cf env itil4-evalapp                # snapshot current env vars
# build a vars file with those values
cf push itil4-evalapp --vars-file /tmp/cf-vars.yml
cf app itil4-evalapp               # verify running
cf logs itil4-evalapp --recent     # check startup
rm /tmp/cf-vars.yml                # contains password
```

**Do not** use `deploy_btp.sh` for the current shape of the repo — that
script reads HANA vars from `.env`, which is stale; using it would
overwrite the working BTP env vars with the dev defaults.

## Gotchas

- **The `.env` is stale by design.** HANA DBADMIN's password was rotated
  in 2026-08 and the local `.env` was not updated. To connect to prod
  HANA from a script, pass `HANA_PASSWORD=<value>` on the command line
  — the env-var value wins over `.env`. See `scripts/inspect-hana.mjs`
  for an example.
- **XSUAA service instance is in BTP, not in code.** The config is in
  `xs-security.json`. If you change scopes there, recreate the
  service instance via `cf create-service xsuaa application <name> -c
  xs-security.json`.
- **Buildpack is pinned to v1.9.1.** BTP's default `nodejs_buildpack`
  resolves to 1.9.2 which fails on cflinuxfs4. The pin is in
  `manifest.yml`.
- **The app is 100% additive.** The 4 roles in `ROLE_PERMISSIONS` map
  1-to-1 to the 4 XSUAA scopes (`$XSAPPNAME.{admin,manager,reviewer,
  content_editor}`). When adding a new permission, add it to the
  shared `permissions` set in `shared/constants.js` and to the
  appropriate scope's role collection description in `xs-security.json`.
