# Testing strategy

ITIL-EvalApp uses layered tests so failures are caught at the cheapest useful level before a deployment reaches candidates.

## Test layers

### Unit and regression

```bash
npm run test:unit
```

Runs the Node test suite under `tests/*.test.js`. This covers scoring, authentication/RBAC helpers, signing, HANA/pool behavior, rate limiting, question-set integrity, media/proctor recovery, client error handling, multi-select rules, security regressions, and testing infrastructure.

### Browser E2E

The Playwright runner is pinned to the same version used by CI and is installed without modifying `package.json` or `package-lock.json`:

```bash
npm run test:e2e:setup
npx playwright install chromium
npm run test:e2e
```

Browser tests exercise candidate entry, consent, exam rendering, exact multi-select behavior, navigation/submission, accessibility contrast, refresh recovery, admin entry, and module-load recovery.

### Application smoke

```bash
npm run test:smoke
```

Exercises the application/API smoke path in test mode, including temporary fixture creation and guaranteed cleanup.

### Production dependency audit

```bash
npm run audit:prod
```

Fails on high-severity vulnerabilities reachable from production dependencies. Development-only tooling is intentionally excluded from this production gate.

### Secret scanning

```bash
npm run scan:secrets
npm run scan:secrets:staged
```

Both CI and the local pre-commit hook use the same JavaScript RegExp engine and the patterns in `.githooks/secret-patterns.txt`. This avoids differences between grep/awk regex dialects. The scanner fails closed when a configured pattern is invalid.

### Post-deploy smoke

After deploying to Cloud Foundry/BTP:

```bash
SMOKE_BASE_URL=https://your-app.example npm run test:postdeploy
```

The smoke is intentionally non-destructive. It validates the landing page, core security headers, request IDs, `/api/status`, and a critical client module. If a dedicated disposable smoke access code has been configured, it can also validate that code without starting an exam:

```bash
SMOKE_BASE_URL=https://your-app.example \
SMOKE_ACCESS_CODE=ABC234 \
npm run test:postdeploy
```

GitHub Actions also exposes **Post-deploy smoke** as a manually dispatched workflow. `POST_DEPLOY_SMOKE_ACCESS_CODE` is optional and should be stored as a GitHub Actions secret, never committed to the repository.

## CI gates

Every pull request targeting `main` runs:

1. `npm ci`
2. production dependency audit
3. unit/regression tests
4. syntax checks
5. repository secret scan
6. Playwright browser E2E in a separate job

Both jobs must be green before merging.

## Recommended release sequence

1. Open a PR from a feature branch.
2. Require both CI jobs to pass.
3. Merge to `main`.
4. Deploy the resulting `main` SHA to BTP/Cloud Foundry.
5. Run the **Post-deploy smoke** workflow against the deployed URL.
6. For larger releases, perform human UAT with multiple simultaneous candidates across the supported browser/OS matrix.

## UAT matrix for significant releases

At minimum, explicitly exercise:

- 5–10 simultaneous candidate sessions
- Chrome and Edge; Safari where supported
- Windows and macOS
- Wi-Fi interruption and recovery
- page refresh/resume during an exam
- webcam permission loss/recovery
- screen-share stop/recovery
- repeated tab switching
- submit retry/double-click behavior
- admin console while candidates are active
- Admin, Manager, Reviewer, and Content Editor role boundaries when test users are available

Automated testing reduces regression risk, but the UAT matrix remains valuable for browser permissions, operating-system behavior, corporate network policies, and real BTP/XSUAA integration.
