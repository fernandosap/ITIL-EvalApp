# Open secret incidents

This file tracks credentials that have been exposed in the git history
or in a published artifact, and what the response plan is. Once a
secret has been rotated (and confirmed not in use), the entry can be
moved to `RESOLVED_SECRETS.md`.

**Do not include the literal value of any secret in this file.** Refer
to the secret by a short identifier (e.g. `HANA_ITIL_EXAM_ADMIN_PASSWORD`,
`DBADMIN_DEV_PASSWORD`) and store the actual value only in the operator's
password manager / BTP credential store. The point of this file is to
document WHERE a secret was exposed, WHEN, and WHAT TO DO — not to
re-publish the secret in a new place.

Format:

```
## YYYY-MM-DD — <one-line summary>

- **Secret identifier**: short symbolic name (NOT the value)
- **Where**: file:line, commit SHA, or artifact URL
- **Public?**: yes / no (was this reachable on the public internet?)
- **Status**: open / rotated / under review
- **Action**: concrete next step the operator needs to take
- **Mitigation**: code change (if any) that prevents recurrence
```

---

## 2026-08-19 — HANA prod password in AGENTS.md

- **Secret identifier**: `HANA_ITIL_EXAM_ADMIN_PASSWORD` (the
  `ITIL_EXAM_ADMIN` user's HANA Cloud password, stored in BTP env
  var `HANA_PASSWORD`). The literal value is in commit `b06518d` of
  this repo and is in git history until purged; it must NOT be
  reproduced in this file.
- **Where**: `AGENTS.md` line 377 in commit `b06518d` ("ops(deploy):
  pin buildpack to v1.9.1 and document live BTP state"), pushed to
  `github.com/fernandosap/ITIL-EvalApp` (public repo) on 2026-08-18.
  The literal value was also briefly re-published in this file in
  commit `13e156b` and removed again in the follow-up; the historic
  commit still contains it.
- **Public?**: yes (the GitHub repo is public)
- **Status**: open — **the secret must be rotated in BTP ASAP**
- **Action for the operator**:
  1. **Rotate the password in BTP first.** In SAP BTP Cockpit →
     HANA Cloud → your instance → "Reset Administrator Password"
     for the `ITIL_EXAM_ADMIN` user. Pick a new strong password
     and store it in the BTP credential store (NOT a new
     `cf set-env` plain-text value).
  2. Verify the new password works by hitting
     `https://academycd-evalapp.cfapps.us10.hana.ondemand.com/api/health`
     after the rotation propagates (~30s typically).
  3. **Investigate potential compromise via the HANA audit trail,
     not via `cf logs`.** The leaked credential is a direct HANA
     user, not an application-layer one. `cf logs itil4-evalapp`
     shows app-layer traffic and would not capture a direct HANA
     connection. The relevant investigation surface is the HANA
     Cloud audit log for the `ITIL_EXAM_ADMIN` user:
       - SAP BTP Cockpit → HANA Cloud → your instance → "Audit
         Trail" / "Security" tab.
       - Or: connect as DBADMIN and query
         `SELECT * FROM SYS.AUDIT_LOG WHERE USER_NAME = 'ITIL_EXAM_ADMIN'
         AND EVENT_TIMESTAMP > '2026-08-18' ORDER BY EVENT_TIMESTAMP DESC;`
       - Look for connection attempts from non-CF source IPs in
         the window between 2026-08-18 (the leak) and the rotation
         time. The CF egress IP range is documented; everything
         else is suspect.
  4. **Optional but low priority: rewrite git history.** Once the
     secret is rotated, the literal value has no operational value,
     so the urgency drops. If you want full hygiene later, use
     `git filter-repo --invert-paths --path AGENTS.md` (or
     `git filter-repo --replace-text expressions.txt` with the
     secret in `expressions.txt`) + force-push. Coordinate before
     doing it because changing commit SHAs breaks any clone/fork.
- **Mitigation in code**:
  - The offending line was removed from `AGENTS.md` on 2026-08-19
    (commit `13e156b`).
  - The pre-commit secret scanner
    (`.githooks/secret-patterns.txt`) includes a pattern matching
    the leaked secret's prefix (`SAPacademy_*_2026!`) and would
    block any future re-introduction. The scanner does NOT scan
    historic commits, so any future operator who suspects a new
    leak should run `git log -p -S 'SAPacademy'` manually.
  - **Process gap that allowed the leak**: the doc was written
    without running the pre-commit hook first. Operators editing
    `AGENTS.md` (or any other doc with env-var examples) should
    run `git config core.hooksPath .githooks` in their clone and
    let the hook scan the staged diff before committing.
- **Related (already rotated)**: an earlier commit referenced the
  stale local-dev DBADMIN password
  (`DBADMIN_DEV_PASSWORD`, the original `WelcomeWelcome`-style
  password that came in `.env` and was replaced on 2026-08-18).
  That one is already rotated; documented for context only.
