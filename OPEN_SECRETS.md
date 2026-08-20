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
- **Status**: rotated — **`HANA_ITIL_EXAM_ADMIN_PASSWORD` was rotated in BTP on 2026-08-18** (per operator confirmation). The new value is held by the operator in the BTP credential store. The literal value of the OLD password still exists in commit `b06518d` (and briefly in `13e156b`) but has no operational value going forward.
- **Remaining work** (post-rotation, in priority order):
  1. **Audit the leak window.** Investigate the HANA Cloud audit trail for `ITIL_EXAM_ADMIN` connections between 2026-08-18 (the leak) and the rotation time. Look for connection attempts from non-CF source IPs. The CF egress IP range is documented; everything else is suspect. (This is the actual remaining operational work — the rotation itself is done.)
       - SAP BTP Cockpit → HANA Cloud → your instance → "Audit Trail" / "Security" tab.
       - Or: connect as DBADMIN and query
         `SELECT * FROM SYS.AUDIT_LOG WHERE USER_NAME = 'ITIL_EXAM_ADMIN'
         AND EVENT_TIMESTAMP > '2026-08-18' ORDER BY EVENT_TIMESTAMP DESC;`
  2. **Optional but low priority: rewrite git history.** The old secret is rotated, so urgency is gone. If you want full hygiene later, use `git filter-repo --invert-paths --path AGENTS.md` (or `git filter-repo --replace-text expressions.txt` with the secret in `expressions.txt`) + force-push. Coordinate before doing it because changing commit SHAs breaks any clone/fork.
  3. **Optional: enable branch protection on `main`** (GitHub-side, Settings → Branches → main → Require PR + disallow force push). The pre-commit secret scanner caught the re-introduction in `13e156b` immediately; a server-side gate is a second line of defense.
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
