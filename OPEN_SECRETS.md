# Open secret incidents

This file tracks credentials that have been exposed in the git history
or in a published artifact, and what the response plan is. Once a
secret has been rotated (and confirmed not in use), the entry can be
moved to `RESOLVED_SECRETS.md`.

Format:

```
## YYYY-MM-DD — <one-line summary>

- **Secret**: what was exposed
- **Where**: file:line, commit SHA, or artifact URL
- **Public?**: yes / no (was this reachable on the public internet?)
- **Status**: open / rotated / under review
- **Action**: concrete next step the operator needs to take
- **Mitigation**: code change (if any) that prevents recurrence
```

---

## 2026-08-19 — HANA prod password in AGENTS.md

- **Secret**: `SAPacademy_ITIL_EXAM_2026!` (BTP HANA `ITIL_EXAM_ADMIN` user)  # secretscan: ok
- **Where**: `AGENTS.md` line 377, commit `b06518d` ("ops(deploy): pin
  buildpack to v1.9.1 and document live BTP state"), pushed to
  `github.com/fernandosap/ITIL-EvalApp` (public repo) on 2026-08-18.
- **Public?**: yes (the GitHub repo is public)
- **Status**: open — **the secret must be rotated in BTP ASAP**
- **Action for the operator**:
  1. In SAP BTP Cockpit → HANA Cloud → your instance → "Reset
     Administrator Password" for the `ITIL_EXAM_ADMIN` user. Pick a
     new strong password and store it in the BTP credential store
     (NOT a new `cf set-env` plain-text value).
  2. Verify the new password works by hitting
     `https://academycd-evalapp.cfapps.us10.hana.ondemand.com/api/health`
     after the rotation propagates (~30s typically).
  3. Audit `cf logs itil4-evalapp --recent` for any suspicious
     connections between 2026-08-18 (the leak) and the rotation
     date. Look for non-CF IP addresses hitting the HANA port.
  4. Optional but recommended: rewrite git history to remove the
     secret entirely (`git filter-repo --invert-paths --path
     AGENTS.md` + force-push) so future clones of the public repo
     don't see it. This is destructive — coordinate before doing
     it because it changes commit SHAs.
- **Mitigation in code**: the offending line was removed from
  `AGENTS.md` on 2026-08-19 (commit `XXXX`). The HANA password no
  longer appears in the working tree or HEAD. Going forward, the
  pre-commit secret scanner (`.githooks/secret-patterns.txt`)
  already includes a pattern that matches `SAPacademy_*_2026!`
  and would have caught this if a scanner was in place before
  the commit landed. The scanner does NOT scan historic
  commits, so any future operator must run
  `git log -p -S 'SAPacademy'` manually if they suspect a leak.
- **Related**: an earlier commit in the same area
  (`AGENTS.md` "HANA findings" section) referenced
  \`WelcomeWelcome1.\` (the stale DBADMIN password). That one  # secretscan: ok
  is already rotated; documented for context only.  # secretscan: ok
