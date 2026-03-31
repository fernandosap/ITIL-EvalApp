# Deploy ITIL Eval App to SAP BTP (Cloud Foundry)

This app is configured as a Node.js app and deploys with Cloud Foundry `nodejs_buildpack`.

## Prerequisites

- SAP BTP subaccount with Cloud Foundry enabled
- `cf` CLI installed
- Access to your target org/space

## Deployment files

- `index.html` (exam frontend)
- `server.js` (Node middleware + HANA API)
- `manifest.yml` (CF app config)
- `.cfignore` (exclude local metadata)
- `deploy_btp.sh` (deploy script with env + push)

## Deploy

1. Log in to your SAP BTP CF landscape:

```bash
cf login -a https://api.cf.<region>.hana.ondemand.com
```

Example endpoints:
- EU10: `https://api.cf.eu10.hana.ondemand.com`
- US10: `https://api.cf.us10-001.hana.ondemand.com`

2. Target org and space:

```bash
cf target -o <your-org> -s <your-space>
```

3. Export HANA env vars:

```bash
export HANA_HOST=<host>
export HANA_PORT=443
export HANA_USER=<user>
export HANA_PASSWORD=<password>
export HANA_SCHEMA=ITIL_EXAM
export HANA_ENCRYPT=true
export HANA_SSL_VALIDATE_CERTIFICATE=false
```

Optional (server-side AI proctoring via Anthropic):

```bash
export ANTHROPIC_API_KEY=<your-key>
export ANTHROPIC_MODEL=claude-sonnet-4-20250514
export ANTHROPIC_VERSION=2023-06-01
```

4. Deploy:

```bash
./deploy_btp.sh --domain <your-default-domain>
```

If `ANTHROPIC_API_KEY` is present, the script sets Anthropic env vars on the app and restages automatically.

5. Get route:

```bash
cf app itil4-evalapp
```

Open the generated route in your browser.

## Stable URL behavior

This project is configured with a fixed public hostname in `manifest.yml` and `deploy_btp.sh`.

Default canonical URL:

- `https://academycd-evalapp.cfapps.us10.hana.ondemand.com`

Route template:

```yaml
routes:
  - route: ((route_host)).((default_domain))
```

`deploy_btp.sh` passes `route_host` automatically and defaults it to `academycd-evalapp`.

If you ever need a different hostname for a one-off deploy, override it like this:

```bash
ROUTE_HOST=my-other-host ./deploy_btp.sh --domain <your-default-domain>
```

## Notes

- App and API are served from the same domain, so no CORS setup is needed.
- Sessions/results/codes are persisted in HANA through the Node API.
- The question bank is also stored in HANA and is no longer committed in the active app code.

## Migration (Admin Notes)

If you are adopting the newer HTML admin functionality with seat notes, run:

- [`migrations/2026-03-18_add_notes_to_access_codes.sql`](/Users/I848070/Documents/Github/ITIL-EvalApp/migrations/2026-03-18_add_notes_to_access_codes.sql)

This migration is idempotent and only adds `ACCESS_CODES.NOTES` if it does not already exist.

## Migration (Question Bank)

Run this once in HANA to create the secure question storage table:

- [2026-03-28_create_exam_questions.sql](/Users/I848070/Documents/Github/ITIL-EvalApp/migrations/2026-03-28_create_exam_questions.sql)

The app now reads questions and answer keys from `EXAM_QUESTIONS`.

## Secure Question Updates

When updating exam content in the future:

1. Do not embed questions, answers, access codes, or admin hashes in `index.html` or any standalone HTML file.
2. Do not commit the active question bank into the repository.
3. Load updated questions directly into `ITIL_EXAM.EXAM_QUESTIONS` in HANA.
4. Deploy the app after the HANA content update if backend behavior changes.
