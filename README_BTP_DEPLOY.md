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

4. Deploy:

```bash
./deploy_btp.sh --domain <your-default-domain>
```

5. Get route:

```bash
cf app itil4-evalapp
```

Open the generated route in your browser.

## Stable URL behavior

This project is configured with a fixed route in `manifest.yml`:

```yaml
routes:
  - route: itil4-evalapp.((default_domain))
```

Use `--var default_domain=...` on deploy so the URL stays constant across pushes.

## Notes

- App and API are served from the same domain, so no CORS setup is needed.
- Sessions/results/codes are persisted in HANA through the Node API.

## Migration (Admin Notes)

If you are adopting the newer HTML admin functionality with seat notes, run:

- [`migrations/2026-03-18_add_notes_to_access_codes.sql`](/Users/I848070/Documents/Github/ITIL-EvalApp/migrations/2026-03-18_add_notes_to_access_codes.sql)

This migration is idempotent and only adds `ACCESS_CODES.NOTES` if it does not already exist.
