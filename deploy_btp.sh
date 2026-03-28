#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_NAME="itil4-evalapp"

usage() {
  cat <<'EOF'
Usage:
  ./deploy_btp.sh [--api <cf-api-endpoint>] [--org <org>] [--space <space>] [--domain <default-domain>] [--env-file <file>] [--sso]

Example:
  ./deploy_btp.sh \
    --api https://api.cf.us10-001.hana.ondemand.com \
    --org my-org \
    --space dev \
    --domain cfapps.us10-001.hana.ondemand.com

Notes:
  - This script expects manifest.yml in the same directory.
  - If you are already logged in and targeted, you can run it with no args.
  - It auto-detects API/org/space from `cf target` when omitted.
  - It auto-detects a shared external domain from `cf domains` when omitted.
  - If your landscape requires SSO, add --sso to force interactive SSO login.
  - HANA vars can be exported or loaded from --env-file/.deploy.env/.env.
  - Required HANA vars:
    HANA_HOST, HANA_PORT, HANA_USER, HANA_PASSWORD, HANA_SCHEMA,
    HANA_ENCRYPT, HANA_SSL_VALIDATE_CERTIFICATE
  - Recommended admin auth var:
    ADMIN_HASH
  - Optional Anthropic vars for AI proctoring:
    ANTHROPIC_API_KEY, ANTHROPIC_MODEL, ANTHROPIC_VERSION
EOF
}

CF_API=""
CF_ORG=""
CF_SPACE=""
DEFAULT_DOMAIN=""
USE_SSO="false"
ENV_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api)
      CF_API="${2:-}"; shift 2 ;;
    --org)
      CF_ORG="${2:-}"; shift 2 ;;
    --space)
      CF_SPACE="${2:-}"; shift 2 ;;
    --domain)
      DEFAULT_DOMAIN="${2:-}"; shift 2 ;;
    --sso)
      USE_SSO="true"; shift ;;
    --env-file)
      ENV_FILE="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1 ;;
  esac
done

if ! command -v cf >/dev/null 2>&1; then
  echo "Cloud Foundry CLI (cf) is not installed or not in PATH." >&2
  exit 1
fi

if [[ ! -f "$APP_DIR/manifest.yml" ]]; then
  echo "manifest.yml not found in: $APP_DIR" >&2
  exit 1
fi

cd "$APP_DIR"

if [[ -n "$ENV_FILE" ]]; then
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Env file not found: $ENV_FILE" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  set -a; source "$ENV_FILE"; set +a
elif [[ -f "$APP_DIR/.deploy.env" ]]; then
  # shellcheck disable=SC1091
  set -a; source "$APP_DIR/.deploy.env"; set +a
elif [[ -f "$APP_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  set -a; source "$APP_DIR/.env"; set +a
fi

# Try to infer missing context from current CF target
TARGET_OUT="$(cf target 2>/dev/null || true)"

if [[ -z "$CF_API" ]]; then
  CF_API="$(printf '%s\n' "$TARGET_OUT" | awk -F': *' '/^API endpoint:/ {print $2; exit}')"
fi
if [[ -z "$CF_ORG" ]]; then
  CF_ORG="$(printf '%s\n' "$TARGET_OUT" | awk -F': *' '/^org:/ {print $2; exit}')"
fi
if [[ -z "$CF_SPACE" ]]; then
  CF_SPACE="$(printf '%s\n' "$TARGET_OUT" | awk -F': *' '/^space:/ {print $2; exit}')"
fi

if [[ -z "$DEFAULT_DOMAIN" ]]; then
  DOMAINS="$(cf domains 2>/dev/null || true)"
  # Prefer standard app domain first (e.g., cfapps.us10.hana.ondemand.com), avoid cert.* and internal.
  DEFAULT_DOMAIN="$(
    printf '%s\n' "$DOMAINS" \
      | awk '$1 != "name" && $1 != "Getting" && $1 != "apps.internal" && $1 !~ /^cert\./ && $2 == "shared" {print $1; exit}'
  )"
  if [[ -z "$DEFAULT_DOMAIN" ]]; then
    DEFAULT_DOMAIN="$(
      printf '%s\n' "$DOMAINS" \
        | awk '$1 != "name" && $1 != "Getting" && $1 != "apps.internal" && $2 == "shared" {print $1; exit}'
    )"
  fi
fi

if [[ -z "$CF_API" || -z "$CF_ORG" || -z "$CF_SPACE" || -z "$DEFAULT_DOMAIN" ]]; then
  echo "Missing deployment context and could not auto-detect all values." >&2
  usage
  exit 1
fi

REQUIRED_HANA_VARS=(
  HANA_HOST
  HANA_PORT
  HANA_USER
  HANA_PASSWORD
  HANA_SCHEMA
  HANA_ENCRYPT
  HANA_SSL_VALIDATE_CERTIFICATE
)
for v in "${REQUIRED_HANA_VARS[@]}"; do
  if [[ -z "${!v:-}" ]]; then
    echo "Missing required environment variable: $v" >&2
    usage
    exit 1
  fi
done

if [[ "$USE_SSO" == "true" ]]; then
  echo "==> Logging in with SSO to Cloud Foundry API: $CF_API"
  cf login -a "$CF_API" --sso
elif [[ -z "$TARGET_OUT" ]]; then
  echo "==> Logging in to Cloud Foundry API: $CF_API"
  cf login -a "$CF_API"
else
  echo "==> Using existing Cloud Foundry login session"
fi

echo "==> Targeting org/space: $CF_ORG / $CF_SPACE"
cf target -o "$CF_ORG" -s "$CF_SPACE"

echo "==> Deploying app with fixed route + HANA vars"
cf push \
  --var "default_domain=$DEFAULT_DOMAIN" \
  --var "hana_host=$HANA_HOST" \
  --var "hana_port=$HANA_PORT" \
  --var "hana_user=$HANA_USER" \
  --var "hana_password=$HANA_PASSWORD" \
  --var "hana_schema=$HANA_SCHEMA" \
  --var "hana_encrypt=$HANA_ENCRYPT" \
  --var "hana_ssl_validate_certificate=$HANA_SSL_VALIDATE_CERTIFICATE"

if [[ -n "${ADMIN_HASH:-}" ]]; then
  echo "==> Configuring ADMIN_HASH for secure admin login"
  cf set-env "$APP_NAME" ADMIN_HASH "$ADMIN_HASH"
  NEED_RESTAGE="true"
else
  NEED_RESTAGE="false"
  echo "==> ADMIN_HASH not set; admin login will be disabled until it is configured"
fi

if [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
  echo "==> Configuring Anthropic env vars for server-side proctoring"
  cf set-env "$APP_NAME" ANTHROPIC_API_KEY "$ANTHROPIC_API_KEY"
  if [[ -n "${ANTHROPIC_MODEL:-}" ]]; then
    cf set-env "$APP_NAME" ANTHROPIC_MODEL "$ANTHROPIC_MODEL"
  fi
  if [[ -n "${ANTHROPIC_VERSION:-}" ]]; then
    cf set-env "$APP_NAME" ANTHROPIC_VERSION "$ANTHROPIC_VERSION"
  fi
  NEED_RESTAGE="true"
else
  echo "==> ANTHROPIC_API_KEY not set; AI proctoring endpoint will stay disabled"
fi

if [[ "$NEED_RESTAGE" == "true" ]]; then
  echo "==> Restaging app to apply environment variable changes"
  cf restage "$APP_NAME"
fi

echo "==> Deployment complete. App details:"
cf app "$APP_NAME"
