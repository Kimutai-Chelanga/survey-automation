#!/usr/bin/env bash
# =============================================================================
# pre-build-setup.sh
# Run this ONCE before your first "docker compose build && docker compose up"
# and again after any fresh clone on a new server.
#
# Usage:
#   chmod +x pre-build-setup.sh
#   ./pre-build-setup.sh
# =============================================================================

# NOTE: We deliberately do NOT use "set -e" here because chmod/chown on
# pre-existing root-owned directories will fail for non-root users on shared
# servers. That is acceptable — the Docker volume-init service handles those
# cases inside the containers at runtime.
set -uo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

# Run chmod, fall back to sudo if permission denied, warn (but don't fail) if both fail.
safe_chmod() {
    local mode="$1"
    local target="$2"
    if chmod "$mode" "$target" 2>/dev/null; then
        return 0
    fi
    echo "     ⚠  chmod $mode $target failed as $(whoami) — trying sudo..."
    if sudo chmod "$mode" "$target" 2>/dev/null; then
        echo "     ✓  sudo chmod $mode $target succeeded"
        return 0
    fi
    echo "     ⚠  Could not chmod $target — Docker volume-init will handle this at runtime"
    return 0   # non-fatal: Docker init service fixes this inside containers
}

# mkdir -p then safe_chmod
ensure_dir() {
    local dir="$1"
    local mode="${2:-755}"
    if ! mkdir -p "$dir" 2>/dev/null; then
        sudo mkdir -p "$dir" 2>/dev/null || true
    fi
    safe_chmod "$mode" "$dir"
}

# ─────────────────────────────────────────────────────────────────────────────
echo "========================================================================"
echo " Pre-Build Permission & Environment Setup"
echo " Running as: $(whoami)  (UID=$(id -u))"
echo "========================================================================"

# ── 1. Host-side bind-mount directories ──────────────────────────────────────
echo ""
echo "1/5  Creating host-side bind-mount directories..."

ensure_dir ./nginx/logs        755
ensure_dir ./nginx/ssl         755
ensure_dir ./airflow/dags      755
ensure_dir ./airflow/scripts   755
ensure_dir ./host_downloads    777
ensure_dir ./editthiscookie    755

echo "     ✓ Host directories ready"

# ── 2. Check .env.production ─────────────────────────────────────────────────
echo ""
echo "2/5  Checking .env.production..."

if [ ! -f .env.production ]; then
    echo "     ✗ .env.production not found!"
    echo "       Copy .env.example to .env.production and fill in the values."
    exit 1
fi

REQUIRED_KEYS=(
    AIRFLOW_FERNET_KEY
    AIRFLOW_WEBSERVER_SECRET_KEY
    AIRFLOW_UID
    AIRFLOW_GID
    APP_PASSWORD_HASH
)

MISSING=()
for key in "${REQUIRED_KEYS[@]}"; do
    if ! grep -q "^${key}=" .env.production; then
        MISSING+=("$key")
    fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
    echo "     ✗ Missing required keys in .env.production:"
    for k in "${MISSING[@]}"; do echo "       - $k"; done
    echo ""
    echo "     Generate AIRFLOW_FERNET_KEY:"
    echo "       python3 -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    echo ""
    echo "     Generate AIRFLOW_WEBSERVER_SECRET_KEY:"
    echo "       python3 -c \"import secrets; print(secrets.token_hex(32))\""
    echo ""
    echo "     Set AIRFLOW_UID / AIRFLOW_GID:"
    echo "       echo AIRFLOW_UID=\$(id -u) >> .env.production"
    echo "       echo AIRFLOW_GID=0         >> .env.production"
    exit 1
fi

echo "     ✓ .env.production looks good"

# ── 3. Auto-fix missing AIRFLOW_UID / AIRFLOW_GID ────────────────────────────
echo ""
echo "3/5  Checking AIRFLOW_UID / AIRFLOW_GID..."

AIRFLOW_UID_VAL=$(grep "^AIRFLOW_UID=" .env.production | cut -d= -f2 || true)
AIRFLOW_GID_VAL=$(grep "^AIRFLOW_GID=" .env.production | cut -d= -f2 || true)

if [ -z "${AIRFLOW_UID_VAL}" ]; then
    echo "     ⚠  AIRFLOW_UID not set — appending UID=$(id -u)..."
    echo "AIRFLOW_UID=$(id -u)" >> .env.production
fi
if [ -z "${AIRFLOW_GID_VAL}" ]; then
    echo "     ⚠  AIRFLOW_GID not set — appending 0..."
    echo "AIRFLOW_GID=0" >> .env.production
fi

echo "     ✓ AIRFLOW_UID=$(grep '^AIRFLOW_UID=' .env.production | cut -d= -f2)"
echo "     ✓ AIRFLOW_GID=$(grep '^AIRFLOW_GID=' .env.production | cut -d= -f2)"

# ── 4. Stale Docker state check ───────────────────────────────────────────────
echo ""
echo "4/5  Checking for stale Docker state..."

RUNNING=$(docker compose ps -q 2>/dev/null | wc -l | tr -d ' ')
if [ "$RUNNING" -gt 0 ]; then
    echo "     ⚠  ${RUNNING} container(s) still running."
    echo "        Stop them first:          docker compose down"
    echo "        Or wipe volumes entirely: CLEAN_VOLUMES=1 docker compose down -v"
else
    if [ "${CLEAN_VOLUMES:-0}" = "1" ]; then
        echo "     CLEAN_VOLUMES=1 — removing all named volumes for a fresh start..."
        docker compose down -v --remove-orphans 2>/dev/null || true
        echo "     ✓ Volumes removed"
    else
        echo "     ✓ No running containers"
        echo "       (Tip: CLEAN_VOLUMES=1 ./pre-build-setup.sh for a completely fresh start)"
    fi
fi

# ── 5. Pull base images ───────────────────────────────────────────────────────
echo ""
echo "5/5  Pulling base images (speeds up build)..."
docker pull apache/airflow:2.7.1-python3.10 2>/dev/null || echo "     ⚠  Could not pull airflow base"
docker pull busybox:latest                  2>/dev/null || echo "     ⚠  Could not pull busybox"
docker pull mongo:7.0                       2>/dev/null || echo "     ⚠  Could not pull mongo"
docker pull postgres:15                     2>/dev/null || echo "     ⚠  Could not pull postgres"
docker pull nginx:alpine                    2>/dev/null || echo "     ⚠  Could not pull nginx"
echo "     ✓ Base images ready"

echo ""
echo "========================================================================"
echo " Pre-build setup complete."
echo ""
echo " Next steps:"
echo "   docker compose build --no-cache"
echo "   docker compose up -d"
echo ""
echo " Watch init logs:"
echo "   docker compose logs -f volume-init airflow-init"
echo ""
echo " Verify volumes are writable after startup:"
echo "   docker exec airflow_scheduler ls -la /workspace/chrome_profiles"
echo "   docker exec airflow_scheduler ls -la /workspace/recordings"
echo "   docker exec airflow_scheduler ls -la /workspace/downloads"
echo "========================================================================"
