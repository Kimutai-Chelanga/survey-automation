#!/bin/bash
# ============================================================
# final-automation — Full Auto Setup Script
# Run this on a fresh Ubuntu VPS to get everything running
# Usage: bash setup.sh
# ============================================================

set -e  # Exit on any error

# ─────────────────────────────────────────────────────────
# COLORS
# ─────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log()    { echo -e "${GREEN}[✓]${NC} $1"; }
warn()   { echo -e "${YELLOW}[!]${NC} $1"; }
error()  { echo -e "${RED}[✗]${NC} $1"; exit 1; }
section(){ echo -e "\n${CYAN}══════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}══════════════════════════════════════${NC}"; }

# ─────────────────────────────────────────────────────────
# 0. MUST RUN AS ROOT
# ─────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
  error "Please run as root: sudo bash setup.sh"
fi

section "1. System Update & Dependencies"

apt-get update -y
apt-get install -y \
  curl wget git unzip gnupg2 \
  ca-certificates lsb-release \
  software-properties-common \
  ufw fail2ban \
  htop net-tools

log "System packages installed"

# ─────────────────────────────────────────────────────────
# 1. INSTALL DOCKER
# ─────────────────────────────────────────────────────────
section "2. Installing Docker & Docker Compose"

if command -v docker &>/dev/null; then
  warn "Docker already installed: $(docker --version)"
else
  curl -fsSL https://get.docker.com | bash
  log "Docker installed"
fi

# Docker Compose v2 (plugin)
if docker compose version &>/dev/null; then
  warn "Docker Compose already available: $(docker compose version)"
else
  apt-get install -y docker-compose-plugin
  log "Docker Compose plugin installed"
fi

systemctl enable docker
systemctl start docker
log "Docker service enabled and started"

# ─────────────────────────────────────────────────────────
# 2. CLONE THE REPO
# ─────────────────────────────────────────────────────────
section "3. Cloning Repository"

REPO_URL="git@github.com:Kimutai-Chelanga/final-automation.git"
PROJECT_DIR="/opt/final-automation"

if [[ -d "$PROJECT_DIR/.git" ]]; then
  warn "Repo already exists at $PROJECT_DIR — pulling latest..."
  cd "$PROJECT_DIR"
  git pull origin main || git pull origin dev || warn "Could not pull — check your SSH key or branch"
else
  echo ""
  echo "Choose clone method:"
  echo "  1) SSH  (recommended if SSH key is set up on GitHub)"
  echo "  2) HTTPS (will ask for GitHub token)"
  read -rp "Enter 1 or 2: " CLONE_METHOD

  if [[ "$CLONE_METHOD" == "1" ]]; then
    git clone "$REPO_URL" "$PROJECT_DIR"
  else
    read -rp "Enter your GitHub username: " GH_USER
    read -rsp "Enter your GitHub personal access token: " GH_TOKEN
    echo ""
    git clone "https://${GH_USER}:${GH_TOKEN}@github.com/Kimutai-Chelanga/final-automation.git" "$PROJECT_DIR"
  fi
  log "Repository cloned to $PROJECT_DIR"
fi

cd "$PROJECT_DIR"

# ─────────────────────────────────────────────────────────
# 3. GENERATE .env.production
# ─────────────────────────────────────────────────────────
section "4. Setting Up Environment File"

ENV_FILE="$PROJECT_DIR/.env.production"

if [[ -f "$ENV_FILE" ]]; then
  warn ".env.production already exists — skipping generation"
  warn "Edit it manually at: $ENV_FILE"
else
  # Generate secrets
  FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || \
               python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
  SECRET_KEY=$(openssl rand -hex 32)
  MONGO_PASS=$(openssl rand -hex 16)
  PG_PASS=$(openssl rand -hex 16)
  VNC_PASS=$(openssl rand -hex 8)

  # Get server IP
  SERVER_IP=$(curl -s ifconfig.me || hostname -I | awk '{print $1}')

  cat > "$ENV_FILE" <<EOF
# ── Database ──────────────────────────────────────────────
POSTGRES_USER=airflow
POSTGRES_PASSWORD=${PG_PASS}
POSTGRES_DB=messages

MONGO_USERNAME=admin
MONGO_PASSWORD=${MONGO_PASS}

# ── Airflow ───────────────────────────────────────────────
AIRFLOW_FERNET_KEY=${FERNET_KEY}
AIRFLOW_WEBSERVER_SECRET_KEY=${SECRET_KEY}
AIRFLOW_UID=50000
AIRFLOW_GID=0

# ── App Auth ──────────────────────────────────────────────
APP_USERNAME=admin
APP_PASSWORD_HASH=
SESSION_TIMEOUT=480

# ── VNC ───────────────────────────────────────────────────
VNC_PASSWORD=${VNC_PASS}

# ── Timezone & Scheduling ─────────────────────────────────
DAG_TIMEZONE=Africa/Nairobi
DAG_SCHEDULE=0 16 * * 0
DAG_START_DATE=2025-07-01

# ── API Keys (fill these in) ──────────────────────────────
GEMINI_API_KEY=
OPENAI_API_KEY=
HYPERBROWSER_API_KEY=

# ── Email (for Airflow alerts) ────────────────────────────
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=true
EMAIL_USERNAME=
EMAIL_PASSWORD=
EMAIL_FROM=
EMAIL_TO=

# ── Twitter / X ───────────────────────────────────────────
TWITTER_CONSUMER_KEY=
TWITTER_CONSUMER_SECRET=
TWITTER_ACCESS_TOKEN_KEY=
TWITTER_ACCESS_TOKEN_SECRET=

# ── Backup Settings ───────────────────────────────────────
BACKUP_MAX_SIZE_MB=25
BACKUP_RETENTION_DAYS=7

# ── Server ────────────────────────────────────────────────
SERVER_IP=${SERVER_IP}
EOF

  log ".env.production generated at $ENV_FILE"
  warn "IMPORTANT: Open $ENV_FILE and fill in your API keys and email settings before starting"
fi

# ─────────────────────────────────────────────────────────
# 4. GENERATE APP PASSWORD HASH
# ─────────────────────────────────────────────────────────
section "5. Setting App Password"

echo ""
read -rsp "Set a password for the Streamlit app (admin user): " APP_PASS
echo ""

HASH=$(python3 -c "import hashlib; print(hashlib.sha256('${APP_PASS}'.encode()).hexdigest())" 2>/dev/null || echo "")

if [[ -n "$HASH" ]]; then
  sed -i "s/^APP_PASSWORD_HASH=.*/APP_PASSWORD_HASH=${HASH}/" "$ENV_FILE"
  log "App password hash saved"
else
  warn "Could not generate hash automatically — run: python3 generate_password_hash.py"
fi

# ─────────────────────────────────────────────────────────
# 5. SET UP NGINX DIRECTORIES & SSL
# ─────────────────────────────────────────────────────────
section "6. Preparing Nginx & SSL"

mkdir -p "$PROJECT_DIR/nginx/ssl"
mkdir -p "$PROJECT_DIR/nginx/logs"

# Self-signed cert if none exists
CERT="$PROJECT_DIR/nginx/ssl/cert.pem"
KEY="$PROJECT_DIR/nginx/ssl/key.pem"

if [[ ! -f "$CERT" ]]; then
  SERVER_IP=$(curl -s ifconfig.me || hostname -I | awk '{print $1}')
  openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout "$KEY" \
    -out "$CERT" \
    -subj "/CN=${SERVER_IP}/O=FinalAutomation/C=KE" \
    2>/dev/null
  log "Self-signed SSL certificate generated"
else
  warn "SSL cert already exists — skipping"
fi

# ─────────────────────────────────────────────────────────
# 6. CONFIGURE FIREWALL
# ─────────────────────────────────────────────────────────
section "7. Configuring Firewall (UFW)"

ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw allow 80/tcp    # HTTP
ufw allow 443/tcp   # HTTPS
ufw allow 8443/tcp  # Alternative HTTPS (Airflow via nginx)
ufw --force enable

log "Firewall configured"

# ─────────────────────────────────────────────────────────
# 7. INSTALL PYTHON DEPS (for helper scripts)
# ─────────────────────────────────────────────────────────
section "8. Installing Python (for scripts)"

if ! command -v python3 &>/dev/null; then
  apt-get install -y python3 python3-pip
fi

pip3 install cryptography --break-system-packages 2>/dev/null || true

log "Python ready"

# ─────────────────────────────────────────────────────────
# 8. FIX PERMISSIONS ON PROJECT DIR
# ─────────────────────────────────────────────────────────
section "9. Fixing Permissions"

chmod +x "$PROJECT_DIR"/*.sh 2>/dev/null || true
chmod 600 "$ENV_FILE"

log "Permissions set"

# ─────────────────────────────────────────────────────────
# 9. BUILD & START DOCKER SERVICES
# ─────────────────────────────────────────────────────────
section "10. Building & Starting Docker Services"

cd "$PROJECT_DIR"

echo ""
read -rp "Start Docker services now? (y/n): " START_NOW

if [[ "$START_NOW" == "y" || "$START_NOW" == "Y" ]]; then

  # Use prod compose if available, fallback to default
  COMPOSE_FILE="docker-compose.prod.yml"
  [[ ! -f "$COMPOSE_FILE" ]] && COMPOSE_FILE="docker-compose.yml"

  log "Using compose file: $COMPOSE_FILE"

  docker compose -f "$COMPOSE_FILE" pull --ignore-pull-failures 2>/dev/null || true
  docker compose -f "$COMPOSE_FILE" build

  # Start dependencies first
  docker compose -f "$COMPOSE_FILE" up -d postgres mongodb
  log "Waiting 20s for databases to be healthy..."
  sleep 20

  # Run volume-init and airflow-init
  docker compose -f "$COMPOSE_FILE" up volume-init
  docker compose -f "$COMPOSE_FILE" up airflow-init

  # Start everything else
  docker compose -f "$COMPOSE_FILE" up -d

  log "All services started!"
else
  warn "Skipped. To start manually, run:"
  echo "  cd $PROJECT_DIR && docker compose -f docker-compose.prod.yml up -d"
fi

# ─────────────────────────────────────────────────────────
# 10. SUMMARY
# ─────────────────────────────────────────────────────────
section "✅ Setup Complete!"

SERVER_IP=$(curl -s ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')

echo ""
echo -e "${GREEN}Your services should be available at:${NC}"
echo "  Streamlit App  →  https://${SERVER_IP}"
echo "  Airflow UI     →  https://${SERVER_IP}:8443"
echo ""
echo -e "${YELLOW}Important next steps:${NC}"
echo "  1. Fill in API keys in: $ENV_FILE"
echo "  2. Set up your domain + real SSL if needed"
echo "  3. Check running services: docker compose ps"
echo "  4. View logs: docker compose logs -f airflow-scheduler"
echo ""
echo -e "${CYAN}Project directory: $PROJECT_DIR${NC}"
echo ""
