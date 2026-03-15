#!/bin/bash

# generate_env.sh
# Production .env.production file generator with all required variables
# Usage: ./generate_env.sh

set -e

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Production Environment Configuration Generator         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check if .env.production already exists
if [ -f ".env.production" ]; then
    echo "⚠️  WARNING: .env.production file already exists!"
    read -p "Do you want to overwrite it? (yes/no): " overwrite
    if [ "$overwrite" != "yes" ]; then
        echo "Aborted. No changes made."
        exit 0
    fi
    # Backup existing .env.production
    backup_name=".env.production.backup.$(date +%Y%m%d_%H%M%S)"
    cp .env.production "$backup_name"
    echo "✓ Existing .env.production backed up to: $backup_name"
    echo ""
fi

# Function to generate random password
generate_password() {
    local length=$1
    LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c ${length}
}

# Function to generate Fernet key for Airflow
generate_fernet_key() {
    python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || \
    echo "46BKJoQYlPkex6sjeX8L6J0Df0qy7l1i6V8z9Aqo3Ys="
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 1: Generate Secure Passwords"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Generating secure random passwords..."

MONGO_PASSWORD=$(generate_password 32)
POSTGRES_PASSWORD=$(generate_password 32)
VNC_PASSWORD=$(generate_password 16)
AIRFLOW_WEBSERVER_SECRET_KEY=$(generate_password 50)
AIRFLOW_FERNET_KEY=$(generate_fernet_key)

# Airflow admin credentials — fixed values
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=kimu

echo "✓ Passwords generated"
echo "✓ Airflow admin credentials set (username: admin)"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 2: User Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Streamlit admin username only (no password)
read -p "Streamlit admin username [admin]: " APP_USERNAME
APP_USERNAME=${APP_USERNAME:-admin}

echo ""

# Email configuration
read -p "Admin email address [admin@localhost]: " ADMIN_EMAIL
ADMIN_EMAIL=${ADMIN_EMAIL:-admin@localhost}

echo ""
echo "Email notifications configuration (optional - press Enter to skip):"
read -p "SMTP email address (e.g., your-email@gmail.com): " EMAIL_USERNAME
read -p "SMTP email password: " EMAIL_PASSWORD

if [ -z "$EMAIL_USERNAME" ]; then
    EMAIL_USERNAME=""
    EMAIL_PASSWORD=""
    EMAIL_FROM="noreply@localhost"
    EMAIL_TO="admin@localhost"
    echo "✓ Email notifications disabled"
else
    EMAIL_FROM=${EMAIL_USERNAME}
    EMAIL_TO=${EMAIL_USERNAME}
    echo "✓ Email notifications configured"
fi

echo ""

# Twitter API (optional)
echo "Twitter API configuration (optional - press Enter to skip all):"
read -p "Twitter Consumer Key: " TWITTER_CONSUMER_KEY
read -p "Twitter Consumer Secret: " TWITTER_CONSUMER_SECRET
read -p "Twitter Access Token: " TWITTER_ACCESS_TOKEN_KEY
read -p "Twitter Access Token Secret: " TWITTER_ACCESS_TOKEN_SECRET

if [ -z "$TWITTER_CONSUMER_KEY" ]; then
    TWITTER_CONSUMER_KEY=""
    TWITTER_CONSUMER_SECRET=""
    TWITTER_ACCESS_TOKEN_KEY=""
    TWITTER_ACCESS_TOKEN_SECRET=""
    echo "✓ Twitter API disabled"
else
    echo "✓ Twitter API configured"
fi

echo ""

# API Keys (optional)
echo "AI API Keys (optional - press Enter to skip):"
read -p "Gemini API Key: " GEMINI_API_KEY
read -p "OpenAI API Key: " OPENAI_API_KEY
read -p "HyperBrowser API Key: " HYPERBROWSER_API_KEY

GEMINI_API_KEY=${GEMINI_API_KEY:-}
OPENAI_API_KEY=${OPENAI_API_KEY:-}
HYPERBROWSER_API_KEY=${HYPERBROWSER_API_KEY:-}

echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 3: Generating Configuration File"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Create the .env.production file
cat > .env.production << EOF
# ============================================================================
# Production Environment Variables
# Generated: $(date)
# IMPORTANT: Keep this file secure and never commit to version control!
# ============================================================================

# ============================================================================
# SECURITY CONFIGURATION
# ============================================================================

# MongoDB Credentials
MONGO_USERNAME=admin
MONGO_PASSWORD=${MONGO_PASSWORD}

# PostgreSQL Credentials
POSTGRES_USER=airflow
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_DB=messages
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# VNC Access
VNC_PASSWORD=${VNC_PASSWORD}

# Streamlit App Authentication
APP_USERNAME=${APP_USERNAME}
SESSION_TIMEOUT=480

# ============================================================================
# AIRFLOW CONFIGURATION
# ============================================================================

AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY}
AIRFLOW_WEBSERVER_SECRET_KEY=${AIRFLOW_WEBSERVER_SECRET_KEY}
AIRFLOW_ADMIN_USERNAME=${AIRFLOW_ADMIN_USERNAME}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
AIRFLOW_ADMIN_EMAIL=${ADMIN_EMAIL}

# Airflow User/Group IDs (0 = root, required for Chrome in container)
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Airflow Core Settings
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__PARALLELISM=8
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__DEFAULT_TIMEZONE=Africa/Nairobi
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth

# ============================================================================
# DATABASE URLS
# ============================================================================

DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/messages
MONGODB_URI=mongodb://admin:${MONGO_PASSWORD}@mongodb:27017/messages_db?authSource=admin

# ============================================================================
# EMAIL CONFIGURATION (Notifications & Alerts)
# ============================================================================

EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=true
EMAIL_USERNAME=${EMAIL_USERNAME}
EMAIL_PASSWORD=${EMAIL_PASSWORD}
EMAIL_FROM=${EMAIL_FROM}
EMAIL_TO=${EMAIL_TO}

# ============================================================================
# BACKUP CONFIGURATION
# ============================================================================

BACKUP_MAX_SIZE_MB=25
BACKUP_RETENTION_DAYS=7
BACKUP_DIR=/opt/airflow/backups
WORKFLOWS_EXPORT_DIR=/opt/airflow/workflows_export
PROMPT_BACKUPS_DIR=/opt/airflow/prompt_backups

# ============================================================================
# TWITTER API CONFIGURATION
# ============================================================================

TWITTER_CONSUMER_KEY=${TWITTER_CONSUMER_KEY}
TWITTER_CONSUMER_SECRET=${TWITTER_CONSUMER_SECRET}
TWITTER_ACCESS_TOKEN_KEY=${TWITTER_ACCESS_TOKEN_KEY}
TWITTER_ACCESS_TOKEN_SECRET=${TWITTER_ACCESS_TOKEN_SECRET}
TWITTER_API_KEY=${TWITTER_CONSUMER_KEY}
TWITTER_API_SECRET=${TWITTER_CONSUMER_SECRET}
TWITTER_ACCESS_TOKEN=${TWITTER_ACCESS_TOKEN_KEY}
TWITTER_BEARER_TOKEN=

# ============================================================================
# AI API KEYS
# ============================================================================

GEMINI_API_KEY=${GEMINI_API_KEY}
OPENAI_API_KEY=${OPENAI_API_KEY}
HYPERBROWSER_API_KEY=${HYPERBROWSER_API_KEY}
HYPERBROWSER_EXTENSION_ID=cace7b97-d0e4-4a6f-9bc5-a868c4546975
HYPERBROWSER_PROFILE_ID=d4ea19ca-ba98-4506-8f71-355cb07f0db0
HYPERBROWSER_MAX_STEPS=25

# ============================================================================
# CHROME & BROWSER CONFIGURATION
# ============================================================================

CHROME_PROFILE_DIR=/workspace/chrome_profiles
CHROME_PROFILES_BASE_DIR=/workspace/chrome_profiles
CHROME_USER_DATA_DIR=/workspace/chrome_profiles
CHROME_DOWNLOAD_DIR=/workspace/downloads
CHROME_EXECUTABLE=/usr/bin/google-chrome-stable
CHROME_DEBUG_PORT_START=9222
AUTOMA_EXTENSION_ID=infppggnoaenmfagbfknfkancpbljcca

# ============================================================================
# WORKFLOW & SCHEDULING
# ============================================================================

DAG_TIMEZONE=Africa/Nairobi
DAG_SCHEDULE=0 16 * * 0
DAG_START_DATE=2025-07-01
CHECK_INTERVAL_MINUTES=10
MAX_WORKFLOWS_PER_DAY=6
MAX_WORKFLOWS_PER_HOUR=2
WORKFLOW_GENERATION_ENABLED=true

# ============================================================================
# SCRAPING CONFIGURATION
# ============================================================================

URL_TO_SCRAPE=https://x.com/queenHaley___/with_replies
EXCLUDE_TERMS=queenhaley___
FILTER_LINKS=true
REQUIRED_DIGITS=19
NUM_MESSAGES=5
OPEN_LINKS_BATCH_SIZE=10

# Scheduling
SCRAPE_SCHEDULE=0 */2 * * *
OPEN_LINKS_SCHEDULE=*/15 * * * *

# Session Management
SESSION_REUSE_STRATEGY=single_shared
EXISTING_SESSION_ID=6c8c089f-07ec-4b1b-8de0-95dd3c6490b6
TWITTER_MESSAGE=https://messenger.com

# ============================================================================
# WHATSAPP CONFIGURATION (Optional)
# ============================================================================

WHATSAPP_API_TOKEN=
WHATSAPP_API_URL=
WHATSAPP_PHONE_NUMBER=

# ============================================================================
# DIRECTORIES & PATHS
# ============================================================================

ROOT_DOWNLOADS_DIR=/root/Downloads
RECORDINGS_DIR=/workspace/recordings
DOWNLOADS_DIR=/workspace/downloads
WORKSPACE_DIR=/workspace

# Display Settings
DISPLAY=:99
VNC_PORT=5900
NOVNC_PORT=6080

# ============================================================================
# STREAMLIT SERVER CONFIGURATION
# ============================================================================

STREAMLIT_SERVER_ENABLE_CORS=false
STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true
STREAMLIT_SERVER_HEADLESS=true

# ============================================================================
# PYTHON & ENVIRONMENT
# ============================================================================

PYTHONPATH=/app:/opt/airflow/src
PYTHONWARNINGS=ignore::UserWarning
PYTHONUNBUFFERED=1
TZ=Africa/Nairobi

# ============================================================================
# DEBUG & LOGGING
# ============================================================================

DEBUG=false
DEBUG_MODE=false
LOG_LEVEL=WARNING

# ============================================================================
# JAVASCRIPT EXECUTOR
# ============================================================================

JS_EXECUTOR_PATH=/opt/airflow/src/scripts/local_execute
EOF

# Set secure permissions
chmod 600 .env.production

echo "✓ .env.production file created successfully!"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Generated Credentials"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Streamlit:"
echo "  Username: ${APP_USERNAME}"
echo ""
echo "Airflow:"
echo "  Username: ${AIRFLOW_ADMIN_USERNAME}"
echo "  Password: ${AIRFLOW_ADMIN_PASSWORD}"
echo "  URL: https://YOUR_SERVER_IP:8443"
echo ""
echo "VNC:"
echo "  Password: ${VNC_PASSWORD}"
echo "  URL: https://YOUR_SERVER_IP/vnc/"
echo ""
echo "MongoDB:"
echo "  Username: admin"
echo "  Password: ${MONGO_PASSWORD}"
echo ""
echo "PostgreSQL:"
echo "  Username: airflow"
echo "  Password: ${POSTGRES_PASSWORD}"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Save credentials to backup file
credentials_file=".credentials.$(date +%Y%m%d_%H%M%S).txt"
cat > "$credentials_file" << CREDS
Production Credentials
Generated: $(date)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Streamlit:
  Username: ${APP_USERNAME}
  URL: https://185.213.25.170/

Airflow:
  Username: ${AIRFLOW_ADMIN_USERNAME}
  Password: ${AIRFLOW_ADMIN_PASSWORD}
  URL: https://185.213.25.170:8443

VNC:
  Password: ${VNC_PASSWORD}
  URL: https://185.213.25.170/vnc/

MongoDB:
  Username: admin
  Password: ${MONGO_PASSWORD}

PostgreSQL:
  Username: airflow
  Password: ${POSTGRES_PASSWORD}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
IMPORTANT: Store these credentials securely and delete this file!
CREDS

chmod 600 "$credentials_file"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    SETUP COMPLETE!                         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "IMPORTANT NOTES:"
echo "  1. ✓ .env.production created with secure permissions (600)"
echo "  2. ✓ Credentials saved to: $credentials_file"
echo "  3. ⚠️  SAVE YOUR PASSWORDS NOW - You won't see them again!"
echo "  4. 🗑️  Delete $credentials_file after saving credentials"
echo "  5. 🚫 Never commit .env.production to version control"
echo ""
echo "NEXT STEPS:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "1. Build and start services:"
echo "   docker compose -f docker-compose.prod.yml build"
echo "   docker compose -f docker-compose.prod.yml up -d"
echo ""
echo "2. Check service status:"
echo "   docker compose -f docker-compose.prod.yml ps"
echo ""
echo "3. View logs:"
echo "   docker compose -f docker-compose.prod.yml logs -f"
echo ""
echo "4. Access your applications:"
echo "   • Streamlit: https://185.213.25.170/"
echo "   • Airflow:   https://185.213.25.170:8443/"
echo "   • VNC:       https://185.213.25.170/vnc/"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
