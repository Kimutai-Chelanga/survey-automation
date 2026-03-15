#!/bin/bash
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Production Environment Generator                           ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check required tools
for cmd in openssl python3; do
    if ! command -v $cmd &> /dev/null; then
        echo -e "${RED}❌ Error: $cmd is not installed${NC}"
        exit 1
    fi
done

# Install Python packages if needed
python3 -c "import bcrypt" 2>/dev/null || pip3 install bcrypt --quiet
python3 -c "from cryptography.fernet import Fernet" 2>/dev/null || pip3 install cryptography --quiet

echo -e "${BLUE}🔐 Generating secure credentials...${NC}"
echo ""

# Generate passwords
POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-32)
MONGO_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-32)
AIRFLOW_FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AIRFLOW_SECRET_KEY=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-32)
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 24 | tr -d "=+/" | cut -c1-24)
VNC_PASSWORD=$(openssl rand -base64 16 | tr -d "=+/" | cut -c1-16)

# Get user inputs
echo -e "${BLUE}📝 Required Information:${NC}"
echo ""
read -p "GitHub Repository (e.g., pnata0890-dev/final-automation): " GITHUB_REPO
GITHUB_REPO=${GITHUB_REPO:-pnata0890-dev/final-automation}

read -p "Contabo Server IP (leave empty if not deployed yet): " SERVER_IP
SERVER_IP=${SERVER_IP:-YOUR_SERVER_IP}

read -p "App Username [admin]: " APP_USER
APP_USER=${APP_USER:-admin}

echo ""
echo -e "${YELLOW}Enter a strong password for Streamlit web login:${NC}"
while true; do
    read -s -p "Password (min 8 chars): " APP_PASSWORD
    echo
    read -s -p "Confirm password: " APP_PASSWORD_CONFIRM
    echo

    if [ "$APP_PASSWORD" == "$APP_PASSWORD_CONFIRM" ]; then
        if [ ${#APP_PASSWORD} -ge 8 ]; then
            break
        else
            echo -e "${RED}Password must be at least 8 characters${NC}"
        fi
    else
        echo -e "${RED}Passwords don't match${NC}"
    fi
done

APP_PASSWORD_HASH=$(python3 -c "import bcrypt; print(bcrypt.hashpw(b'$APP_PASSWORD', bcrypt.gensalt()).decode())")

echo ""
echo -e "${BLUE}📝 API Keys (press Enter to skip if not available):${NC}"
read -p "Gemini API Key: " GEMINI_KEY
read -p "OpenAI API Key: " OPENAI_KEY
read -p "Hyperbrowser API Key: " HYPERBROWSER_KEY

# Create .env.production
cat > .env.production << EOF
# ============================================================================
# PRODUCTION ENVIRONMENT - Auto-generated $(date +%Y-%m-%d)
# ============================================================================

# GitHub
GITHUB_REPOSITORY=$GITHUB_REPO

# Server
SERVER_HOST=$SERVER_IP
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO

# Streamlit App
APP_USERNAME=$APP_USER
APP_PASSWORD_HASH=$APP_PASSWORD_HASH
SESSION_TIMEOUT=480

# PostgreSQL
POSTGRES_USER=automation_prod
POSTGRES_PASSWORD=$POSTGRES_PASSWORD
POSTGRES_DB=messages
DATABASE_URL=postgresql://automation_prod:$POSTGRES_PASSWORD@postgres:5432/messages

# MongoDB
MONGO_ROOT_USER=admin
MONGO_ROOT_PASSWORD=$MONGO_PASSWORD
MONGO_DB=messages_db
MONGODB_URI=mongodb://admin:$MONGO_PASSWORD@mongodb:27017/messages_db?authSource=admin

# Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_FERNET_KEY=$AIRFLOW_FERNET_KEY
AIRFLOW_WEBSERVER_SECRET_KEY=$AIRFLOW_SECRET_KEY
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$AIRFLOW_ADMIN_PASSWORD

# VNC
VNC_PASSWORD=$VNC_PASSWORD

# API Keys
GEMINI_API_KEY=${GEMINI_KEY:-}
OPENAI_API_KEY=${OPENAI_KEY:-}
HYPERBROWSER_API_KEY=${HYPERBROWSER_KEY:-}

# Chrome & Browser
CHROME_PROFILE_DIR=/workspace/chrome_profiles
CHROME_PROFILES_BASE_DIR=/workspace/chrome_profiles
CHROME_USER_DATA_DIR=/workspace/chrome_profiles
CHROME_DOWNLOAD_DIR=/workspace/downloads
CHROME_EXECUTABLE=/usr/bin/google-chrome-stable
CHROME_DEBUG_PORT_START=9222

# Workflow & Scheduling
DAG_TIMEZONE=Africa/Nairobi
DAG_SCHEDULE=0 16 * * 0
DAG_START_DATE=2025-07-01
CHECK_INTERVAL_MINUTES=10
MAX_WORKFLOWS_PER_DAY=6
MAX_WORKFLOWS_PER_HOUR=2

# Backup
BACKUP_SCHEDULE=0 2 * * *
BACKUP_RETENTION_DAYS=7

# Directories
RECORDINGS_DIR=/workspace/recordings
DOWNLOADS_DIR=/workspace/downloads
WORKSPACE_DIR=/workspace

# Email (Update with your details)
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=true
EMAIL_USERNAME=pnata0890@gmail.com
EMAIL_PASSWORD=jfusqkjruytwktit
EMAIL_FROM=pnata0890@gmail.com
EMAIL_TO=pnata0890@gmail.com

# Scraping
URL_TO_SCRAPE=https://x.com/queenHaley___/with_replies
EXCLUDE_TERMS=queenhaley___
FILTER_LINKS=true
REQUIRED_DIGITS=19
NUM_MESSAGES=5
OPEN_LINKS_BATCH_SIZE=10
EOF

chmod 600 .env.production

# Save credentials
cat > .production-credentials.txt << EOF
═══════════════════════════════════════════════════════════════
PRODUCTION CREDENTIALS - $(date)
═══════════════════════════════════════════════════════════════

Server IP: $SERVER_IP

Streamlit:
  URL: https://$SERVER_IP/
  Username: $APP_USER
  Password: $APP_PASSWORD

Airflow:
  URL: https://$SERVER_IP/airflow/
  Username: admin
  Password: $AIRFLOW_ADMIN_PASSWORD

VNC:
  URL: https://$SERVER_IP/vnc/
  Password: $VNC_PASSWORD

PostgreSQL:
  Host: postgres (internal)
  User: automation_prod
  Password: $POSTGRES_PASSWORD

MongoDB:
  Host: mongodb (internal)
  User: admin
  Password: $MONGO_PASSWORD

═══════════════════════════════════════════════════════════════
SAVE THESE IN A PASSWORD MANAGER THEN DELETE THIS FILE!
═══════════════════════════════════════════════════════════════
EOF

chmod 600 .production-credentials.txt

echo ""
echo -e "${GREEN}✅ Files created:${NC}"
echo "   • .env.production"
echo "   • .production-credentials.txt"
echo ""
echo -e "${YELLOW}📋 View credentials:${NC} cat .production-credentials.txt"
echo -e "${YELLOW}🔒 Save to password manager, then delete:${NC} rm .production-credentials.txt"
echo ""
