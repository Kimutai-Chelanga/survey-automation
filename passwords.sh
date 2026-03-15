#!/bin/bash

# ===================================================================
# Generate Production Environment Variables
# ===================================================================
# This script generates secure passwords and creates .env.production
# Usage: ./generate-production-env.sh
# ===================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_TEMPLATE="$PROJECT_ROOT/.env.production"
ENV_OUTPUT="$PROJECT_ROOT/.env.production"
CREDENTIALS_FILE="$PROJECT_ROOT/.production-credentials.txt"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Production Environment Generator for Contabo VPS          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if .env.production template exists
if [ ! -f "$ENV_TEMPLATE" ]; then
    echo -e "${RED}❌ Error: .env.production template not found!${NC}"
    echo -e "${YELLOW}Expected location: $ENV_TEMPLATE${NC}"
    exit 1
fi

# Check required commands
echo -e "${BLUE}🔍 Checking required tools...${NC}"
for cmd in openssl python3; do
    if ! command -v $cmd &> /dev/null; then
        echo -e "${RED}❌ Error: $cmd is not installed${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓${NC} $cmd found"
done

# Check Python packages
echo ""
echo -e "${BLUE}🔍 Checking Python packages...${NC}"
if ! python3 -c "import bcrypt" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  bcrypt not found. Installing...${NC}"
    pip3 install bcrypt --quiet
fi
echo -e "${GREEN}✓${NC} bcrypt available"

if ! python3 -c "from cryptography.fernet import Fernet" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  cryptography not found. Installing...${NC}"
    pip3 install cryptography --quiet
fi
echo -e "${GREEN}✓${NC} cryptography available"

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}               Password & Key Generation${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Generate secure random passwords
echo -e "${BLUE}🔐 Generating secure passwords...${NC}"
POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d '\n')
MONGO_PASSWORD=$(openssl rand -base64 32 | tr -d '\n')
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 32 | tr -d '\n')
VNC_PASSWORD=$(openssl rand -base64 32 | tr -d '\n')
echo -e "${GREEN}✓${NC} Generated 4 secure passwords (32 characters each)"

# Generate Airflow keys
echo -e "${BLUE}🔑 Generating Airflow Fernet key...${NC}"
AIRFLOW_FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo -e "${GREEN}✓${NC} Generated Airflow Fernet key"

echo -e "${BLUE}🔑 Generating Airflow webserver secret...${NC}"
AIRFLOW_WEBSERVER_SECRET=$(openssl rand -base64 32 | tr -d '\n')
echo -e "${GREEN}✓${NC} Generated Airflow webserver secret key"

# Prompt for Streamlit password
echo ""
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}This password is for logging into the Streamlit web interface${NC}"
echo -e "${YELLOW}You'll use: username='admin' and this password to login${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
echo ""
while true; do
    read -s -p "$(echo -e ${BLUE}Enter password for Streamlit login${NC}): " STREAMLIT_PASSWORD
    echo
    read -s -p "$(echo -e ${BLUE}Confirm password${NC}): " STREAMLIT_PASSWORD_CONFIRM
    echo

    if [ "$STREAMLIT_PASSWORD" == "$STREAMLIT_PASSWORD_CONFIRM" ]; then
        if [ ${#STREAMLIT_PASSWORD} -lt 8 ]; then
            echo -e "${RED}❌ Password must be at least 8 characters${NC}"
            echo ""
        else
            break
        fi
    else
        echo -e "${RED}❌ Passwords don't match. Try again.${NC}"
        echo ""
    fi
done

echo -e "${BLUE}🔐 Generating bcrypt hash for Streamlit password...${NC}"
APP_PASSWORD_HASH=$(python3 -c "import bcrypt; print(bcrypt.hashpw(b'$STREAMLIT_PASSWORD', bcrypt.gensalt()).decode())")
echo -e "${GREEN}✓${NC} Generated Streamlit password hash"

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}               Creating .env.production File${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Backup existing .env.production if it exists
if [ -f "$ENV_OUTPUT" ]; then
    BACKUP_FILE="${ENV_OUTPUT}.backup.$(date +%Y%m%d_%H%M%S)"
    echo -e "${YELLOW}⚠️  Existing .env.production found. Creating backup...${NC}"
    cp "$ENV_OUTPUT" "$BACKUP_FILE"
    echo -e "${GREEN}✓${NC} Backup created: $BACKUP_FILE"
    echo ""
fi

# Create new .env.production by replacing placeholders
echo -e "${BLUE}📝 Updating .env.production with generated values...${NC}"

# Create a temporary working copy
TEMP_FILE="${ENV_OUTPUT}.tmp.$"
cp "$ENV_TEMPLATE" "$TEMP_FILE"

# Replace PostgreSQL password
sed -i.bak "s|CHANGE_THIS_NOW_postgres_prod_2025_secure_password|$POSTGRES_PASSWORD|g" "$TEMP_FILE"
# Update DATABASE_URL with actual password
sed -i.bak "s|postgresql://\${POSTGRES_USER}:\${POSTGRES_PASSWORD}@|postgresql://automation_user:$POSTGRES_PASSWORD@|g" "$TEMP_FILE"

# Replace MongoDB password
sed -i.bak "s|CHANGE_THIS_NOW_mongo_prod_2025_secure_password|$MONGO_PASSWORD|g" "$TEMP_FILE"
# Update MONGODB_URI with actual password
sed -i.bak "s|mongodb://\${MONGO_ROOT_USER}:\${MONGO_ROOT_PASSWORD}@|mongodb://admin:$MONGO_PASSWORD@|g" "$TEMP_FILE"

# Replace Streamlit password hash
sed -i.bak "s|CHANGE_THIS_bcrypt_hash_generated_from_your_password|$APP_PASSWORD_HASH|g" "$TEMP_FILE"

# Replace Airflow keys and passwords
sed -i.bak "s|CHANGE_THIS_32_char_fernet_key_here_exactly_32_chars|$AIRFLOW_FERNET_KEY|g" "$TEMP_FILE"
sed -i.bak "s|CHANGE_THIS_webserver_secret_key_here|$AIRFLOW_WEBSERVER_SECRET|g" "$TEMP_FILE"
sed -i.bak "s|CHANGE_THIS_airflow_admin_password_here|$AIRFLOW_ADMIN_PASSWORD|g" "$TEMP_FILE"

# Update AIRFLOW__CORE__SQL_ALCHEMY_CONN with actual password
sed -i.bak "s|postgresql+psycopg2://\${POSTGRES_USER}:\${POSTGRES_PASSWORD}@|postgresql+psycopg2://automation_user:$POSTGRES_PASSWORD@|g" "$TEMP_FILE"

# Replace VNC password
sed -i.bak "s|CHANGE_THIS_vnc_password_here|$VNC_PASSWORD|g" "$TEMP_FILE"

# Move temp file to final location
mv "$TEMP_FILE" "$ENV_OUTPUT"

# Remove temporary sed backup files
rm -f "${TEMP_FILE}.bak"

echo -e "${GREEN}✓${NC} Updated POSTGRES_PASSWORD"
echo -e "${GREEN}✓${NC} Updated MONGO_ROOT_PASSWORD"
echo -e "${GREEN}✓${NC} Updated APP_PASSWORD_HASH"
echo -e "${GREEN}✓${NC} Updated AIRFLOW_FERNET_KEY"
echo -e "${GREEN}✓${NC} Updated AIRFLOW_WEBSERVER_SECRET_KEY"
echo -e "${GREEN}✓${NC} Updated AIRFLOW_ADMIN_PASSWORD"
echo -e "${GREEN}✓${NC} Updated VNC_PASSWORD"
echo -e "${GREEN}✓${NC} Updated all connection strings"

# Set secure permissions
chmod 600 "$ENV_OUTPUT"
echo ""
echo -e "${GREEN}✓${NC} Set secure permissions (600) on .env.production"

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}            Saving Credentials to Secure File${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Save credentials to a secure file
cat > "$CREDENTIALS_FILE" << EOF
═══════════════════════════════════════════════════════════════
PRODUCTION CREDENTIALS - KEEP THIS FILE SECURE!
Generated: $(date)
═══════════════════════════════════════════════════════════════

🌐 STREAMLIT WEB INTERFACE
   URL: https://YOUR_SERVER_IP/
   Username: admin
   Password: $STREAMLIT_PASSWORD

✈️  AIRFLOW WEB INTERFACE
   URL: https://YOUR_SERVER_IP/airflow/
   Username: admin
   Password: $AIRFLOW_ADMIN_PASSWORD

🖥️  VNC WEB ACCESS
   URL: https://YOUR_SERVER_IP/vnc/
   Password: $VNC_PASSWORD

🗄️  POSTGRESQL DATABASE
   Host: postgres (internal only)
   Port: 5432
   Database: messages
   Username: automation_user
   Password: $POSTGRES_PASSWORD
   Connection: postgresql://automation_user:$POSTGRES_PASSWORD@postgres:5432/messages

🍃 MONGODB DATABASE
   Host: mongodb (internal only)
   Port: 27017
   Database: messages_db
   Username: admin
   Password: $MONGO_PASSWORD
   Connection: mongodb://admin:$MONGO_PASSWORD@mongodb:27017/messages_db?authSource=admin

🔑 AIRFLOW KEYS
   Fernet Key: $AIRFLOW_FERNET_KEY
   Webserver Secret: $AIRFLOW_WEBSERVER_SECRET

═══════════════════════════════════════════════════════════════
IMPORTANT NOTES:
═══════════════════════════════════════════════════════════════
1. Store this file in a password manager (1Password, Bitwarden, etc.)
2. NEVER commit this file to Git
3. Delete this file after saving to password manager
4. Database ports are NOT exposed to internet (internal Docker only)
5. Replace YOUR_SERVER_IP with your actual Contabo server IP

To delete this file: rm $CREDENTIALS_FILE
═══════════════════════════════════════════════════════════════
EOF

chmod 600 "$CREDENTIALS_FILE"

echo -e "${GREEN}✓${NC} Credentials saved to: ${YELLOW}$CREDENTIALS_FILE${NC}"
echo -e "${YELLOW}⚠️  IMPORTANT: Copy these credentials to your password manager!${NC}"

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                   Verification${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Verify no placeholders remain
PLACEHOLDERS=$(grep -c "CHANGE_THIS" "$ENV_OUTPUT" || true)
if [ "$PLACEHOLDERS" -gt 0 ]; then
    echo -e "${RED}⚠️  Warning: Found $PLACEHOLDERS remaining placeholders in .env.production${NC}"
    echo -e "${YELLOW}Run this to check: grep CHANGE_THIS $ENV_OUTPUT${NC}"
else
    echo -e "${GREEN}✓${NC} No placeholders remaining - all values updated!"
fi

# Check file permissions
PERMS=$(stat -c "%a" "$ENV_OUTPUT" 2>/dev/null || stat -f "%OLp" "$ENV_OUTPUT" 2>/dev/null)
if [ "$PERMS" == "600" ]; then
    echo -e "${GREEN}✓${NC} File permissions are secure (600)"
else
    echo -e "${YELLOW}⚠️  File permissions: $PERMS (should be 600)${NC}"
fi

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                   ✅ SUCCESS!                                  ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}✓${NC} .env.production file created with secure passwords"
echo -e "${GREEN}✓${NC} All 7 passwords and keys generated and updated"
echo -e "${GREEN}✓${NC} Credentials saved to: ${YELLOW}$CREDENTIALS_FILE${NC}"
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                    Next Steps${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "1. ${BLUE}Review credentials:${NC}"
echo -e "   ${YELLOW}cat $CREDENTIALS_FILE${NC}"
echo ""
echo -e "2. ${BLUE}Save credentials to password manager${NC}"
echo -e "   Copy all credentials from the file above"
echo ""
echo -e "3. ${BLUE}Verify .env.production:${NC}"
echo -e "   ${YELLOW}grep -E 'PASSWORD|KEY|HASH' $ENV_OUTPUT${NC}"
echo ""
echo -e "4. ${BLUE}Test locally (RECOMMENDED):${NC}"
echo -e "   ${YELLOW}./scripts/test-deployment.sh${NC}"
echo ""
echo -e "5. ${BLUE}Update nginx.conf with your IP:${NC}"
echo -e "   Current IP: ${GREEN}$(curl -s ifconfig.me)${NC}"
echo -e "   Edit: ${YELLOW}nano nginx/nginx.conf${NC}"
echo ""
echo -e "6. ${BLUE}Verify .gitignore:${NC}"
echo -e "   ${YELLOW}grep -E '\\.env\\.production|\\.production-credentials' .gitignore${NC}"
echo ""
echo -e "7. ${BLUE}Commit changes (NOT .env.production):${NC}"
echo -e "   ${YELLOW}git add nginx/nginx.conf docker-compose.prod.yml${NC}"
echo -e "   ${YELLOW}git commit -m 'Production configuration ready'${NC}"
echo -e "   ${YELLOW}git push origin main${NC}"
echo ""
echo -e "8. ${BLUE}After saving credentials, delete the file:${NC}"
echo -e "   ${YELLOW}rm $CREDENTIALS_FILE${NC}"
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}⚠️  CRITICAL REMINDERS:${NC}"
echo -e "   • NEVER commit .env.production or .production-credentials.txt to Git"
echo -e "   • Store all passwords in a password manager"
echo -e "   • Test locally before deploying to production"
echo -e "   • Update nginx.conf with your actual IP address"
echo ""
echo -e "${GREEN}Ready for production deployment! 🚀${NC}"
echo ""
