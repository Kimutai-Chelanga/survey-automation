#!/bin/bash

# Production Deployment Script for Contabo Server
# This script sets up your Docker environment with security configurations

set -e

echo "=============================================="
echo "Production Deployment Setup"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 1. Get user's laptop IP address
echo ""
print_info "Step 1: Configure IP Whitelist"
echo "=============================================="
echo "Your application will ONLY be accessible from whitelisted IPs."
echo ""
read -p "Enter your laptop's public IP address (or press Enter to detect): " LAPTOP_IP

if [ -z "$LAPTOP_IP" ]; then
    print_info "Attempting to detect your public IP..."
    LAPTOP_IP=$(curl -s https://api.ipify.org || curl -s https://ifconfig.me || echo "")
    if [ -z "$LAPTOP_IP" ]; then
        print_error "Could not detect IP automatically. Please enter manually:"
        read -p "Your public IP: " LAPTOP_IP
    else
        print_info "Detected IP: $LAPTOP_IP"
        read -p "Is this correct? (y/n): " confirm
        if [ "$confirm" != "y" ]; then
            read -p "Enter correct IP: " LAPTOP_IP
        fi
    fi
fi

# 2. Create nginx directory structure
print_info "Step 2: Creating nginx directory structure..."
mkdir -p nginx/ssl
mkdir -p nginx/logs
chmod 755 nginx
chmod 755 nginx/ssl
chmod 755 nginx/logs

# 3. Update nginx configuration with user's IP
print_info "Step 3: Updating nginx configuration..."
if [ ! -f "nginx/nginx.conf" ]; then
    cp nginx.conf nginx/nginx.conf
fi

sed -i "s/YOUR_LAPTOP_IP_HERE/$LAPTOP_IP/g" nginx/nginx.conf
print_info "IP whitelist configured: $LAPTOP_IP"

# 4. Generate self-signed SSL certificate
print_info "Step 4: Generating SSL certificates..."
if [ ! -f "nginx/ssl/cert.pem" ]; then
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout nginx/ssl/key.pem \
        -out nginx/ssl/cert.pem \
        -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost" \
        2>/dev/null
    chmod 644 nginx/ssl/cert.pem
    chmod 600 nginx/ssl/key.pem
    print_info "SSL certificates generated (self-signed)"
    print_warning "For production, replace with real SSL certificates from Let's Encrypt"
else
    print_info "SSL certificates already exist"
fi

# 5. Generate strong passwords
print_info "Step 5: Generating secure passwords..."

generate_password() {
    openssl rand -base64 32 | tr -d "=+/" | cut -c1-25
}

MONGO_PASS=$(generate_password)
VNC_PASS=$(generate_password)
AIRFLOW_SECRET=$(generate_password)
AIRFLOW_PASS=$(generate_password)
DB_PASS=$(generate_password)

# 6. Update environment file
print_info "Step 6: Creating production environment file..."
if [ ! -f ".env.production" ]; then
    cp .env.production.template .env.production 2>/dev/null || touch .env.production
fi

# Update passwords in .env.production
sed -i "s/CHANGE_THIS_STRONG_PASSWORD_123/$MONGO_PASS/g" .env.production
sed -i "s/CHANGE_THIS_VNC_PASSWORD_456/$VNC_PASS/g" .env.production
sed -i "s/CHANGE_THIS_TO_RANDOM_STRING_789/$AIRFLOW_SECRET/g" .env.production
sed -i "s/CHANGE_THIS_AIRFLOW_PASSWORD_012/$AIRFLOW_PASS/g" .env.production
sed -i "s/CHANGE_DB_PASSWORD/$DB_PASS/g" .env.production

# Save credentials securely
print_info "Saving credentials to secure file..."
cat > .credentials.txt << EOF
============================================
PRODUCTION CREDENTIALS
Generated: $(date)
============================================

MongoDB:
  Username: admin
  Password: $MONGO_PASS

VNC Access:
  Password: $VNC_PASS

Airflow Web UI:
  Username: admin
  Password: $AIRFLOW_PASS

PostgreSQL:
  Username: airflow
  Password: $DB_PASS

Whitelisted IP: $LAPTOP_IP

============================================
IMPORTANT: Store these credentials securely!
Delete this file after saving to password manager.
============================================
EOF

chmod 600 .credentials.txt
print_info "Credentials saved to .credentials.txt"

# 7. Configure firewall (UFW)
print_info "Step 7: Configuring firewall..."
if command -v ufw &> /dev/null; then
    print_warning "Configuring UFW firewall..."

    # Enable UFW
    sudo ufw --force enable

    # Default policies
    sudo ufw default deny incoming
    sudo ufw default allow outgoing

    # Allow SSH (CRITICAL - don't lock yourself out!)
    sudo ufw allow 22/tcp comment 'SSH'

    # Allow HTTP/HTTPS only from whitelisted IP
    sudo ufw allow from $LAPTOP_IP to any port 80 proto tcp comment 'HTTP from laptop'
    sudo ufw allow from $LAPTOP_IP to any port 443 proto tcp comment 'HTTPS from laptop'
    sudo ufw allow from $LAPTOP_IP to any port 8443 proto tcp comment 'Airflow HTTPS from laptop'

    # Show status
    sudo ufw status numbered

    print_info "Firewall configured successfully"
else
    print_warning "UFW not found. Please configure your firewall manually:"
    echo "  - Allow SSH (port 22)"
    echo "  - Allow HTTP (port 80) only from $LAPTOP_IP"
    echo "  - Allow HTTPS (port 443) only from $LAPTOP_IP"
    echo "  - Allow port 8443 only from $LAPTOP_IP"
fi

# 8. Set proper permissions
print_info "Step 8: Setting file permissions..."
chmod 600 .env.production
chmod 755 deploy-production.sh
find . -name "*.sh" -exec chmod +x {} \;

# 9. Create necessary directories
print_info "Step 9: Creating application directories..."
mkdir -p airflow/dags airflow/logs airflow/scripts
mkdir -p host_downloads
# Ignore errors for __pycache__ directories
chmod -R 755 airflow 2>/dev/null || true

# 10. Pull and build images
print_info "Step 10: Building Docker images..."
read -p "Build Docker images now? (y/n): " build_confirm
if [ "$build_confirm" = "y" ]; then
    docker compose -f docker-compose.prod.yml build
    print_info "Docker images built successfully"
fi

# 11. Summary
echo ""
echo "=============================================="
print_info "Deployment Configuration Complete!"
echo "=============================================="
echo ""
echo "Next Steps:"
echo "1. Review credentials in .credentials.txt"
echo "2. Start services: docker compose -f docker-compose.prod.yml up -d"
echo "3. Access services:"
echo "   - Streamlit: https://YOUR_SERVER_IP"
echo "   - Airflow: https://YOUR_SERVER_IP:8443"
echo "   - VNC: https://YOUR_SERVER_IP/vnc/"
echo ""
echo "Security Notes:"
echo "✓ Only accessible from IP: $LAPTOP_IP"
echo "✓ SSL certificates generated (replace with real ones)"
echo "✓ Strong passwords generated"
echo "✓ Firewall configured (if UFW available)"
echo "✓ Database ports not exposed externally"
echo ""
print_warning "IMPORTANT: Delete .credentials.txt after saving passwords!"
echo ""

# Ask to start services
read -p "Start Docker services now? (y/n): " start_confirm
if [ "$start_confirm" = "y" ]; then
    print_info "Starting services..."
    docker compose -f docker-compose.prod.yml up -d

    echo ""
    print_info "Waiting for services to be healthy..."
    sleep 10

    docker compose -f docker-compose.prod.yml ps

    echo ""
    print_info "Services started successfully!"
    echo "Access your application at: https://$(curl -s https://api.ipify.org)"
fi

echo ""
print_info "Setup complete!"
