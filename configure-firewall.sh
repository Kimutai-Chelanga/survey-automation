#!/bin/bash
# UFW Firewall Configuration Script
# Configures Ubuntu firewall for secure production deployment

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    print_error "Please run as root (use sudo)"
    exit 1
fi

# Get whitelisted IP
read -p "Enter your laptop's public IP address: " LAPTOP_IP

if [ -z "$LAPTOP_IP" ]; then
    print_error "IP address cannot be empty"
    exit 1
fi

print_info "Configuring firewall for IP: $LAPTOP_IP"

# Install UFW if not present
if ! command -v ufw &> /dev/null; then
    print_info "Installing UFW..."
    apt-get update
    apt-get install -y ufw
fi

# Ask before resetting
read -p "Reset UFW to default configuration? (y/n): " reset_confirm
if [ "$reset_confirm" = "y" ]; then
    print_warning "Resetting UFW to default configuration..."
    ufw --force reset
fi

# Set default policies
print_info "Setting default policies..."
ufw default deny incoming
ufw default allow outgoing

# Allow SSH (CRITICAL!)
print_info "Allowing SSH access..."
ufw allow 22/tcp comment 'SSH'

# Allow services only from whitelisted IP
print_info "Allowing access from $LAPTOP_IP only..."

# Streamlit (HTTPS)
ufw allow from $LAPTOP_IP to any port 443 proto tcp comment 'Streamlit HTTPS'

# Airflow (HTTPS)
ufw allow from $LAPTOP_IP to any port 8443 proto tcp comment 'Airflow HTTPS'

# HTTP (for redirect to HTTPS)
ufw allow from $LAPTOP_IP to any port 80 proto tcp comment 'HTTP redirect'

# VNC (if needed directly, though accessible via HTTPS)
# ufw allow from $LAPTOP_IP to any port 6080 proto tcp comment 'VNC noVNC'

# Enable UFW
print_info "Enabling UFW..."
ufw --force enable

# Show status
echo ""
print_info "Firewall Status:"
ufw status verbose

echo ""
print_info "Firewall configuration complete!"
print_warning "IMPORTANT: Make sure you can still SSH before closing this session!"
echo ""
echo "Test your SSH connection in a new terminal before logging out."
echo "If locked out, you'll need console access to disable UFW with: sudo ufw disable"
