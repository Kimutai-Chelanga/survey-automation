#!/bin/bash

# setup_nginx_ssl.sh
# Complete setup script for SSL and Nginx configuration
# Usage: ./setup_nginx_ssl.sh

set -e

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Nginx SSL & IP Whitelist Configuration Setup          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check if running from correct directory
if [ ! -f "docker-compose.prod.yml" ]; then
    echo "ERROR: docker-compose.prod.yml not found!"
    echo "Please run this script from the project root directory."
    exit 1
fi

# Create nginx directory structure
echo "Setting up nginx directories..."
mkdir -p ./nginx/ssl
mkdir -p ./nginx/logs
echo "✓ Directories created"
echo ""

# Step 1: Generate SSL Certificates
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 1: SSL Certificate Generation"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

SSL_DIR="./nginx/ssl"

# Check if certificates already exist
if [ -f "$SSL_DIR/cert.pem" ] && [ -f "$SSL_DIR/key.pem" ]; then
    echo "⚠ SSL certificates already exist!"
    read -p "Regenerate certificates? (yes/no): " regen_ssl
    if [ "$regen_ssl" != "yes" ]; then
        echo "Skipping SSL generation..."
    else
        # Backup existing
        backup_dir="$SSL_DIR/backup_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$backup_dir"
        mv "$SSL_DIR/cert.pem" "$backup_dir/" 2>/dev/null || true
        mv "$SSL_DIR/key.pem" "$backup_dir/" 2>/dev/null || true
        echo "✓ Backed up old certificates to: $backup_dir"
    fi
fi

if [ ! -f "$SSL_DIR/cert.pem" ] || [ "$regen_ssl" = "yes" ]; then
    echo "Enter SSL certificate details:"
    echo ""

    read -p "Server IP address or domain [185.213.25.170]: " SERVER_NAME
    SERVER_NAME=${SERVER_NAME:-185.213.25.170}

    read -p "Country Code (2 letters) [KE]: " COUNTRY
    COUNTRY=${COUNTRY:-KE}

    read -p "State/Province [Nairobi]: " STATE
    STATE=${STATE:-Nairobi}

    read -p "City [Nairobi]: " CITY
    CITY=${CITY:-Nairobi}

    read -p "Organization [MyOrg]: " ORG
    ORG=${ORG:-MyOrg}

    read -p "Organizational Unit [IT]: " OU
    OU=${OU:-IT}

    read -p "Certificate validity in days [365]: " DAYS
    DAYS=${DAYS:-365}

    echo ""
    echo "Generating SSL certificates..."

    # Generate private key
    openssl genrsa -out "$SSL_DIR/key.pem" 4096 2>/dev/null

    # Generate certificate
    openssl req -new -x509 -key "$SSL_DIR/key.pem" -out "$SSL_DIR/cert.pem" -days $DAYS \
        -subj "/C=$COUNTRY/ST=$STATE/L=$CITY/O=$ORG/OU=$OU/CN=$SERVER_NAME" \
        -addext "subjectAltName=IP:$SERVER_NAME,DNS:$SERVER_NAME,DNS:localhost,IP:127.0.0.1" 2>/dev/null

    # Set permissions
    chmod 600 "$SSL_DIR/key.pem"
    chmod 644 "$SSL_DIR/cert.pem"

    echo "✓ SSL certificates generated successfully!"
    echo ""
    echo "Certificate Details:"
    openssl x509 -in "$SSL_DIR/cert.pem" -noout -subject -dates
    echo ""
fi

# Step 2: Update IP Whitelist
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STEP 2: IP Whitelist Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

NGINX_CONF="./nginx/nginx.conf"

# Check if nginx.conf exists
if [ ! -f "$NGINX_CONF" ]; then
    echo "ERROR: nginx.conf not found at $NGINX_CONF"
    echo "Please create the nginx configuration file first."
    exit 1
fi

# Backup nginx.conf
backup_name="./nginx/nginx.conf.backup.$(date +%Y%m%d_%H%M%S)"
cp "$NGINX_CONF" "$backup_name"
echo "✓ Backed up nginx.conf to: $backup_name"
echo ""

# Detect current IP
echo "Detecting your current public IP address..."
CURRENT_IP=$(curl -s https://api.ipify.org 2>/dev/null || curl -s https://ifconfig.me 2>/dev/null || echo "")

if [ -z "$CURRENT_IP" ]; then
    echo "⚠ Could not auto-detect public IP"
    CURRENT_IP="185.213.25.170"
else
    echo "✓ Detected current public IP: $CURRENT_IP"
fi
echo ""

# Get IPs to whitelist
echo "Configure IP whitelist:"
echo "Enter IP addresses to allow (one per line)"
echo "Press Enter with empty line when done"
echo ""

IPS=()
read -p "IP address 1 [$CURRENT_IP]: " IP1
IP1=${IP1:-$CURRENT_IP}
IPS+=("$IP1")

counter=2
while true; do
    read -p "IP address $counter (or press Enter to finish): " IP
    if [ -z "$IP" ]; then
        break
    fi
    IPS+=("$IP")
    ((counter++))
done

echo ""
echo "IPs to whitelist:"
for ip in "${IPS[@]}"; do
    echo "  ✓ $ip/32"
done
echo "  ✓ 127.0.0.1/32 (localhost - always included)"
echo ""

read -p "Continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# Update nginx.conf
echo ""
echo "Updating nginx configuration..."

# Create new geo block
GEO_BLOCK="    # IP Whitelist - Allowed IPs
    geo \$allowed_ip {
        default 0;
        127.0.0.1/32 1;  # Allow localhost"

for ip in "${IPS[@]}"; do
    GEO_BLOCK="$GEO_BLOCK
        $ip/32 1;"
done

GEO_BLOCK="$GEO_BLOCK
    }"

# Replace geo block in nginx.conf
awk -v new_geo="$GEO_BLOCK" '
BEGIN { in_geo=0; printed=0 }
/^[[:space:]]*# IP Whitelist/ { in_geo=1; next }
in_geo && /^[[:space:]]*geo \$allowed_ip \{/ { next }
in_geo && /^[[:space:]]*\}/ {
    if (!printed) {
        print new_geo
        printed=1
    }
    in_geo=0
    next
}
in_geo { next }
{ print }
' "$NGINX_CONF" > "${NGINX_CONF}.tmp"

mv "${NGINX_CONF}.tmp" "$NGINX_CONF"

echo "✓ nginx.conf updated successfully!"
echo ""

# Display updated configuration
echo "Updated IP Whitelist:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -A 20 "# IP Whitelist" "$NGINX_CONF" | grep -B 1 -A 20 "geo \$allowed_ip" | head -n 15
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Final summary
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    Setup Complete!                         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Summary:"
echo "  ✓ SSL certificates generated in: $SSL_DIR"
echo "  ✓ Nginx configuration updated"
echo "  ✓ IP whitelist configured with ${#IPS[@]} IP(s)"
echo ""
echo "Next Steps:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "1. Start/restart your services:"
echo "   docker compose -f docker-compose.prod.yml up -d"
echo ""
echo "2. Verify nginx is running:"
echo "   docker compose -f docker-compose.prod.yml ps nginx"
echo ""
echo "3. Check nginx logs if needed:"
echo "   docker compose -f docker-compose.prod.yml logs -f nginx"
echo ""
echo "4. Access your applications:"
echo "   • Streamlit:  https://${IP1}"
echo "   • Airflow:    https://${IP1}:8443"
echo "   • VNC:        https://${IP1}/vnc/"
echo ""
echo "IMPORTANT NOTES:"
echo "  • These are self-signed certificates"
echo "  • Your browser will show a security warning"
echo "  • Click 'Advanced' and 'Proceed' to accept the certificate"
echo "  • For production, consider using Let's Encrypt"
echo ""
echo "Troubleshooting:"
echo "  • If you can't access: Check your firewall allows ports 80, 443, 8443"
echo "  • If 403 error: Verify your current IP is in the whitelist"
echo "  • Test nginx config: docker compose -f docker-compose.prod.yml exec nginx nginx -t"
echo ""
