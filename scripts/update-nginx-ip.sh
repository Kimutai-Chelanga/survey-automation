#!/bin/bash
set -e

echo "=========================================="
echo "Update Nginx IP Whitelist"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Get current public IP
echo "🌐 Detecting your current public IP..."
CURRENT_IP=$(curl -s ifconfig.me)

if [ -z "$CURRENT_IP" ]; then
    echo -e "${RED}❌ Failed to detect public IP${NC}"
    echo "Please enter your IP manually:"
    read -p "Your public IP: " CURRENT_IP
fi

echo -e "${GREEN}✓ Your current IP: $CURRENT_IP${NC}"
echo ""

# Check nginx config file
NGINX_CONF="nginx/nginx.conf"

if [ ! -f "$NGINX_CONF" ]; then
    echo -e "${RED}❌ Error: $NGINX_CONF not found${NC}"
    exit 1
fi

# Check if IP is already in config
if grep -q "$CURRENT_IP" "$NGINX_CONF"; then
    echo -e "${GREEN}✓ Your IP ($CURRENT_IP) is already in nginx.conf${NC}"
    echo ""
    echo "Current whitelist:"
    grep -A 5 "geo \$whitelist" "$NGINX_CONF"
    exit 0
fi

# Check for placeholder
if grep -q "YOUR_IP_ADDRESS_HERE" "$NGINX_CONF"; then
    echo -e "${YELLOW}⚠️  Found placeholder IP in config${NC}"
    read -p "Replace placeholder with $CURRENT_IP? (Y/n): " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        # Replace placeholder
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "s/YOUR_IP_ADDRESS_HERE/$CURRENT_IP/g" "$NGINX_CONF"
        else
            # Linux
            sed -i "s/YOUR_IP_ADDRESS_HERE/$CURRENT_IP/g" "$NGINX_CONF"
        fi
        echo -e "${GREEN}✓ Updated nginx.conf with your IP${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  No placeholder found${NC}"
    read -p "Add $CURRENT_IP to whitelist? (Y/n): " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        # Add IP after the "default 0;" line
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            sed -i '' "/default 0;/a\\
    $CURRENT_IP 1;  # Added $(date +%Y-%m-%d)
" "$NGINX_CONF"
        else
            # Linux
            sed -i "/default 0;/a\\    $CURRENT_IP 1;  # Added $(date +%Y-%m-%d)" "$NGINX_CONF"
        fi
        echo -e "${GREEN}✓ Added your IP to nginx.conf${NC}"
    fi
fi

echo ""
echo "Current whitelist configuration:"
echo "=========================================="
grep -A 10 "geo \$whitelist" "$NGINX_CONF"
echo "=========================================="
echo ""

# Validate nginx config
if command -v docker &> /dev/null; then
    echo "🔍 Validating nginx configuration..."
    docker run --rm -v "$(pwd)/nginx/nginx.conf:/etc/nginx/nginx.conf:ro" nginx:alpine nginx -t 2>&1 | grep -q "successful" && \
        echo -e "${GREEN}✓ Nginx configuration is valid${NC}" || \
        echo -e "${RED}❌ Nginx configuration has errors${NC}"
fi

echo ""
echo -e "${GREEN}Next steps:${NC}"
echo "1. Review the whitelist above"
echo "2. Add more IPs if needed (office, VPN, etc.)"
echo "3. Commit changes: git add nginx/nginx.conf"
echo ""
