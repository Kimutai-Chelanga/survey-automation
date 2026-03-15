#!/bin/bash
# Manual IP whitelist updater
# Usage: ./set_ip.sh

echo "Current whitelisted IPs:"
grep -A 5 "geo \$allowed_ip" /opt/final-automation/nginx/nginx.conf | grep -E "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"
echo ""

read -p "Enter IP address to whitelist: " IP

# Validate IP format
if [[ ! $IP =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    echo "❌ Invalid IP address format"
    exit 1
fi

echo "IP to add: $IP"

# Check if already whitelisted
if grep -q "$IP" /opt/final-automation/nginx/nginx.conf; then
    echo "✅ IP already whitelisted"
    exit 0
fi

# Add IP to whitelist (after 127.0.0.1 line)
cd /opt/final-automation
sed -i "/127.0.0.1\/32 1;/a \        $IP/32 1;" nginx/nginx.conf

echo "✅ Done! IP $IP added to whitelist"
