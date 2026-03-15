#!/bin/bash
# Simple IP whitelist updater
# Usage: ./update_ip.sh

# Get current IP
IP=$(curl -s https://api.ipify.org)

if [ -z "$IP" ]; then
    echo "❌ Could not detect IP"
    exit 1
fi

echo "Current IP: $IP"

# Check if already whitelisted
if grep -q "$IP" /opt/final-automation/nginx/nginx.conf; then
    echo "✅ IP already whitelisted"
    exit 0
fi

# Add IP to whitelist (after 127.0.0.1 line)
cd /opt/final-automation
sed -i "/127.0.0.1\/32 1;/a \        $IP/32 1;" nginx/nginx.conf

echo "✅ Done! IP $IP added to whitelist"
