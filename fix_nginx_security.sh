#!/bin/bash

# fix_nginx_security.sh
# Adds IP whitelist enforcement to nginx.conf
# Usage: ./fix_nginx_security.sh

set -e

echo "╔════════════════════════════════════════════════════════════╗"
echo "║          Nginx IP Security Enforcement Setup              ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

NGINX_CONF="./nginx/nginx.conf"

# Check if nginx.conf exists
if [ ! -f "$NGINX_CONF" ]; then
    echo "❌ ERROR: nginx.conf not found at $NGINX_CONF"
    exit 1
fi

# Backup existing nginx.conf
backup_name="./nginx/nginx.conf.backup.$(date +%Y%m%d_%H%M%S)"
cp "$NGINX_CONF" "$backup_name"
echo "✓ Backed up nginx.conf to: $backup_name"
echo ""

# Detect your current public IP
echo "🔍 Detecting your current public IP address..."
echo ""
echo "IMPORTANT: This is YOUR IP (where you browse from), NOT the server IP!"
echo "  • Your IP (client): The IP you're browsing from"
echo "  • Server IP: 185.213.25.170 (where services run)"
echo ""
YOUR_IP=$(curl -s https://api.ipify.org 2>/dev/null || curl -s https://ifconfig.me 2>/dev/null || echo "")

if [ -z "$YOUR_IP" ]; then
    echo "⚠ Could not auto-detect your public IP"
    echo "Visit https://whatismyipaddress.com/ to find your IP"
    read -p "Enter your public IP address: " YOUR_IP
else
    echo "✓ Detected your IP: $YOUR_IP"
    echo ""
    read -p "Is this your current browsing IP (not server IP)? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo ""
        echo "Visit https://whatismyipaddress.com/ to find your correct IP"
        read -p "Enter your correct public IP address: " YOUR_IP
    fi
fi

echo ""
echo "📝 IP Whitelist Configuration:"
echo "   • Your IP: $YOUR_IP/32"
echo "   • Localhost: 127.0.0.1/32"
echo ""

# Allow additional IPs
echo "Do you want to add additional IPs? (optional)"
read -p "Enter additional IP (or press Enter to skip): " ADDITIONAL_IP

IPS=("$YOUR_IP")
if [ -n "$ADDITIONAL_IP" ]; then
    IPS+=("$ADDITIONAL_IP")
fi

# Create the new nginx.conf with IP enforcement
cat > "$NGINX_CONF" << 'NGINX_EOF'
user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log warn;
pid /var/run/nginx.pid;

events {
    worker_connections 1024;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    log_format main '$remote_addr - $remote_user [$time_local] "$request" '
                    '$status $body_bytes_sent "$http_referer" '
                    '"$http_user_agent" "$http_x_forwarded_for"';

    access_log /var/log/nginx/access.log main;

    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    types_hash_max_size 2048;
    client_max_body_size 100M;

    # Gzip Settings
    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_types text/plain text/css text/xml text/javascript
               application/json application/javascript application/xml+rss
               application/rss+xml font/truetype font/opentype
               application/vnd.ms-fontobject image/svg+xml;

    # IP Whitelist - Allowed IPs
    geo $allowed_ip {
        default 0;
        127.0.0.1/32 1;  # Allow localhost
NGINX_EOF

# Add whitelisted IPs
for ip in "${IPS[@]}"; do
    echo "        $ip/32 1;" >> "$NGINX_CONF"
done

# Continue with the rest of the config
cat >> "$NGINX_CONF" << 'NGINX_EOF'
    }

    # Map to create better error pages for blocked IPs
    map $allowed_ip $access_status {
        0 "blocked";
        1 "allowed";
    }

    # Rate limiting
    limit_req_zone $binary_remote_addr zone=general:10m rate=10r/s;
    limit_req_zone $binary_remote_addr zone=api:10m rate=30r/s;

    # SSL Settings
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;

    # Upstream definitions
    upstream streamlit {
        server app:8501;
    }

    upstream airflow {
        server airflow-webserver:8080;
    }

    upstream vnc {
        server app:6080;
    }

    # HTTP Server - Redirect to HTTPS
    server {
        listen 80;
        server_name _;

        # IP Whitelist Check - CRITICAL SECURITY
        if ($allowed_ip = 0) {
            return 403 "Access denied. Your IP ($remote_addr) is not whitelisted.";
        }

        # Redirect all HTTP to HTTPS
        location / {
            return 301 https://$host$request_uri;
        }
    }

    # HTTPS Server - Streamlit App
    server {
        listen 443 ssl;
        http2 on;
        server_name _;

        ssl_certificate /etc/nginx/ssl/cert.pem;
        ssl_certificate_key /etc/nginx/ssl/key.pem;

        # IP Whitelist Check - CRITICAL SECURITY
        if ($allowed_ip = 0) {
            return 403 "Access denied. Your IP ($remote_addr) is not whitelisted.";
        }

        # Rate limiting
        limit_req zone=general burst=20 nodelay;

        # Security headers
        add_header X-Frame-Options "SAMEORIGIN" always;
        add_header X-Content-Type-Options "nosniff" always;
        add_header X-XSS-Protection "1; mode=block" always;
        add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

        # Streamlit WebSocket and HTTP
        location / {
            proxy_pass http://streamlit;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Host $host;
            proxy_set_header X-Forwarded-Port $server_port;

            proxy_buffering off;
            proxy_read_timeout 86400;
            proxy_redirect off;
        }

        # Streamlit health check
        location /_stcore/health {
            proxy_pass http://streamlit/_stcore/health;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            access_log off;
        }

        # VNC Web Interface
        location /vnc/ {
            proxy_pass http://vnc/;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            proxy_buffering off;
            proxy_read_timeout 86400;
        }
    }

    # HTTPS Server - Airflow
    server {
        listen 8443 ssl;
        http2 on;
        server_name _;

        ssl_certificate /etc/nginx/ssl/cert.pem;
        ssl_certificate_key /etc/nginx/ssl/key.pem;

        # IP Whitelist Check - CRITICAL SECURITY
        if ($allowed_ip = 0) {
            return 403 "Access denied. Your IP ($remote_addr) is not whitelisted.";
        }

        # Rate limiting
        limit_req zone=api burst=50 nodelay;

        # Security headers
        add_header X-Frame-Options "SAMEORIGIN" always;
        add_header X-Content-Type-Options "nosniff" always;
        add_header X-XSS-Protection "1; mode=block" always;
        add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

        # Airflow Web UI
        location / {
            proxy_pass http://airflow;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Host $host;
            proxy_set_header X-Forwarded-Port $server_port;

            proxy_buffering off;
            proxy_read_timeout 300;
            proxy_connect_timeout 300;
            proxy_send_timeout 300;
        }

        # Airflow health check
        location /health {
            proxy_pass http://airflow/health;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            access_log off;
        }
    }
}
NGINX_EOF

echo "✅ nginx.conf updated with IP enforcement!"
echo ""

# Display the whitelist
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 IP Whitelist Configuration:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -A 10 "# IP Whitelist" "$NGINX_CONF" | grep -E "^\s+[0-9]+\." || echo "Error displaying IPs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                    Setup Complete!                         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "🔒 Security Status:"
echo "   ✓ IP whitelist enforcement ENABLED"
echo "   ✓ Only whitelisted IPs can access services"
echo "   ✓ All HTTP redirected to HTTPS"
echo ""
echo "📍 Your Access URLs:"
echo "   • Streamlit:  https://185.213.25.170"
echo "   • Airflow:    https://185.213.25.170:8443"
echo "   • VNC:        https://185.213.25.170/vnc/"
echo ""
echo "⚠️  IMPORTANT: Make sure ports are open on your server:"
echo "   sudo ufw allow 80/tcp"
echo "   sudo ufw allow 443/tcp"
echo "   sudo ufw allow 8443/tcp"
echo ""
echo "🚀 Next Steps:"
echo "   1. Restart nginx to apply changes:"
echo "      docker compose -f docker-compose.prod.yml restart nginx"
echo ""
echo "   2. Test access from your IP:"
echo "      curl -I https://185.213.25.170"
echo ""
echo "   3. Verify blocking from other IPs (should get 403)"
echo ""
echo "📝 To add more IPs later, edit: ./nginx/nginx.conf"
echo "   Then restart: docker compose -f docker-compose.prod.yml restart nginx"
echo ""
