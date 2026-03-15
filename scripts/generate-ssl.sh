#!/bin/bash
# Generate self-signed SSL certificates for nginx
# Run this on your Contabo server after setup

set -e

echo "🔐 Generating SSL certificates for nginx..."

# Create ssl directory
mkdir -p nginx/ssl
cd nginx/ssl

# Generate self-signed certificate
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout key.pem \
  -out cert.pem \
  -subj "/C=KE/ST=Nairobi/L=Nairobi/O=Organization/CN=localhost"

# Set proper permissions
chmod 600 key.pem
chmod 644 cert.pem

echo "✅ SSL certificates generated:"
echo "   - Certificate: $(pwd)/cert.pem"
echo "   - Private key: $(pwd)/key.pem"
echo ""
echo "⚠️  Note: These are self-signed certificates."
echo "   Your browser will show a security warning - this is expected."
echo "   Click 'Advanced' → 'Proceed' to access your application."
echo ""
echo "💡 For production with a domain name, consider using Let's Encrypt:"
echo "   https://letsencrypt.org/getting-started/"
