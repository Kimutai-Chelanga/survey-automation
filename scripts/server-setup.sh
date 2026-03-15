#!/bin/bash
# Server setup script for Contabo VPS
# Run this script ONCE on your Contabo server after initial setup

set -e

echo "🚀 Starting server setup for final-automation..."

# Update system
echo "📦 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install required packages
echo "📦 Installing required packages..."
sudo apt install -y \
    git \
    curl \
    wget \
    ufw \
    fail2ban \
    ca-certificates \
    gnupg \
    lsb-release

# Install Docker
echo "🐳 Installing Docker..."
if ! command -v docker &> /dev/null; then
    # Add Docker's official GPG key
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

    # Set up Docker repository
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    # Install Docker Engine
    sudo apt update
    sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

    # Add current user to docker group
    sudo usermod -aG docker $USER
    echo "✅ Docker installed successfully"
else
    echo "✅ Docker already installed"
fi

# Configure firewall
echo "🔥 Configuring firewall..."
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
echo "y" | sudo ufw enable
echo "✅ Firewall configured"

# Configure fail2ban for SSH protection
echo "🛡️ Configuring fail2ban..."
sudo systemctl enable fail2ban
sudo systemctl start fail2ban

# Create application directory
echo "📁 Creating application directory..."
sudo mkdir -p /opt/final-automation
sudo chown $USER:$USER /opt/final-automation

# Generate SSH key for GitHub (if not exists)
if [ ! -f ~/.ssh/id_ed25519 ]; then
    echo "🔑 Generating SSH key for GitHub..."
    ssh-keygen -t ed25519 -C "server@final-automation" -f ~/.ssh/id_ed25519 -N ""
    echo ""
    echo "📋 Add this SSH key to your GitHub repository:"
    echo "   Settings > Deploy keys > Add deploy key"
    echo ""
    cat ~/.ssh/id_ed25519.pub
    echo ""
    read -p "Press Enter after adding the deploy key to GitHub..."
fi

# Clone repository
echo "📥 Cloning repository..."
cd /opt/final-automation
if [ ! -d ".git" ]; then
    git clone git@github.com:pnata0890-dev/final-automation.git .
else
    echo "✅ Repository already cloned"
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p nginx/ssl data logs chrome-data airflow/{dags,logs,plugins}

# Generate self-signed SSL certificate for testing
echo "🔐 Generating self-signed SSL certificate..."
if [ ! -f nginx/ssl/cert.pem ]; then
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout nginx/ssl/key.pem \
        -out nginx/ssl/cert.pem \
        -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"
    echo "✅ SSL certificate generated"
fi

# Create .env.production file template
echo "📝 Creating .env.production template..."
if [ ! -f .env.production ]; then
    cat > .env.production << 'EOF'
# Production environment variables
# IMPORTANT: Replace all values with secure random passwords

# PostgreSQL
POSTGRES_USER=automation_user
POSTGRES_PASSWORD=CHANGE_ME_STRONG_PASSWORD_1
POSTGRES_DB=automation_db

# Airflow PostgreSQL
AIRFLOW_DB_PASSWORD=CHANGE_ME_STRONG_PASSWORD_2

# MongoDB
MONGO_ROOT_USER=admin
MONGO_ROOT_PASSWORD=CHANGE_ME_STRONG_PASSWORD_3
MONGO_DB=automation_db

# Chrome VNC
VNC_PASSWORD=CHANGE_ME_STRONG_PASSWORD_4

# Application
SECRET_KEY=CHANGE_ME_SECRET_KEY_5
ENVIRONMENT=production
DEBUG=false

# GitHub (for private registry access)
GITHUB_REPOSITORY=pnata0890-dev/final-automation
EOF
    echo "⚠️  IMPORTANT: Edit .env.production and change all passwords!"
    echo "   Run: nano /opt/final-automation/.env.production"
fi

# Set proper permissions
echo "🔒 Setting proper permissions..."
chmod 600 .env.production
chmod 700 nginx/ssl

# Login to GitHub Container Registry
echo "🔑 Setting up GitHub Container Registry access..."
echo "You'll need a GitHub Personal Access Token with 'read:packages' permission"
echo "Create one at: https://github.com/settings/tokens/new"
echo ""
read -p "Enter your GitHub username: " GITHUB_USER
read -sp "Enter your GitHub Personal Access Token: " GITHUB_TOKEN
echo ""
echo "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_USER" --password-stdin

# Create systemd service for automatic startup
echo "⚙️ Creating systemd service..."
sudo tee /etc/systemd/system/final-automation.service > /dev/null << EOF
[Unit]
Description=Final Automation Docker Compose
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/final-automation
ExecStart=/usr/bin/docker compose -f docker-compose.prod.yml up -d
ExecStop=/usr/bin/docker compose -f docker-compose.prod.yml down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable final-automation.service

echo ""
echo "✅ Server setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit /opt/final-automation/.env.production with secure passwords"
echo "2. Update nginx/nginx.conf with your IP address"
echo "3. Configure GitHub Secrets in your repository:"
echo "   - SERVER_HOST: Your Contabo server IP"
echo "   - SERVER_USER: Your SSH username (usually 'root' or your username)"
echo "   - SERVER_SSH_KEY: Your private SSH key (~/.ssh/id_ed25519)"
echo "   - SERVER_PORT: SSH port (usually 22)"
echo "4. Push to main branch to trigger automatic deployment"
echo ""
echo "Manual deployment:"
echo "   cd /opt/final-automation"
echo "   docker compose -f docker-compose.prod.yml up -d"
echo ""