# ⚡ Quick Start Guide
## 5-Minute Setup Summary

---

## Before Server Purchase

### 1. Get Your IP
```bash
curl ifconfig.me
# Save this number!
```

### 2. Setup Local Environment
```bash
# Clone and prepare
git clone https://github.com/pnata0890-dev/final-automation.git
cd final-automation
chmod +x scripts/*.sh

# Generate production config
./scripts/generate-production-env.sh
# Save credentials shown!

# Update nginx with your IP
./scripts/update-nginx-ip.sh

# Test locally
./scripts/test-deployment.sh
```

### 3. Create GitHub Token
- Go to: https://github.com/settings/tokens/new
- Name: "Contabo Deployment"
- Permissions: `write:packages`, `read:packages`
- Copy token (save it!)

### 4. Verify & Commit
```bash
./scripts/verify-before-commit.sh
# Must show: ✅ SAFE TO COMMIT

git add .
git commit -m "Production ready"
git push origin main
```

---

## After Getting Contabo Server

### 1. First Login
```bash
ssh root@YOUR_SERVER_IP
# Enter password from Contabo email

# Change password immediately
passwd
```

### 2. Run Setup
```bash
apt update && apt upgrade -y
apt install -y git

cd /root
git clone https://github.com/pnata0890-dev/final-automation.git
cd final-automation

chmod +x scripts/*.sh
./scripts/server-setup.sh
# Follow prompts to add SSH key to GitHub
```

### 3. Upload Environment
```bash
# From your local machine
scp .env.production root@YOUR_SERVER_IP:/opt/final-automation/
```

### 4. Configure GitHub Secrets
Go to: `https://github.com/YOUR_REPO/settings/secrets/actions`

Add these secrets:
- `SERVER_HOST` → Your server IP
- `SERVER_USER` → `root`
- `SERVER_PORT` → `22`
- `SERVER_SSH_KEY` → Output of `cat ~/.ssh/id_ed25519` on server

### 5. Deploy
```bash
# On server
cd /opt/final-automation

# Login to registry
echo "YOUR_GITHUB_TOKEN" | docker login ghcr.io -u pnata0890-dev --password-stdin

# Deploy
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d

# Watch it start
docker compose -f docker-compose.prod.yml logs -f
```

---

## Access Your Application

Open in browser:
- **Streamlit**: `https://YOUR_IP/`
- **Airflow**: `https://YOUR_IP/airflow/`
- **VNC**: `https://YOUR_IP/vnc/`

**SSL Warning:** Click "Advanced" → "Proceed" (normal for self-signed certs)

Login with credentials from `.production-credentials.txt`

---

## Common Commands

```bash
# View services
docker compose -f docker-compose.prod.yml ps

# View logs
docker compose -f docker-compose.prod.yml logs -f

# Restart
docker compose -f docker-compose.prod.yml restart

# Update
cd /opt/final-automation
git pull
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

---

## Troubleshooting

**Can't access?**
```bash
# Check firewall
sudo ufw status

# Check services
docker compose -f docker-compose.prod.yml ps

# View nginx logs
docker logs nginx-proxy
```

**IP changed?**
```bash
nano nginx/nginx.conf
# Add new IP to whitelist
docker compose -f docker-compose.prod.yml restart nginx
```

---

## Need Help?

See full guide: `DEPLOYMENT_GUIDE.md`

**Everything working?** ✅ You're done! 🎉
