# 🚀 Complete Deployment Guide
## From GitHub Code to Running Server

---

## 📋 What You'll Need

### Before Starting:
- [ ] GitHub account with your repository
- [ ] Contabo VPS purchased (recommended: VPS M or VPS L)
- [ ] Your local machine with Git installed
- [ ] 2-3 hours of focused time

### What You'll Get:
- Server IP from Contabo email
- Root password from Contabo email
- Access to your application at `https://YOUR_IP/`

---

## 🎯 Step-by-Step Deployment

### Phase 1: Prepare Your Local Repository (30 minutes)

#### Step 1.1: Get Your Current IP Address

```bash
# Run this to get your public IP
curl ifconfig.me
# Example output: 203.45.67.89

# SAVE THIS IP - you'll need it!
```

**💡 Important:** If you have dynamic IP (changes frequently):
- Use a VPN with static IP, OR
- You'll need to update nginx.conf each time your IP changes

#### Step 1.2: Clone and Setup Repository

```bash
# Clone your repository
git clone https://github.com/pnata0890-dev/final-automation.git
cd final-automation

# Make all scripts executable
chmod +x scripts/*.sh
```

#### Step 1.3: Generate Production Environment

```bash
# Run the environment generator
./scripts/generate-production-env.sh
```

**What it will ask you:**
1. GitHub Repository: `pnata0890-dev/final-automation` (press Enter)
2. Server IP: Skip for now (press Enter)
3. App Username: `admin` (or your choice)
4. App Password: Create a strong password
5. API Keys: Enter yours or skip

**What it creates:**
- `.env.production` - Contains all secrets (NOT committed to Git)
- `.production-credentials.txt` - All your passwords (save then delete)

```bash
# View your credentials
cat .production-credentials.txt

# CRITICAL: Save these to password manager NOW!
# Then delete the file
rm .production-credentials.txt
```

#### Step 1.4: Update Nginx Configuration

```bash
# Run the IP update script
./scripts/update-nginx-ip.sh
```

This will:
- Detect your current IP
- Update `nginx/nginx.conf` automatically
- Validate the configuration

**Manual method (if script doesn't work):**
```bash
nano nginx/nginx.conf

# Find this section and update:
geo $whitelist {
    default 0;
    YOUR_IP_HERE 1;  # ← Replace with your actual IP from Step 1.1
}

# Save: Ctrl+X, Y, Enter
```

#### Step 1.5: Test Locally (CRITICAL!)

```bash
# Test everything works before server deployment
./scripts/test-deployment.sh
```

**Expected result:**
```
✅ ALL TESTS PASSED!
🌐 Access Points:
   • Streamlit App:  http://localhost:8501
   • VNC (Browser):  http://localhost:6080/vnc.html
```

**If tests fail:**
- Check the error messages
- Fix issues before proceeding
- Run test again until it passes

```bash
# Clean up test containers when done
docker compose -f docker-compose.test.yml down -v
```

#### Step 1.6: Verify Before Committing

```bash
# Run security verification
./scripts/verify-before-commit.sh
```

**Must show:**
```
✅ SAFE TO COMMIT
✓ No sensitive files detected
```

**If it shows issues:**
- Fix them before proceeding
- Common issue: `.env.production` in staging area
- Fix: `git reset HEAD .env.production`

#### Step 1.7: Create GitHub Personal Access Token

1. Go to: https://github.com/settings/tokens/new
2. Token name: "Contabo Deployment"
3. Expiration: 90 days or longer
4. Scopes (check these boxes):
   - ✅ `write:packages`
   - ✅ `read:packages`
5. Click "Generate token"
6. **COPY THE TOKEN IMMEDIATELY** - you can't see it again!
7. Save in password manager

#### Step 1.8: Commit and Push

```bash
# Check what will be committed
git status

# Should NOT include .env.production!
# Add files
git add .
git commit -m "Production deployment configuration"
git push origin main
```

---

### Phase 2: Purchase and Setup Contabo Server (1 hour)

#### Step 2.1: Purchase VPS

1. Go to https://contabo.com/
2. Choose VPS:
   - **Recommended: VPS M** (€9/month, 8GB RAM, 4 CPU)
   - **For heavy use: VPS L** (€15/month, 16GB RAM, 6 CPU)
3. Select:
   - OS: **Ubuntu 22.04 LTS** or **Ubuntu 24.04 LTS**
   - Region: Closest to you
4. Complete purchase

#### Step 2.2: Wait for Confirmation Email

Contabo will send you an email (usually within 1-24 hours) with:
```
Server IP: 45.67.89.123
Root Password: TempPassword123
SSH Port: 22
```

**SAVE THIS EMAIL!**

#### Step 2.3: First Connection

```bash
# Connect to your server
ssh root@YOUR_SERVER_IP
# Enter the password from email

# You should see Ubuntu welcome message
```

**Immediately change root password:**
```bash
passwd
# Enter new strong password twice
# SAVE THIS PASSWORD IN PASSWORD MANAGER!
```

#### Step 2.4: Run Server Setup Script

```bash
# Update system
apt update && apt upgrade -y

# Install git
apt install -y git

# Clone your repository
cd /root
git clone https://github.com/pnata0890-dev/final-automation.git
cd final-automation

# Make scripts executable
chmod +x scripts/*.sh

# Run server setup
./scripts/server-setup.sh
```

**What the script does:**
1. ✓ Installs Docker & Docker Compose
2. ✓ Configures firewall (only ports 22, 80, 443)
3. ✓ Sets up fail2ban (SSH brute-force protection)
4. ✓ Generates SSH deploy key
5. ✓ Generates SSL certificates
6. ✓ Creates systemd service for auto-start

**During script execution:**

**A. When it shows SSH public key:**
```
📋 Add this SSH key to your GitHub repository:
ssh-ed25519 AAAAC3Nza... server@final-automation
```

1. Copy the entire key (starts with `ssh-ed25519`)
2. Go to: https://github.com/pnata0890-dev/final-automation/settings/keys
3. Click "Add deploy key"
4. Title: `Contabo Server`
5. Paste the key
6. ✅ Check "Allow write access"
7. Click "Add key"
8. Return to terminal and press Enter

**B. When it asks for GitHub login:**
```
Enter your GitHub username: pnata0890-dev
Enter your GitHub Personal Access Token:
```
Paste your token from Step 1.7 (it won't show when typing - that's normal)

**Script should end with:**
```
✅ Server setup complete!
```

#### Step 2.5: Upload Production Environment

You need to copy `.env.production` from your local machine to the server.

**Option A: Using SCP (from your local machine):**
```bash
# In a NEW terminal on your local machine
cd /path/to/final-automation

scp .env.production root@YOUR_SERVER_IP:/opt/final-automation/
```

**Option B: Create directly on server:**
```bash
# On the server
cd /opt/final-automation
nano .env.production

# Paste the content from your local .env.production
# Save: Ctrl+X, Y, Enter

# Set secure permissions
chmod 600 .env.production
```

#### Step 2.6: Update Server IP in .env.production

```bash
# On the server
cd /opt/final-automation
nano .env.production

# Find this line:
SERVER_HOST=YOUR_SERVER_IP

# Replace with your actual IP:
SERVER_HOST=45.67.89.123

# Save: Ctrl+X, Y, Enter
```

#### Step 2.7: Configure GitHub Secrets

1. Go to: https://github.com/pnata0890-dev/final-automation/settings/secrets/actions
2. Click "New repository secret" for each:

**Secret 1: SERVER_HOST**
- Name: `SERVER_HOST`
- Value: Your Contabo IP (e.g., `45.67.89.123`)

**Secret 2: SERVER_USER**
- Name: `SERVER_USER`
- Value: `root`

**Secret 3: SERVER_PORT**
- Name: `SERVER_PORT`
- Value: `22`

**Secret 4: SERVER_SSH_KEY**
- Name: `SERVER_SSH_KEY`
- Value: Your private SSH key

To get the SSH key:
```bash
# On your Contabo server
cat ~/.ssh/id_ed25519

# Copy ENTIRE output including:
# -----BEGIN OPENSSH PRIVATE KEY-----
# ... all the content ...
# -----END OPENSSH PRIVATE KEY-----
```

---

### Phase 3: Deploy Application (30 minutes)

#### Option A: Manual Deployment (Recommended First Time)

```bash
# On your Contabo server
cd /opt/final-automation

# Login to GitHub Container Registry
echo "YOUR_GITHUB_TOKEN" | docker login ghcr.io -u pnata0890-dev --password-stdin
# Replace YOUR_GITHUB_TOKEN with token from Step 1.7

# Pull and start all services
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d

# Watch logs to ensure everything starts
docker compose -f docker-compose.prod.yml logs -f
```

**What you should see:**
```
postgres_db        | database system is ready to accept connections
mongodb            | Waiting for connections on port 27017
streamlit_app      | You can now view your Streamlit app in your browser.
airflow_webserver  | Uvicorn running on http://0.0.0.0:8080
nginx-proxy        | nginx entered RUNNING state
```

Press `Ctrl+C` to stop watching (services keep running).

**Check service status:**
```bash
docker compose -f docker-compose.prod.yml ps
```

All services should show "Up" and "healthy".

#### Option B: Automatic Deployment via GitHub Actions

After manual deployment works once:

```bash
# On your local machine
git add .
git commit -m "Trigger deployment"
git push origin main
```

GitHub Actions will automatically:
1. ✓ Build Docker images
2. ✓ Push to GitHub Container Registry
3. ✓ SSH to your server
4. ✓ Pull latest images
5. ✓ Restart services

Watch progress: https://github.com/pnata0890-dev/final-automation/actions

---

## 🌐 Accessing Your Services

### Get Your Server IP

From Contabo email or run on server:
```bash
curl ifconfig.me
```

### Access Points

**1. Streamlit Application (Main Interface)**
```
URL: https://YOUR_SERVER_IP/
Username: admin (or what you set)
Password: (from .production-credentials.txt)
```

**2. Airflow Dashboard (Workflow Management)**
```
URL: https://YOUR_SERVER_IP/airflow/
Username: admin
Password: (from .production-credentials.txt)
```

**3. VNC Browser Access (See Chrome in Action)**
```
URL: https://YOUR_SERVER_IP/vnc/
Password: (from .production-credentials.txt)
```

### Browser SSL Warning (Expected!)

When you first visit `https://YOUR_IP/`, you'll see:
```
⚠️ Your connection is not private
   NET::ERR_CERT_AUTHORITY_INVALID
```

**This is NORMAL!** Here's why and how to proceed:

**Chrome/Edge:**
1. Click "Advanced"
2. Click "Proceed to [your-ip] (unsafe)"

**Firefox:**
1. Click "Advanced"
2. Click "Accept the Risk and Continue"

**Safari:**
1. Click "Show Details"
2. Click "visit this website"

**Why this happens:**
- You're using a self-signed SSL certificate
- Still encrypted (HTTPS), just not verified by a certificate authority
- Safe because YOU created the certificate

### Alternative: SSH Tunnel (No SSL Warnings)

```bash
# From your local machine
ssh -L 8501:localhost:8501 \
    -L 6080:localhost:6080 \
    -L 8080:localhost:8080 \
    root@YOUR_SERVER_IP

# Then access via localhost:
# http://localhost:8501  - Streamlit
# http://localhost:6080  - VNC
# http://localhost:8080  - Airflow
```

---

## 🔍 Verification Checklist

After deployment, verify everything works:

### Check Services
```bash
# On server
cd /opt/final-automation
docker compose -f docker-compose.prod.yml ps
```

**All should show "Up (healthy)":**
- ✅ nginx-proxy
- ✅ streamlit_app
- ✅ postgres_db
- ✅ mongodb
- ✅ airflow_webserver
- ✅ airflow_scheduler

### Check Logs
```bash
# All services
docker compose -f docker-compose.prod.yml logs --tail=50

# Specific service
docker compose -f docker-compose.prod.yml logs -f app
```

### Check from Browser
1. ✅ Can access `https://YOUR_IP/` (Streamlit login)
2. ✅ Can login with credentials
3. ✅ Can access `https://YOUR_IP/airflow/`
4. ✅ Can access `https://YOUR_IP/vnc/`
5. ✅ Can't access from different IP (security working!)

### Check Resources
```bash
# Resource usage
docker stats

# Disk space
df -h

# Memory
free -h
```

---

## 🛠️ Common Operations

### View Logs
```bash
# Real-time all services
docker compose -f docker-compose.prod.yml logs -f

# Specific service
docker compose -f docker-compose.prod.yml logs -f streamlit_app

# Last 100 lines
docker compose -f docker-compose.prod.yml logs --tail=100
```

### Restart Services
```bash
# Restart all
docker compose -f docker-compose.prod.yml restart

# Restart one service
docker compose -f docker-compose.prod.yml restart app
```

### Update Application
```bash
# On server
cd /opt/final-automation

# Pull latest code
git pull origin main

# Pull new images
docker compose -f docker-compose.prod.yml pull

# Restart with new code
docker compose -f docker-compose.prod.yml up -d

# Clean old images
docker image prune -af
```

### Stop/Start
```bash
# Stop everything
docker compose -f docker-compose.prod.yml down

# Start everything
docker compose -f docker-compose.prod.yml up -d

# Stop and remove all data (⚠️ DANGEROUS!)
docker compose -f docker-compose.prod.yml down -v
```

---

## 🚨 Troubleshooting

### Can't Access from Browser

**Problem:** Connection refused or timeout

**Solutions:**
```bash
# 1. Check services are running
docker compose -f docker-compose.prod.yml ps

# 2. Check firewall
sudo ufw status

# 3. Check nginx logs
docker logs nginx-proxy

# 4. Verify your IP is whitelisted
curl ifconfig.me
grep -A 5 "geo \$whitelist" nginx/nginx.conf
```

### IP Changed

**Problem:** Got 403 Forbidden after IP change

**Solution:**
```bash
# On server
cd /opt/final-automation
nano nginx/nginx.conf

# Add new IP to whitelist:
geo $whitelist {
    default 0;
    203.45.67.89 1;  # Old IP
    198.51.100.20 1; # New IP - ADD THIS
}

# Restart nginx
docker compose -f docker-compose.prod.yml restart nginx
```

### Service Won't Start

**Problem:** Container exits immediately

**Solutions:**
```bash
# Check which service failed
docker compose -f docker-compose.prod.yml ps

# View logs for failed service
docker compose -f docker-compose.prod.yml logs SERVICE_NAME

# Common fixes:
# 1. Database issue - wait and restart
docker compose -f docker-compose.prod.yml restart postgres mongodb

# 2. Out of disk space
df -h
docker system prune -af

# 3. Out of memory
free -h
docker compose -f docker-compose.prod.yml restart
```

### GitHub Actions Fails

**Problem:** Deployment workflow fails

**Solutions:**
1. Check workflow logs: https://github.com/YOUR_REPO/actions
2. Verify secrets are set correctly
3. Test SSH connection:
   ```bash
   ssh root@YOUR_SERVER_IP
   ```
4. Manual deployment:
   ```bash
   cd /opt/final-automation
   git pull origin main
   docker compose -f docker-compose.prod.yml up -d
   ```

---

## 📊 Monitoring

### Daily Checks
```bash
# Service health
docker compose -f docker-compose.prod.yml ps

# Resource usage
docker stats

# Disk space
df -h

# Backup status
ls -lh backups/
```

### Weekly Maintenance
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Clean Docker
docker system prune -af

# Check logs for errors
docker compose -f docker-compose.prod.yml logs --since=7d | grep -i error

# Verify backups
ls -lht backups/ | head -10
```

---

## 🎯 Success Indicators

You know everything is working when:

1. ✅ All services show "Up (healthy)"
2. ✅ Can login to Streamlit at `https://YOUR_IP/`
3. ✅ Can login to Airflow at `https://YOUR_IP/airflow/`
4. ✅ Can access VNC at `https://YOUR_IP/vnc/`
5. ✅ Can't access from unauthorized IP
6. ✅ GitHub Actions deployments succeed
7. ✅ Logs show no errors
8. ✅ Backups are being created daily

---

## 📞 Quick Reference

### Local Machine
```bash
# Get your IP
curl ifconfig.me

# Test locally
./scripts/test-deployment.sh

# Deploy changes
git push origin main

# SSH to server
ssh root@YOUR_SERVER_IP
```

### On Server
```bash
# Go to app
cd /opt/final-automation

# View services
docker compose -f docker-compose.prod.yml ps

# View logs
docker compose -f docker-compose.prod.yml logs -f

# Restart
docker compose -f docker-compose.prod.yml restart

# Update
git pull && docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

### Access URLs
```
Streamlit: https://YOUR_IP/
Airflow:   https://YOUR_IP/airflow/
VNC:       https://YOUR_IP/vnc/
```

---

## ✅ Final Checklist

Before considering deployment complete:

- [ ] All services running and healthy
- [ ] Can access all 3 URLs (Streamlit, Airflow, VNC)
- [ ] Can login with credentials
- [ ] GitHub Actions deployment works
- [ ] Credentials saved in password manager
- [ ] `.production-credentials.txt` deleted
- [ ] IP whitelist configured correctly
- [ ] Tested from your IP (works)
- [ ] Tested from different IP (blocked)
- [ ] Backups directory created
- [ ] Server auto-starts on reboot (test: `sudo reboot`)

---

## 🎓 Next Steps

After successful deployment:

1. **Test Your Workflows**
   - Create test automation
   - Verify Chrome profiles work
   - Test VNC recording

2. **Set Up Monitoring**
   - Add email notifications
   - Set up uptime monitoring
   - Configure alerts

3. **Optional Improvements**
   - Get domain name
   - Set up Let's Encrypt SSL
   - Configure automated backups to cloud
   - Set up VPN access

4. **Regular Maintenance**
   - Update system monthly
   - Check logs weekly
   - Verify backups weekly
   - Renew GitHub token before expiry

---

**🎉 Congratulations! Your application is now live on Contabo!**
