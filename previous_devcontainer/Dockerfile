FROM nikolaik/python-nodejs:python3.11-nodejs24-slim

WORKDIR /app

# Install system dependencies (including X11 and GUI tools if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget gnupg \
    ca-certificates fonts-liberation libappindicator3-1 libasound2 \
    libatk-bridge2.0-0 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 \
    libexpat1 libfontconfig1 libgbm1 libgcc1 libglib2.0-0 libgtk-3-0 \
    libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 \
    libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 \
    libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 \
    libxss1 libxtst6 lsb-release wget xdg-utils xvfb fluxbox \
    x11vnc novnc websockify dbus-x11 x11-apps xterm net-tools \
    git build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome (needed for some headless browsers)
RUN wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb

# Verify Chrome installation
RUN which google-chrome-stable && google-chrome-stable --version

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (required for browser-use)
RUN playwright install

# Install pnpm
RUN npm install -g pnpm

# Install Ansible via pip (if needed)
RUN pip install ansible ansible-core

# Make GUI start script executable (if you have one)
COPY start-gui.sh /usr/local/bin/start-gui.sh
RUN chmod +x /usr/local/bin/start-gui.sh

# Create workspace directory structure
RUN mkdir -p /workspace && chmod 777 /workspace

# Pre-create chrome profile directory with correct permissions
RUN mkdir -p /workspace/chrome_profile && \
    chmod -R 777 /workspace/chrome_profile && \
    chown -R root:root /workspace/chrome_profile

# Set workspace as working directory
WORKDIR /workspace

EXPOSE 6080 9222

# If you have a GUI start script, use it; otherwise start a shell or streamlit directly
CMD ["/usr/local/bin/start-gui.sh"]