FROM node:20-bullseye

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates fonts-liberation libappindicator3-1 libasound2 \
    libatk-bridge2.0-0 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 \
    libexpat1 libfontconfig1 libgbm1 libgcc1 libglib2.0-0 libgtk-3-0 \
    libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 \
    libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 \
    libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 \
    libxss1 libxtst6 lsb-release wget xdg-utils xvfb fluxbox \
    x11vnc novnc websockify dbus-x11 x11-apps xterm net-tools \
    git build-essential curl \
    # Add Python3 and pip here
    python3 python3-pip \
    # Ansible dependencies
    openssh-client sshpass rsync && \
    rm -rf /var/lib/apt/lists/*

# Install Chrome
RUN wget -O /tmp/chrome.deb \
    https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y /tmp/chrome.deb && rm /tmp/chrome.deb

# Install pnpm
RUN npm install -g pnpm

# Install Ansible via pip
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install ansible ansible-core

# Make GUI start script executable
COPY start-gui.sh /usr/local/bin/start-gui.sh
RUN chmod +x /usr/local/bin/start-gui.sh

# Create workspace directory structure (will be available for post-create commands)
RUN mkdir -p /workspace && chmod 777 /workspace

# Pre-create chrome profile directory with correct permissions
RUN mkdir -p /workspace/chrome_profile && \
    chmod -R 777 /workspace/chrome_profile && \
    chown -R root:root /workspace/chrome_profile

# Set workspace as working directory
WORKDIR /workspace

EXPOSE 6080 9222
CMD ["/usr/local/bin/start-gui.sh"]