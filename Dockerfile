# -----------------------------------------------------------------------
# Base image — Python 3.11 + Node.js 24 (slim)
# -----------------------------------------------------------------------
FROM nikolaik/python-nodejs:python3.11-nodejs24-slim

# -----------------------------------------------------------------------
# Working directory
# -----------------------------------------------------------------------
WORKDIR /app

# -----------------------------------------------------------------------
# System dependencies
#
# KEY ADDITIONS vs previous version:
#   firefox-esr       — required by Camoufox (C++-patched Firefox fork)
#   libavcodec-extra  — WebRTC/media fingerprint support for Camoufox
#   libglib2.0-0      — Firefox runtime dependency
#   libdbus-glib-1-2  — Firefox D-Bus support
#   git               — needed by some pip installs (camoufox, nodriver)
# -----------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Basic tools
    curl wget gnupg unzip lsof procps xclip git \
    # Image processing
    libjpeg-dev libpng-dev libfreetype6-dev zlib1g-dev libtiff-dev \
    libwebp-dev libmagic1 \
    # X11 / VNC
    xvfb x11vnc fluxbox websockify novnc xterm x11-utils dbus-x11 \
    # Chrome / Chromium runtime dependencies
    fonts-liberation libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libx11-6 libxcomposite1 libxdamage1 libxrandr2 libxcursor1 \
    libgbm-dev libxrender1 libxext6 libxfixes3 libxshmfence1 \
    libglu1-mesa libxi6 libxkbcommon0 libxss1 libxtst6 libasound2 \
    libdbus-1-3 libnspr4 libgtk-3-0 \
    # Firefox / Camoufox runtime dependencies (NEW)
    firefox-esr \
    libdbus-glib-1-2 \
    libglib2.0-0 \
    libxt6 \
    # Fonts for realistic fingerprinting (Camoufox BrowserForge)
    fonts-noto \
    fonts-noto-cjk \
    ttf-mscorefonts-installer \
    fontconfig \
    && fc-cache -fv \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------
# Google Chrome stable
# (Still needed for nodriver + Playwright CDP connect_over_cdp)
# -----------------------------------------------------------------------
RUN wget -q -O /tmp/chrome.deb \
        https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

RUN which google-chrome-stable && google-chrome-stable --version

# -----------------------------------------------------------------------
# Python dependencies
# -----------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------
# Playwright — install Chromium for connect_over_cdp support
# (Playwright itself is no longer used for stealth browsing;
#  Camoufox and nodriver handle that. Playwright is only used to
#  connect to the already-running Chrome via CDP.)
# -----------------------------------------------------------------------
RUN playwright install chromium && playwright install-deps chromium

# -----------------------------------------------------------------------
# Camoufox — download the patched Firefox binary
#
# This runs python -m camoufox fetch which downloads ~700 MB of the
# C++-patched Firefox binary. It is cached in ~/.local/share/camoufox
# so rebuilds after this layer are fast.
#
# IMPORTANT: The binary is architecture-specific. This Dockerfile
# targets linux/amd64. For ARM (e.g. Apple Silicon dev machines)
# you would need linux/arm64 — but production servers are amd64.
# -----------------------------------------------------------------------
RUN python -m camoufox fetch

# Verify Camoufox installed correctly
RUN python -c "from camoufox.sync_api import Camoufox; print('Camoufox OK')"

# -----------------------------------------------------------------------
# App code
# -----------------------------------------------------------------------
COPY . .

# -----------------------------------------------------------------------
# Runtime directories
# -----------------------------------------------------------------------
RUN mkdir -p \
        /app/chrome_profiles \
        /app/camoufox_profiles \
        /app/recordings \
        /app/screenshots \
        /root/.vnc \
        /workspace/chrome_profiles \
        /workspace/camoufox_profiles \
        /workspace/downloads \
    && chmod 777 \
        /app/chrome_profiles \
        /app/camoufox_profiles \
        /app/recordings \
        /app/screenshots \
        /workspace/chrome_profiles \
        /workspace/camoufox_profiles \
        /workspace/downloads

# -----------------------------------------------------------------------
# VNC password
# -----------------------------------------------------------------------
RUN x11vnc -storepasswd secret /root/.vnc/passwd

# -----------------------------------------------------------------------
# Environment variables
# -----------------------------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DISPLAY=:99 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLE_CORS=false \
    # Chrome profiles (used by ChromeSessionManager + nodriver)
    CHROME_PROFILES_BASE_DIR=/workspace/chrome_profiles \
    CHROME_PROFILE_DIR=/workspace/chrome_profiles \
    CHROME_USER_DATA_DIR=/workspace/chrome_profiles \
    CHROME_DOWNLOAD_DIR=/workspace/downloads \
    CHROME_EXECUTABLE=/usr/bin/google-chrome-stable \
    CHROME_DEBUG_PORT_START=9222 \
    # Camoufox profiles (NEW — separate from Chrome profiles)
    CAMOUFOX_PROFILES_BASE_DIR=/workspace/camoufox_profiles \
    # Playwright — point to system Chrome to avoid downloading a second one
    PLAYWRIGHT_BROWSERS_PATH=/usr/bin \
    # Workspace paths
    RECORDINGS_DIR=/workspace/recordings \
    DOWNLOADS_DIR=/workspace/downloads \
    WORKSPACE_DIR=/workspace \
    SCREENSHOTS_DIR=/app/screenshots \
    # VNC / display
    VNC_PASSWORD=secret \
    VNC_PORT=5900 \
    NOVNC_PORT=6080

# -----------------------------------------------------------------------
# Expose ports
#   8501 — Streamlit UI
#   6080 — noVNC browser UI
#   5900 — raw VNC
#   9222-9322 — Chrome CDP debug ports (one per concurrent session)
# -----------------------------------------------------------------------
EXPOSE 8501 6080 5900

# -----------------------------------------------------------------------
# Startup
#
# Launch order:
#   1. Xvfb        — virtual display (required for headed Chrome + Camoufox)
#   2. fluxbox     — minimal window manager (Camoufox needs a WM)
#   3. x11vnc      — VNC server so you can watch the browser
#   4. websockify  — noVNC WebSocket bridge
#   5. streamlit   — the application
#
# Camoufox and Chrome are launched on demand by the application;
# they are NOT started here.
# -----------------------------------------------------------------------
CMD bash -c "\
    Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset & \
    sleep 1 && fluxbox & \
    x11vnc -display :99 -forever -shared -rfbport 5900 \
            -rfbauth /root/.vnc/passwd -noxdamage & \
    websockify --web /usr/share/novnc 6080 localhost:5900 & \
    streamlit run src/streamlit/app.py \
        --server.address=0.0.0.0 \
        --server.port=8501 \
"
