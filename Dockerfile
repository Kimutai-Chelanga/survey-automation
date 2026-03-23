FROM nikolaik/python-nodejs:python3.10-nodejs24-slim

USER root
WORKDIR /app

# ------------------------------------------------------------------
# System dependencies — Chrome + VNC + X11 + xclip + Playwright deps
# ------------------------------------------------------------------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        # Basic utilities
        curl \
        gnupg \
        wget \
        ca-certificates \
        gcc \
        g++ \
        unzip \
        postgresql-client \
        netcat-openbsd \
        lsof \
        procps \
        xclip \
        # Image processing
        libjpeg-dev \
        libpng-dev \
        libfreetype6-dev \
        zlib1g-dev \
        libtiff-dev \
        libwebp-dev \
        libmagic1 \
        # X11 / VNC
        xvfb \
        x11vnc \
        fluxbox \
        websockify \
        novnc \
        xterm \
        x11-utils \
        dbus-x11 \
        # Chrome / Playwright shared dependencies
        fonts-liberation \
        libnss3 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libx11-6 \
        libxcomposite1 \
        libxdamage1 \
        libxrandr2 \
        libxcursor1 \
        libgbm-dev \
        libxrender1 \
        libxext6 \
        libxfixes3 \
        libxshmfence1 \
        libglu1-mesa \
        libxi6 \
        libxkbcommon0 \
        libxss1 \
        libxtst6 \
        libasound2 \
        libdbus-1-3 \
        libnspr4 \
        # Extra libs Playwright needs — Trixie (Debian 13) correct package names
        libwoff1 \
        libvpx9 \
        libevent-2.1-7t64 \
        libopus0 \
        libwebpdemux2 \
        libharfbuzz-icu0 \
        libhyphen0 \
        libmanette-0.2-0 \
        # Additional dependencies for browser-use
        libgtk-3-0 \
        libx11-xcb1 \
        libxcb1 \
        libxcb-shm0 \
        libxcb-xfixes0 \
        libxcb-shape0 \
        libxcb-randr0 \
        libxcb-icccm4 \
        libxcb-image0 \
        libxcb-keysyms1 \
        libxcb-util1 \
        libxcb-xinerama0 \
        libxcb-xkb1 \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------------
# Google Chrome Stable (used for user sessions via CDP)
# ------------------------------------------------------------------
RUN wget -q -O /tmp/chrome.deb \
        https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

RUN which google-chrome-stable && google-chrome-stable --version

# ------------------------------------------------------------------
# Directories & VNC password
# ------------------------------------------------------------------
RUN mkdir -p /app/chrome_profiles /app/recordings \
    && chmod 777 /app/chrome_profiles /app/recordings

RUN mkdir -p /root/.vnc \
    && x11vnc -storepasswd secret /root/.vnc/passwd

# ------------------------------------------------------------------
# Environment
# ------------------------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLE_CORS=false \
    STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true \
    CHROME_PROFILES_BASE_DIR=/app/chrome_profiles \
    CHROME_DEBUG_PORT_START=9222 \
    DISPLAY=:99 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0

# ------------------------------------------------------------------
# Python dependencies
# ------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ------------------------------------------------------------------
# Install Playwright's Chromium browser
# ------------------------------------------------------------------
RUN playwright install chromium \
    && playwright install-deps chromium

# ------------------------------------------------------------------
# Application code
# ------------------------------------------------------------------
COPY . .

RUN if [ -f /app/copy_cookies.sh ]; then chmod +x /app/copy_cookies.sh; fi

# ------------------------------------------------------------------
# Startup script
# ------------------------------------------------------------------
RUN cat > /app/start.sh << 'EOFSCRIPT'
#!/bin/bash
set -e

echo "=== Starting VNC + Streamlit Stack ==="

# Create directories
mkdir -p /app/chrome_profiles /app/recordings /tmp/.X11-unix /root/.vnc \
         /app/workflows_export /app/prompt_backups /app/cookie_scripts
chmod 777 /app/chrome_profiles /app/recordings /app/workflows_export \
          /app/prompt_backups /app/cookie_scripts
chmod 1777 /tmp/.X11-unix

# Clean up stale X locks
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 || true

# Start Xvfb
echo "Starting Xvfb..."
Xvfb :99 -screen 0 1920x1080x24 -ac +extension RANDR +extension GLX +render -noreset \
    > /tmp/xvfb.log 2>&1 &
sleep 3

export DISPLAY=:99
export XDG_RUNTIME_DIR=/tmp/runtime-root
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"

if xdpyinfo -display :99 >/dev/null 2>&1; then
    echo "✓ Xvfb started"
else
    echo "✗ Xvfb failed" && cat /tmp/xvfb.log && exit 1
fi

# D-Bus
if command -v dbus-launch >/dev/null 2>&1; then
    eval "$(dbus-launch --sh-syntax)"
fi

# Fluxbox
echo "Starting Fluxbox..."
fluxbox > /tmp/fluxbox.log 2>&1 &
sleep 2

# VNC password
if [ ! -f /root/.vnc/passwd ]; then
    x11vnc -storepasswd "${VNC_PASSWORD:-secret}" /root/.vnc/passwd
fi

# x11vnc
echo "Starting x11vnc..."
x11vnc -display :99 -forever -shared -rfbport 5900 \
       -rfbauth /root/.vnc/passwd -noxrecord -noxfixes -noxdamage -wait 5 \
       > /tmp/x11vnc.log 2>&1 &
sleep 2

if pgrep -x x11vnc > /dev/null; then
    echo "✓ x11vnc started"
else
    echo "✗ x11vnc failed" && cat /tmp/x11vnc.log && exit 1
fi

# websockify / noVNC
echo "Starting websockify..."
websockify --web /usr/share/novnc 6080 localhost:5900 \
    > /tmp/websockify.log 2>&1 &
sleep 3

if pgrep -f websockify > /dev/null; then
    echo "✓ websockify started"
else
    echo "✗ websockify failed" && cat /tmp/websockify.log && exit 1
fi

echo ""
echo "=== All Services Started ==="
echo "Streamlit : http://localhost:8501"
echo "noVNC     : http://localhost:6080/vnc.html"
echo "Password  : ${VNC_PASSWORD:-secret}"
echo ""
echo "Starting Streamlit..."
exec streamlit run src/streamlit/app.py \
    --server.address=0.0.0.0 \
    --server.port=8501
EOFSCRIPT

RUN chmod +x /app/start.sh

# ------------------------------------------------------------------
# Ports & health check
# ------------------------------------------------------------------
EXPOSE 8501 6080 5900

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["/bin/bash", "/app/start.sh"]