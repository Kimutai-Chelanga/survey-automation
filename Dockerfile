# -------------------------------
# Base image with Python + Node
# -------------------------------
    FROM nikolaik/python-nodejs:python3.11-nodejs24-slim

    WORKDIR /app
    
    # -------------------------------
    # System dependencies (including all required for Chrome & Playwright)
    # -------------------------------
    RUN apt-get update && apt-get install -y --no-install-recommends \
        curl wget gnupg unzip lsof procps xclip \
        libjpeg-dev libpng-dev libfreetype6-dev zlib1g-dev libtiff-dev libwebp-dev libmagic1 \
        xvfb x11vnc fluxbox websockify novnc xterm x11-utils dbus-x11 \
        fonts-liberation libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libx11-6 \
        libxcomposite1 libxdamage1 libxrandr2 libxcursor1 libgbm-dev libxrender1 \
        libxext6 libxfixes3 libxshmfence1 libglu1-mesa libxi6 libxkbcommon0 libxss1 \
        libxtst6 libasound2 libdbus-1-3 libnspr4 libgtk-3-0 \
        # Extra libs to prevent silent Chromium crashes in containers
        libx11-xcb1 libdrm2 libxdamage1 libxrandr2 libgbm1 libasound2 libatk-bridge2.0-0 \
        && rm -rf /var/lib/apt/lists/*
    
    # -------------------------------
    # Install Google Chrome (stable)
    # -------------------------------
    RUN wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
        && apt-get update \
        && apt-get install -y --no-install-recommends /tmp/chrome.deb \
        && rm /tmp/chrome.deb \
        && rm -rf /var/lib/apt/lists/*
    
    RUN which google-chrome-stable && google-chrome-stable --version
    
    # -------------------------------
    # Python dependencies
    # -------------------------------
    COPY requirements.txt .
    RUN pip install --no-cache-dir --upgrade pip \
        && pip install --no-cache-dir -r requirements.txt \
        && pip install --no-cache-dir debugpy
    
    # ✅ CRITICAL: Disable Playwright browser download – we use CDP Chrome only
    ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
    
    # -------------------------------
    # App code
    # -------------------------------
    COPY . .
    
    # -------------------------------
    # Create required directories
    # -------------------------------
    RUN mkdir -p /app/chrome_profiles /app/recordings /root/.vnc \
        && chmod 777 /app/chrome_profiles /app/recordings
    
    # -------------------------------
    # VNC password
    # -------------------------------
    RUN x11vnc -storepasswd secret /root/.vnc/passwd
    
    # Make start script executable (ensure it exists)
    RUN chmod +x /app/start.sh
    
    # -------------------------------
    # Environment variables
    # -------------------------------
    ENV PYTHONUNBUFFERED=1 \
        PYTHONPATH=/app \
        DISPLAY=:99 \
        STREAMLIT_SERVER_HEADLESS=true \
        STREAMLIT_SERVER_ENABLE_CORS=false \
        CHROME_PROFILES_BASE_DIR=/app/chrome_profiles \
        DEBUGPY_LISTEN_PORT=5678 \
        DEBUGPY_WAIT_FOR_CLIENT=false
    
    # -------------------------------
    # Expose ports
    # -------------------------------
    EXPOSE 8501 6080 5900 5678
    
    CMD ["/app/start.sh"]