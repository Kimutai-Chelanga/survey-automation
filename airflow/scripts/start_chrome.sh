#!/bin/bash
set -e

echo "Starting Chrome with remote debugging..."

# Profile directory
PROFILE_DIR="/opt/airflow/chrome_persistent_profile"

# Ensure profile directory exists
mkdir -p "$PROFILE_DIR"
chmod 777 "$PROFILE_DIR"

# Kill any existing Chrome processes
pkill -9 chrome || true
pkill -9 google-chrome || true
sleep 2

# Start Xvfb if not running
if ! pgrep -x Xvfb > /dev/null; then
    echo "Starting Xvfb..."
    Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
    sleep 2
fi

export DISPLAY=:99

# Start Chrome with remote debugging
echo "Launching Chrome..."
google-chrome-stable \
    --remote-debugging-port=9222 \
    --no-first-run \
    --no-default-browser-check \
    --user-data-dir="$PROFILE_DIR" \
    --disable-blink-features=AutomationControlled \
    --disable-dev-shm-usage \
    --disable-gpu \
    --no-sandbox \
    --disable-setuid-sandbox \
    --disable-software-rasterizer \
    --headless=new \
    --window-size=1920,1080 \
    --disable-features=IsolateOrigins,site-per-process \
    --disable-web-security \
    --disable-features=VizDisplayCompositor \
    > /opt/airflow/logs/chrome.log 2>&1 &

CHROME_PID=$!
echo "Chrome started with PID: $CHROME_PID"

# Wait for Chrome to be ready
echo "Waiting for Chrome to start..."
for i in {1..30}; do
    if curl -s http://localhost:9222/json/version > /dev/null 2>&1; then
        echo "Chrome is ready!"
        exit 0
    fi
    echo "Attempt $i/30: Chrome not ready yet..."
    sleep 2
done

echo "ERROR: Chrome failed to start within timeout"
exit 1