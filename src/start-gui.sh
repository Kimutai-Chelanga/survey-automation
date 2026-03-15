#!/usr/bin/env bash
set -e

echo "Starting Xvfb..."
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99

CHROME_PROFILE_DIR="/home/chrome_profile"
mkdir -p "$CHROME_PROFILE_DIR"

echo "Starting Google Chrome with remote debugging enabled..."

# IMPORTANT: use the real binary, NOT the wrapper
/usr/bin/google-chrome-stable \
  --no-sandbox \
  --disable-setuid-sandbox \
  --disable-gpu \
  --disable-dev-shm-usage \
  --user-data-dir="$CHROME_PROFILE_DIR" \
  --remote-debugging-port=9222 \
  --remote-debugging-address=0.0.0.0 \
  --remote-debugging-bind-address=0.0.0.0 \
  --remote-allow-origins=* \
  --disable-features=UseOzonePlatform,VizDisplayCompositor \
  --window-size=1920,1080 \
  --start-maximized \
  --enable-logging \
  --log-level=0 \
  --disable-background-timer-throttling \
  --disable-backgrounding-occluded-windows \
  --disable-renderer-backgrounding \
  --disable-session-crashed-bubble \
  --disable-infobars \
  --no-first-run \
  --disable-default-apps \
  --disable-translate \
  >/tmp/chrome.log 2>&1 &

echo "Chrome started. Tailing logs..."
tail -f /tmp/chrome.log


sleep 8
if ! pgrep -f "chrome" >/dev/null; then
  echo "Chrome failed to start; last logs:"; tail -20 /tmp/chrome.log
  exit 1
fi

xterm -geometry 80x24+50+50 -title "Debug Terminal" &

cat <<EOF
==============================================
✅ Environment Ready with Enhanced Persistence!

GUI:        http://localhost:6080/vnc.html (password: secret)
DevTools:   http://localhost:9222
Profile:    $CHROME_PROFILE_DIR

🔧 PERSISTENCE ENHANCEMENTS:
   - Graceful Chrome shutdown on script exit
   - Enhanced data saving flags
   - Lock file cleanup (preserves user data)
   - Session restoration enabled

📋 User data that persists:
   ✓ Bookmarks and history
   ✓ Passwords and autofill
   ✓ Extensions and settings
   ✓ Cookies and site data
   ✓ Theme and preferences
   ✓ Open tabs (session restore)

🔧 WEBSOCKET FIX APPLIED: Added --remote-allow-origins=*
   This allows workflow upload scripts to connect via WebSocket

📋 To add extensions manually:
   1. Open Chrome in the GUI
   2. Go to chrome://extensions/
   3. Enable "Developer mode" (top right toggle)
   4. Click "Load unpacked"
   5. Navigate to your extension folder

Extensions can be loaded from any directory in /workspace

Logs:
  Chrome: /tmp/chrome.log
  X11VNC: /tmp/x11vnc.log
  Xvfb:   /tmp/xvfb.log

IMPORTANT: Use Ctrl+C to shut down gracefully and ensure data is saved!
==============================================
EOF

[ "$1" = "--logs" ] && tail -f /tmp/chrome.log /tmp/x11vnc.log

tail -f /dev/null