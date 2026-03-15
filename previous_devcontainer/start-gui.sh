#!/bin/bash
set -e

cleanup() {
  echo "Shutting down gracefully..."
  
  # Gracefully close Chrome first to ensure data is saved
  if pgrep -f "chrome" >/dev/null; then
    echo "Closing Chrome gracefully..."
    pkill -TERM -f "chrome" || true
    sleep 3
    # Force kill if still running
    pkill -KILL -f "chrome" || true
  fi
  
  pkill -f "Xvfb|fluxbox|x11vnc|websockify" || true
  exit 0
}
trap cleanup SIGINT SIGTERM

echo "Using persistent Chrome profile at /workspace/chrome_profile"

echo "Starting Xvfb..."
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99
Xvfb :99 -screen 0 1920x1080x24 -ac +extension RANDR +extension GLX +render -noreset \
  >/tmp/xvfb.log 2>&1 &
sleep 3
export DISPLAY=:99
xdpyinfo -display :99 >/dev/null || { echo "Xvfb failed"; cat /tmp/xvfb.log; exit 1; }

export XDG_RUNTIME_DIR=/tmp/runtime-root
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"
eval "$(dbus-launch --sh-syntax)"

fluxbox >/tmp/fluxbox.log 2>&1 &
sleep 2

x11vnc -display :99 -forever -shared -passwd secret -bg -o /tmp/x11vnc.log
websockify --web /usr/share/novnc 6080 localhost:5900 >/tmp/websockify.log 2>&1 &
sleep 3

# Persistent Chrome profile
CHROME_PROFILE_DIR="/workspace/chrome_profile"
mkdir -p "$CHROME_PROFILE_DIR"

# Clean up lock files but preserve all user data
rm -f "$CHROME_PROFILE_DIR/SingletonLock" "$CHROME_PROFILE_DIR/SingletonSocket" "$CHROME_PROFILE_DIR/SingletonCookie"

echo "Starting Chrome with enhanced persistence..."

# Start Chrome with additional flags for better data persistence
google-chrome-stable \
  --no-sandbox --disable-setuid-sandbox \
  --disable-gpu --disable-dev-shm-usage \
  --user-data-dir="$CHROME_PROFILE_DIR" \
  --remote-debugging-port=9222 --remote-debugging-address=0.0.0.0 \
  --remote-allow-origins=* \
  --disable-features=UseOzonePlatform,VizDisplayCompositor \
  --window-size=1920,1080 --start-maximized \
  --enable-logging --log-level=0 \
  --disable-background-timer-throttling \
  --disable-backgrounding-occluded-windows \
  --disable-renderer-backgrounding \
  --keep-alive-for-test \
  --disable-session-crashed-bubble \
  --disable-infobars \
  --no-first-run \
  --disable-default-apps \
  --disable-translate \
  >/tmp/chrome.log 2>&1 &

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