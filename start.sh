#!/bin/bash
# FIXED: Removed Chrome from startup — Streamlit launches it per-session
# FIXED: fbsetbg popup suppressed via xsetroot + fluxbox init rootCommand
set -e

# ============================================================
# 🌍 LOCALE, LANGUAGE & TIMEZONE
# ============================================================

export LANG=en_KE.UTF-8
export LANGUAGE=en_KE:en_US:en
export LC_ALL=en_KE.UTF-8
export TZ=Africa/Nairobi
ln -snf /usr/share/zoneinfo/Africa/Nairobi /etc/localtime 2>/dev/null || true
echo "Africa/Nairobi" > /etc/timezone 2>/dev/null || true

# ============================================================
# 🎨 COLOR CODES
# ============================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ============================================================
# ⚙️ CONFIGURATION
# ============================================================

DISPLAY_NUM=${DISPLAY_NUM:-99}
VNC_PORT=${VNC_PORT:-5900}
NOVNC_PORT=${NOVNC_PORT:-6080}
VNC_PASSWORD=${VNC_PASSWORD:-secret}

SCREEN_WIDTH=1280
SCREEN_HEIGHT=720
SCREEN_DEPTH=24
SCREEN_DPI=96

CHROME_PROFILES_DIR=${CHROME_PROFILES_BASE_DIR:-/workspace/chrome_profiles}
RECORDINGS_DIR=${RECORDINGS_DIR:-/workspace/recordings}

# ============================================================
# 🖨️ PRINT HELPERS
# ============================================================

print_status()  { echo -e "${GREEN}✓${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
print_error()   { echo -e "${RED}✗${NC} $1"; }
print_info()    { echo -e "${BLUE}ℹ${NC} $1"; }

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   STREAMLIT + VNC + CHROME SESSION MANAGER                ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# ============================================================
# 🧹 CLEANUP HANDLER
# ============================================================

cleanup() {
    echo ""
    print_warning "Received shutdown signal..."
    pkill -TERM -f google-chrome 2>/dev/null || true
    pkill -TERM x11vnc 2>/dev/null || true
    pkill -TERM websockify 2>/dev/null || true
    pkill -TERM fluxbox 2>/dev/null || true
    pkill -TERM Xvfb 2>/dev/null || true
    print_status "Cleanup completed"
    exit 0
}

trap cleanup SIGTERM SIGINT SIGQUIT

# ============================================================
# [1/7] DIRECTORIES
# ============================================================

echo -e "${BLUE}[1/7]${NC} Setting up directories..."
mkdir -p \
  "$CHROME_PROFILES_DIR" \
  "$RECORDINGS_DIR" \
  /tmp/.X11-unix \
  /root/.vnc \
  /app/cookie_scripts \
  /workspace/chrome_profiles \
  /workspace/downloads

chmod 777 "$CHROME_PROFILES_DIR" "$RECORDINGS_DIR" /app/cookie_scripts 2>/dev/null || true
chmod 1777 /tmp/.X11-unix

print_status "Directories ready"

# ============================================================
# [2/7] CLEAN OLD X LOCKS
# ============================================================

echo -e "${BLUE}[2/7]${NC} Cleaning old X locks..."
rm -f /tmp/.X${DISPLAY_NUM}-lock /tmp/.X11-unix/X${DISPLAY_NUM}
print_status "Old X locks removed"

# ============================================================
# [3/7] START XVFB — wait until ready
# ============================================================

echo -e "${BLUE}[3/7]${NC} Starting Xvfb..."
Xvfb :${DISPLAY_NUM} \
  -screen 0 ${SCREEN_WIDTH}x${SCREEN_HEIGHT}x${SCREEN_DEPTH} \
  -dpi ${SCREEN_DPI} \
  -ac \
  +extension RANDR \
  +extension GLX \
  +render \
  -noreset \
  > /tmp/xvfb.log 2>&1 &

XVFB_PID=$!
export DISPLAY=:${DISPLAY_NUM}

# Poll until display is actually up (up to 15s)
for i in $(seq 1 15); do
    if xdpyinfo >/dev/null 2>&1; then
        print_status "Xvfb ready after ${i}s (PID: $XVFB_PID)"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        print_error "Xvfb failed to start within 15s"
        cat /tmp/xvfb.log
        exit 1
    fi
done

# ============================================================
# [4/7] XDG + DBUS
# ============================================================

echo -e "${BLUE}[4/7]${NC} Setting up XDG / D-Bus..."
export XDG_RUNTIME_DIR=/tmp/runtime-root
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"

if command -v dbus-launch >/dev/null 2>&1; then
    eval "$(dbus-launch --sh-syntax)"
    print_status "D-Bus started"
else
    print_warning "D-Bus not available — skipping"
fi

# ============================================================
# [5/7] SOLID BACKGROUND + FLUXBOX
#        xsetroot BEFORE fluxbox so it never calls fbsetbg
#        The rootCommand in ~/.fluxbox/init also sets it on
#        every subsequent startup, fully suppressing the popup.
# ============================================================

echo -e "${BLUE}[5/7]${NC} Setting desktop background + starting Fluxbox..."

# Set solid colour now so the display is never black/blank
xsetroot -solid "#1a1a2e" -display :${DISPLAY_NUM} 2>/dev/null || true

# Write a minimal fluxbox config that:
#   - uses rootCommand instead of fbsetbg (no popup)
#   - hides the toolbar (cleaner VNC view)
mkdir -p /root/.fluxbox
cat > /root/.fluxbox/init << 'FLUXBOX_INIT'
session.screen0.rootCommand:	xsetroot -solid "#1a1a2e"
session.menuFile:	~/.fluxbox/menu
session.keyFile:	~/.fluxbox/keys
session.screen0.toolbar.visible:	false
session.screen0.slit.autoHide:	false
FLUXBOX_INIT

fluxbox > /tmp/fluxbox.log 2>&1 &
FLUXBOX_PID=$!
sleep 2
print_status "Fluxbox running (PID: $FLUXBOX_PID) — fbsetbg popup suppressed"

# ============================================================
# [6/7] VNC PASSWORD + X11VNC + NOVNC
# ============================================================

echo -e "${BLUE}[6/7]${NC} Starting VNC services..."

[ -f /root/.vnc/passwd ] || x11vnc -storepasswd "$VNC_PASSWORD" /root/.vnc/passwd
print_status "VNC password ready"

x11vnc \
  -display :${DISPLAY_NUM} \
  -forever \
  -shared \
  -rfbport ${VNC_PORT} \
  -rfbauth /root/.vnc/passwd \
  -noxrecord \
  -noxfixes \
  -noxdamage \
  > /tmp/x11vnc.log 2>&1 &
X11VNC_PID=$!
sleep 2
print_status "x11vnc running (PID: $X11VNC_PID)"

websockify \
  --web /usr/share/novnc \
  ${NOVNC_PORT} \
  localhost:${VNC_PORT} \
  > /tmp/websockify.log 2>&1 &
WEBSOCKIFY_PID=$!
sleep 1
print_status "noVNC running (PID: $WEBSOCKIFY_PID) → http://localhost:${NOVNC_PORT}/vnc.html"

# ============================================================
# [7/7] SUMMARY
# ============================================================

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   ALL SERVICES READY                                       ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "🖥️  Resolution : ${SCREEN_WIDTH}x${SCREEN_HEIGHT} @ ${SCREEN_DPI} DPI"
echo "🌍 Locale      : English (Kenya)"
echo "🕒 Timezone    : Africa/Nairobi"
echo ""
echo "⚠️  Chrome      : NOT started at boot"
echo "   Chrome is launched ON DEMAND by Streamlit when you click"
echo "   'Start Session' in Accounts → Local Chrome tab."
echo "   It will open YOUR chosen URL in this VNC window."
echo ""
echo "🔐 VNC         : localhost:${VNC_PORT}  (password: ${VNC_PASSWORD})"
echo "🌐 noVNC       : http://localhost:${NOVNC_PORT}/vnc.html"
echo "🚀 Streamlit   : http://localhost:8501"
echo ""

# ============================================================
# 🚀 START STREAMLIT (foreground — keeps the container alive)
# ============================================================

exec streamlit run src/streamlit/app.py \
  --server.address=0.0.0.0 \
  --server.port=8501 \
  --server.headless=true \
  --server.enableCORS=false \
  --server.enableXsrfProtection=false