#!/bin/bash
set -e

# ============================================================
# 🌍 LOCALE, LANGUAGE & TIMEZONE (KENYA / WINDOWS-LIKE)
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
# ⚙️ CONFIGURATION (MATCH HP ELITEBOOK x360 1030 G2)
# ============================================================

DISPLAY_NUM=${DISPLAY_NUM:-99}
VNC_PORT=${VNC_PORT:-5900}
NOVNC_PORT=${NOVNC_PORT:-6080}
VNC_PASSWORD=${VNC_PASSWORD:-secret}

# Laptop-equivalent screen settings
SCREEN_WIDTH=1280
SCREEN_HEIGHT=720
SCREEN_DEPTH=24
SCREEN_DPI=96

CHROME_PROFILES_DIR=${CHROME_PROFILES_BASE_DIR:-/app/chrome_profiles}
RECORDINGS_DIR=${RECORDINGS_DIR:-/app/recordings}

CHROME_PROFILE_NAME=default
CHROME_USER_DATA_DIR="${CHROME_PROFILES_DIR}/${CHROME_PROFILE_NAME}"

CHROME_FLAGS="
  --user-data-dir=${CHROME_USER_DATA_DIR}
  --lang=en-KE,en-US,en
  --window-size=${SCREEN_WIDTH},${SCREEN_HEIGHT}
  --force-device-scale-factor=1
  --high-dpi-support=1
  --disable-blink-features=AutomationControlled
  --no-first-run
  --no-default-browser-check
  --disable-infobars
  --window-size=1280,720 
  --window-position=0,0 
  --disable-dev-shm-usage
  --no-sandbox
  --start-maximized
"

CHROME_START_URL="https://www.google.com"

# ============================================================
# 🖨️ PRINT HELPERS
# ============================================================

print_status()  { echo -e "${GREEN}✓${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
print_error()   { echo -e "${RED}✗${NC} $1"; }
print_info()    { echo -e "${BLUE}ℹ${NC} $1"; }

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   STREAMLIT + VNC + CHROME (HP ELITEBOOK MODE)            ║${NC}"
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
# [1/10] DIRECTORIES
# ============================================================

echo -e "${BLUE}[1/10]${NC} Setting up directories..."
mkdir -p \
  "$CHROME_PROFILES_DIR" \
  "$CHROME_USER_DATA_DIR" \
  "$RECORDINGS_DIR" \
  /tmp/.X11-unix \
  /root/.vnc

chmod 777 "$CHROME_PROFILES_DIR" "$CHROME_USER_DATA_DIR" "$RECORDINGS_DIR"
chmod 1777 /tmp/.X11-unix

print_status "Directories ready"
print_info "Chrome profile: $CHROME_USER_DATA_DIR"

# ============================================================
# [2/10] CLEAN OLD X LOCKS
# ============================================================

echo -e "${BLUE}[2/10]${NC} Cleaning old X locks..."
rm -f /tmp/.X${DISPLAY_NUM}-lock /tmp/.X11-unix/X${DISPLAY_NUM}
print_status "Old X locks removed"

# ============================================================
# [3/10] START XVFB (WINDOWS 10 MATCH)
# ============================================================

echo -e "${BLUE}[3/10]${NC} Starting Xvfb..."
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
sleep 3
export DISPLAY=:${DISPLAY_NUM}

if ! xdpyinfo >/dev/null 2>&1; then
    print_error "Xvfb failed to start"
    cat /tmp/xvfb.log
    exit 1
fi

print_status "Xvfb running (PID: $XVFB_PID)"

# ============================================================
# [4/10] XDG + DBUS
# ============================================================

echo -e "${BLUE}[4/10]${NC} Setting up XDG runtime..."
export XDG_RUNTIME_DIR=/tmp/runtime-root
mkdir -p "$XDG_RUNTIME_DIR"
chmod 700 "$XDG_RUNTIME_DIR"

if command -v dbus-launch >/dev/null 2>&1; then
    eval "$(dbus-launch --sh-syntax)"
    print_status "D-Bus started"
else
    print_warning "D-Bus not available"
fi

# ============================================================
# [5/10] FLUXBOX
# ============================================================

echo -e "${BLUE}[5/10]${NC} Starting Fluxbox..."
fluxbox > /tmp/fluxbox.log 2>&1 &
FLUXBOX_PID=$!
sleep 2
print_status "Fluxbox running (PID: $FLUXBOX_PID)"

# ============================================================
# [6/10] START CHROME (1:1 LAPTOP VIEW)
# ============================================================

echo -e "${BLUE}[6/10]${NC} Starting Google Chrome..."

CHROME_BIN=$(command -v google-chrome || command -v google-chrome-stable || true)

if [ -z "$CHROME_BIN" ]; then
    print_error "Google Chrome not found!"
    exit 1
fi

$CHROME_BIN $CHROME_FLAGS "$CHROME_START_URL" > /tmp/chrome.log 2>&1 &

CHROME_PID=$!
sleep 4

if pgrep -f "google-chrome" >/dev/null; then
    print_status "Chrome started (PID: $CHROME_PID)"
else
    print_warning "Chrome may not have started correctly"
fi

# ============================================================
# [7/10] VNC PASSWORD
# ============================================================

echo -e "${BLUE}[7/10]${NC} Setting VNC password..."
[ -f /root/.vnc/passwd ] || x11vnc -storepasswd "$VNC_PASSWORD" /root/.vnc/passwd
print_status "VNC password ready"

# ============================================================
# [8/10] X11VNC
# ============================================================

echo -e "${BLUE}[8/10]${NC} Starting x11vnc..."
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

# ============================================================
# [9/10] NOVNC
# ============================================================

echo -e "${BLUE}[9/10]${NC} Starting noVNC..."
websockify \
  --web /usr/share/novnc \
  ${NOVNC_PORT} \
  localhost:${VNC_PORT} \
  > /tmp/websockify.log 2>&1 &

WEBSOCKIFY_PID=$!
sleep 2
print_status "noVNC running (PID: $WEBSOCKIFY_PID)"

# ============================================================
# [10/10] SUMMARY
# ============================================================

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   HP ELITEBOOK x360 1030 G2 – 1:1 DISPLAY ACTIVE          ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "🖥️  Resolution: ${SCREEN_WIDTH}x${SCREEN_HEIGHT} @ ${SCREEN_DPI} DPI"
echo "🌍 Locale:     English (Kenya)"
echo "🕒 Timezone:   Africa/Nairobi"
echo "🌐 Chrome:     Visible, real window"
echo "🔐 VNC:        localhost:${VNC_PORT}"
echo "🌐 noVNC:      http://localhost:${NOVNC_PORT}/vnc.html"
echo "🚀 Streamlit:  http://localhost:8501"
echo ""

# ============================================================
# 🚀 START STREAMLIT (FOREGROUND)
# ============================================================

exec streamlit run src/streamlit/app.py \
  --server.address=0.0.0.0 \
  --server.port=8501 \
  --server.headless=true \
  --server.enableCORS=false \
  --server.enableXsrfProtection=false
