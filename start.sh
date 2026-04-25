#!/bin/bash
set -e

# -------------------------------------------------------
# Start Xvfb virtual display
# -------------------------------------------------------
Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &

# -------------------------------------------------------
# Start Fluxbox window manager
# -------------------------------------------------------
fluxbox &

# -------------------------------------------------------
# Start x11vnc VNC server
# -------------------------------------------------------
x11vnc -display :99 -forever -shared -rfbport 5900 -rfbauth /root/.vnc/passwd &

# -------------------------------------------------------
# Start noVNC websocket proxy
# -------------------------------------------------------
websockify --web /usr/share/novnc 6080 localhost:5900 &

# -------------------------------------------------------
# Start Streamlit — with or without debugpy wait-for-client
# Set DEBUGPY_WAIT_FOR_CLIENT=true in .env to block on attach
# -------------------------------------------------------
if [ "${DEBUGPY_WAIT_FOR_CLIENT:-false}" = "true" ]; then
    echo "[start.sh] debugpy: waiting for client to attach on port ${DEBUGPY_LISTEN_PORT:-5678}..."
    python -m debugpy \
        --listen 0.0.0.0:${DEBUGPY_LISTEN_PORT:-5678} \
        --wait-for-client \
        -m streamlit run src/streamlit/app.py \
        --server.address=0.0.0.0 \
        --server.port=8501
else
    echo "[start.sh] debugpy: listening on port ${DEBUGPY_LISTEN_PORT:-5678} (attach any time)"
    python -m debugpy \
        --listen 0.0.0.0:${DEBUGPY_LISTEN_PORT:-5678} \
        -m streamlit run src/streamlit/app.py \
        --server.address=0.0.0.0 \
        --server.port=8501
fi