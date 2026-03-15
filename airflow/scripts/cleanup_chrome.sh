#!/bin/bash

echo "Cleaning up Chrome processes..."

# Kill Chrome processes gracefully first
pkill chrome || true
pkill google-chrome || true

# Wait for graceful shutdown
sleep 2

# Force kill if still running
pkill -9 chrome || true
pkill -9 google-chrome || true

# Wait a moment
sleep 1

# Verify Chrome is stopped
if pgrep -x chrome > /dev/null || pgrep -x google-chrome > /dev/null; then
    echo "WARNING: Some Chrome processes may still be running"
    # List remaining processes for debugging
    ps aux | grep -i chrome | grep -v grep
    exit 1
else
    echo "✅ Chrome cleanup completed successfully"
fi

exit 0