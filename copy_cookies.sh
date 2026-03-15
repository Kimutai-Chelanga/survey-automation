#!/bin/bash
# copy_cookies.sh - Copy cookies to clipboard for easy import

set -e

echo "==========================================="
echo "Cookie Clipboard Copy Utility"
echo "==========================================="
echo ""

# Set display for X11
export DISPLAY=:99

# Check if xclip is installed
if ! command -v xclip &> /dev/null; then
    echo "❌ Error: xclip is not installed"
    echo "Please rebuild the Docker container with xclip included"
    exit 1
fi

# Check if the cookie file exists
COOKIE_FILE="editthiscookie/queenhaley.json"

if [ ! -f "$COOKIE_FILE" ]; then
    echo "❌ Error: Cookie file not found at $COOKIE_FILE"
    echo ""
    echo "Available files in editthiscookie/:"
    ls -la editthiscookie/ 2>/dev/null || echo "Directory not found"
    exit 1
fi

# Copy to clipboard
echo "📋 Copying cookies to clipboard..."
cat "$COOKIE_FILE" | xclip -selection clipboard

if [ $? -eq 0 ]; then
    echo "✅ Success! Cookies copied to clipboard"
    echo ""
    echo "You can now paste (Ctrl+V) in your browser import field"
    echo ""
    echo "Cookie file: $COOKIE_FILE"
    echo "File size: $(wc -c < "$COOKIE_FILE") bytes"
    echo "==========================================="
else
    echo "❌ Failed to copy to clipboard"
    exit 1
fi