#!/bin/bash
# File: scripts/restart.sh
# Quick restart script

set -e

GREEN='\033[0;32m'
NC='\033[0m'

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo -e "${GREEN}Performing graceful restart...${NC}"

"$SCRIPT_DIR/shutdown.sh"

echo ""
echo "Waiting 5 seconds before restart..."
sleep 5

"$SCRIPT_DIR/startup.sh"
