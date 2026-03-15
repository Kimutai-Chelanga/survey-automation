#!/bin/bash
# File: scripts/startup.sh
# Graceful startup script for Docker Compose

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Get script and project directories
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Change to project root
cd "$PROJECT_ROOT"

COMPOSE_FILE="docker-compose.yml"

# Detect docker-compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}ERROR: Neither 'docker-compose' nor 'docker compose' found!${NC}"
    exit 1
fi

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Docker Compose Startup Script                       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

print_status() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date +'%H:%M:%S')] ERROR:${NC} $1"
}

# Check if docker-compose.yml exists
if [ ! -f "$COMPOSE_FILE" ]; then
    print_error "docker-compose.yml not found in $PROJECT_ROOT"
    exit 1
fi

# Check if .env exists
if [ ! -f .env ]; then
    print_error ".env file not found!"
    exit 1
fi

# Check for previous containers
print_status "Project root: $PROJECT_ROOT"
print_status "Using command: $DOCKER_COMPOSE"
print_status "Checking for previous containers..."

if [ "$($DOCKER_COMPOSE -f "$COMPOSE_FILE" ps -q | wc -l)" -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Previous containers found.${NC}"
    read -p "Do you want to clean up first? (recommended) (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        "$SCRIPT_DIR/shutdown.sh"
        echo ""
        print_status "Cleanup complete. Starting fresh..."
        sleep 3
    fi
fi

# Start containers
print_status "Starting Docker Compose services..."
$DOCKER_COMPOSE -f "$COMPOSE_FILE" up -d

echo ""
print_status "Waiting for services to be ready..."
sleep 10

# Show status
echo ""
$DOCKER_COMPOSE -f "$COMPOSE_FILE" ps

echo ""
print_status "✓ Startup complete!"
echo ""
echo -e "${BLUE}Services available at:${NC}"
echo "  • Streamlit:  http://localhost:8501"
echo "  • Airflow:    http://localhost:8080"
echo "  • noVNC:      http://localhost:6080/vnc.html"
echo ""
print_status "To view logs: $DOCKER_COMPOSE logs -f"
print_status "To shutdown:  $SCRIPT_DIR/shutdown.sh"
