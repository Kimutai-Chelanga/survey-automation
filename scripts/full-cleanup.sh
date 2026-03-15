#!/bin/bash
# File: scripts/full-cleanup.sh
# Nuclear option - complete cleanup

set -e

RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m'

# Get script and project directories
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Change to project root
cd "$PROJECT_ROOT"

# Detect docker-compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}ERROR: Neither 'docker-compose' nor 'docker compose' found!${NC}"
    exit 1
fi

echo -e "${RED}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${RED}║   FULL CLEANUP - THIS WILL DELETE EVERYTHING!         ║${NC}"
echo -e "${RED}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}This will:${NC}"
echo "  1. Stop all containers"
echo "  2. Remove all containers"
echo "  3. Remove all volumes (ALL DATA LOST)"
echo "  4. Remove all networks"
echo "  5. Remove all orphaned volumes"
echo "  6. Clear Docker build cache (optional)"
echo ""

read -p "Are you SURE? Type 'DELETE EVERYTHING' to confirm: " confirm

if [ "$confirm" != "DELETE EVERYTHING" ]; then
    echo "Cleanup cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}Starting full cleanup...${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Using command: $DOCKER_COMPOSE"

# Stop everything
echo "Stopping all containers..."
$DOCKER_COMPOSE down -v --remove-orphans 2>/dev/null || true

# Force remove containers
echo "Force removing any remaining containers..."
docker ps -aq --filter "name=final-automation" | xargs -r docker rm -f 2>/dev/null || true

# Remove volumes
echo "Removing all project volumes..."
docker volume ls -q | grep final-automation | xargs -r docker volume rm 2>/dev/null || true

# Prune dangling volumes
echo "Removing dangling volumes..."
docker volume prune -f

# Prune networks
echo "Removing unused networks..."
docker network prune -f

# Ask about build cache
read -p "Remove Docker build cache? (saves disk space but rebuilds will be slower) (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker builder prune -f
fi

echo ""
echo -e "${GREEN}✓ Full cleanup complete!${NC}"
echo ""
echo "System is now clean. Run '$SCRIPT_DIR/startup.sh' to start fresh."
