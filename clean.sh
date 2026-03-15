#!/bin/bash

###############################################################################
# Docker Compose Complete Cleanup Script (Improved)
# This script removes all containers, volumes, and associated data
# WARNING: This will permanently delete all data!
###############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project name (change this if your compose file uses a different project name)
PROJECT_NAME="final-automation"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Docker Compose Complete Cleanup Script (v2)           ║${NC}"
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo ""

# Warning prompt
echo -e "${RED}⚠️  WARNING: This will permanently delete ALL data including:${NC}"
echo -e "${RED}   • All containers${NC}"
echo -e "${RED}   • All volumes (databases, recordings, downloads, etc.)${NC}"
echo -e "${RED}   • All networks${NC}"
echo -e "${RED}   • Volume data from disk${NC}"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [[ $confirm != "yes" ]]; then
    echo -e "${YELLOW}Cleanup cancelled.${NC}"
    exit 0
fi

echo ""
echo -e "${YELLOW}Starting cleanup process...${NC}"
echo ""

###############################################################################
# Step 1: Find and navigate to docker-compose.yml directory
###############################################################################
echo -e "${BLUE}[1/7] Finding docker-compose.yml...${NC}"
if [ -f "docker-compose.yml" ]; then
    echo -e "${GREEN}✓ Found in current directory${NC}"
    COMPOSE_DIR=$(pwd)
elif [ -f "../docker-compose.yml" ]; then
    echo -e "${GREEN}✓ Found in parent directory${NC}"
    COMPOSE_DIR=$(cd .. && pwd)
    cd "$COMPOSE_DIR"
else
    echo -e "${YELLOW}⚠ docker-compose.yml not found, will use docker commands directly${NC}"
    COMPOSE_DIR=""
fi
echo ""

###############################################################################
# Step 2: Stop and remove containers
###############################################################################
echo -e "${BLUE}[2/7] Stopping and removing containers...${NC}"
if [ -n "$COMPOSE_DIR" ]; then
    cd "$COMPOSE_DIR"
    docker-compose down -v 2>/dev/null || true
    echo -e "${GREEN}✓ Containers stopped via docker-compose${NC}"
else
    # Stop all project containers manually
    project_containers=$(docker ps -aq --filter "name=${PROJECT_NAME}")
    if [ -n "$project_containers" ]; then
        docker stop $project_containers 2>/dev/null || true
        docker rm -f $project_containers 2>/dev/null || true
        echo -e "${GREEN}✓ Containers stopped manually${NC}"
    else
        echo -e "${GREEN}✓ No containers to stop${NC}"
    fi
fi
echo ""

###############################################################################
# Step 3: Remove all project containers (if any remain)
###############################################################################
echo -e "${BLUE}[3/7] Removing any remaining project containers...${NC}"
remaining_containers=$(docker ps -aq --filter "name=${PROJECT_NAME}")
if [ -n "$remaining_containers" ]; then
    docker rm -f $remaining_containers 2>/dev/null || true
    echo -e "${GREEN}✓ Removed remaining containers${NC}"
else
    echo -e "${GREEN}✓ No remaining containers found${NC}"
fi
echo ""

###############################################################################
# Step 4: List all project volumes BEFORE removal
###############################################################################
echo -e "${BLUE}[4/7] Identifying project volumes...${NC}"
project_volumes=$(docker volume ls --filter "name=${PROJECT_NAME}" -q)
if [ -n "$project_volumes" ]; then
    echo -e "${YELLOW}Found volumes to remove:${NC}"
    echo "$project_volumes" | while read vol; do
        echo -e "  • $vol"
    done
    volume_count=$(echo "$project_volumes" | wc -l)
    echo -e "${YELLOW}Total: $volume_count volumes${NC}"
else
    echo -e "${GREEN}✓ No project volumes found${NC}"
    volume_count=0
fi
echo ""

###############################################################################
# Step 5: Force remove Docker volumes
###############################################################################
echo -e "${BLUE}[5/7] Force removing Docker volumes...${NC}"
if [ -n "$project_volumes" ]; then
    echo "$project_volumes" | while read vol; do
        echo -e "${YELLOW}Removing: $vol${NC}"
        docker volume rm -f "$vol" 2>/dev/null || {
            echo -e "${RED}Failed to remove $vol, trying with sudo...${NC}"
            sudo docker volume rm -f "$vol" 2>/dev/null || echo -e "${RED}✗ Could not remove $vol${NC}"
        }
    done
    echo -e "${GREEN}✓ Volume removal attempted${NC}"
else
    echo -e "${GREEN}✓ No volumes to remove${NC}"
fi
echo ""

###############################################################################
# Step 6: Remove volume data from disk (multiple possible locations)
###############################################################################
echo -e "${BLUE}[6/7] Removing volume data from disk...${NC}"

# Possible Docker volume locations
VOLUME_LOCATIONS=(
    "/var/lib/docker/volumes/${PROJECT_NAME}_*"
    "/var/snap/docker/common/var-lib-docker/volumes/${PROJECT_NAME}_*"
    "$HOME/.local/share/docker/volumes/${PROJECT_NAME}_*"
)

removed_any=false
for location in "${VOLUME_LOCATIONS[@]}"; do
    if ls $location 2>/dev/null | grep -q .; then
        echo -e "${YELLOW}Found volumes at: ${location%/*}${NC}"
        if rm -rf $location 2>/dev/null; then
            echo -e "${GREEN}✓ Removed from ${location%/*}${NC}"
            removed_any=true
        else
            echo -e "${YELLOW}Trying with sudo...${NC}"
            if sudo rm -rf $location 2>/dev/null; then
                echo -e "${GREEN}✓ Removed from ${location%/*} (with sudo)${NC}"
                removed_any=true
            else
                echo -e "${RED}✗ Could not remove from ${location%/*}${NC}"
            fi
        fi
    fi
done

if [ "$removed_any" = false ]; then
    echo -e "${YELLOW}⚠ No volume data found on disk (may already be clean)${NC}"
fi
echo ""

###############################################################################
# Step 7: Remove project networks
###############################################################################
echo -e "${BLUE}[7/7] Removing project networks...${NC}"
project_networks=$(docker network ls --filter "name=${PROJECT_NAME}" -q)
if [ -n "$project_networks" ]; then
    docker network rm $project_networks 2>/dev/null || true
    echo -e "${GREEN}✓ Networks removed${NC}"
else
    echo -e "${GREEN}✓ No project networks found${NC}"
fi
echo ""

###############################################################################
# Additional cleanup: Prune system
###############################################################################
echo -e "${BLUE}Cleaning up dangling resources...${NC}"
docker system prune -f --volumes 2>/dev/null || true
echo -e "${GREEN}✓ System pruned${NC}"
echo ""

###############################################################################
# Verification
###############################################################################
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}VERIFICATION REPORT:${NC}"
echo ""

# Check containers
echo -e "${YELLOW}Remaining project containers:${NC}"
remaining_containers_check=$(docker ps -a --filter "name=${PROJECT_NAME}" --format "{{.Names}}" 2>/dev/null)
if [ -z "$remaining_containers_check" ]; then
    echo -e "${GREEN}✓ None (SUCCESS)${NC}"
else
    echo -e "${RED}✗ Found:${NC}"
    echo "$remaining_containers_check"
fi
echo ""

# Check volumes
echo -e "${YELLOW}Remaining project volumes:${NC}"
remaining_volumes=$(docker volume ls --filter "name=${PROJECT_NAME}" --format "{{.Name}}" 2>/dev/null)
if [ -z "$remaining_volumes" ]; then
    echo -e "${GREEN}✓ None (SUCCESS - All $volume_count volumes removed!)${NC}"
else
    echo -e "${RED}✗ Found:${NC}"
    echo "$remaining_volumes"
    echo ""
    echo -e "${RED}VOLUMES WERE NOT COMPLETELY REMOVED!${NC}"
    echo -e "${YELLOW}Try running these commands manually:${NC}"
    echo "$remaining_volumes" | while read vol; do
        echo -e "  ${BLUE}sudo docker volume rm -f $vol${NC}"
    done
fi
echo ""

# Check networks
echo -e "${YELLOW}Remaining project networks:${NC}"
remaining_networks=$(docker network ls --filter "name=${PROJECT_NAME}" --format "{{.Name}}" 2>/dev/null)
if [ -z "$remaining_networks" ]; then
    echo -e "${GREEN}✓ None (SUCCESS)${NC}"
else
    echo -e "${RED}✗ Found:${NC}"
    echo "$remaining_networks"
fi
echo ""

###############################################################################
# Summary
###############################################################################
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
if [ -z "$remaining_volumes" ] && [ -z "$remaining_containers_check" ]; then
    echo -e "${GREEN}✅ CLEANUP COMPLETED SUCCESSFULLY!${NC}"
    echo -e "${GREEN}   All data has been permanently removed.${NC}"
else
    echo -e "${RED}⚠️  CLEANUP INCOMPLETE!${NC}"
    echo -e "${YELLOW}   Some resources remain. See verification report above.${NC}"
fi
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo -e "  1. Verify volumes are gone: ${BLUE}docker volume ls | grep ${PROJECT_NAME}${NC}"
echo -e "  2. Start fresh: ${BLUE}docker-compose up -d${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
