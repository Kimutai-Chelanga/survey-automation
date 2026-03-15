#!/bin/bash

###############################################################################
# Docker Compose Complete Cleanup Script
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
echo -e "${BLUE}║     Docker Compose Complete Cleanup Script                ║${NC}"
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
# Step 1: Stop and remove containers
###############################################################################
echo -e "${BLUE}[1/6] Stopping and removing containers...${NC}"
if docker-compose down -v 2>/dev/null; then
    echo -e "${GREEN}✓ Containers stopped and removed${NC}"
else
    echo -e "${YELLOW}⚠ No running containers or docker-compose.yml not found${NC}"
fi
echo ""

###############################################################################
# Step 2: Remove all project containers (if any remain)
###############################################################################
echo -e "${BLUE}[2/6] Removing any remaining project containers...${NC}"
remaining_containers=$(docker ps -a --filter "name=${PROJECT_NAME}" -q)
if [ -n "$remaining_containers" ]; then
    docker rm -f $remaining_containers
    echo -e "${GREEN}✓ Removed remaining containers${NC}"
else
    echo -e "${GREEN}✓ No remaining containers found${NC}"
fi
echo ""

###############################################################################
# Step 3: Remove Docker volumes
###############################################################################
echo -e "${BLUE}[3/6] Removing Docker volumes...${NC}"
project_volumes=$(docker volume ls --filter "name=${PROJECT_NAME}" -q)
if [ -n "$project_volumes" ]; then
    docker volume rm $project_volumes 2>/dev/null || true
    echo -e "${GREEN}✓ Docker volumes removed${NC}"
else
    echo -e "${GREEN}✓ No project volumes found${NC}"
fi
echo ""

###############################################################################
# Step 4: Remove volume data from disk
###############################################################################
echo -e "${BLUE}[4/6] Removing volume data from disk...${NC}"
if [ -d "/var/lib/docker/volumes" ]; then
    # Try without sudo first
    if rm -rf /var/lib/docker/volumes/${PROJECT_NAME}_* 2>/dev/null; then
        echo -e "${GREEN}✓ Volume data removed from disk${NC}"
    else
        # Try with sudo
        echo -e "${YELLOW}Attempting with sudo...${NC}"
        if sudo rm -rf /var/lib/docker/volumes/${PROJECT_NAME}_* 2>/dev/null; then
            echo -e "${GREEN}✓ Volume data removed from disk (with sudo)${NC}"
        else
            echo -e "${YELLOW}⚠ Could not remove volume data from disk (may require manual cleanup)${NC}"
        fi
    fi
else
    echo -e "${YELLOW}⚠ Docker volumes directory not found${NC}"
fi
echo ""

###############################################################################
# Step 5: Remove project networks
###############################################################################
echo -e "${BLUE}[5/6] Removing project networks...${NC}"
project_networks=$(docker network ls --filter "name=${PROJECT_NAME}" -q)
if [ -n "$project_networks" ]; then
    docker network rm $project_networks 2>/dev/null || true
    echo -e "${GREEN}✓ Networks removed${NC}"
else
    echo -e "${GREEN}✓ No project networks found${NC}"
fi
echo ""

###############################################################################
# Step 6: Clean up dangling resources
###############################################################################
echo -e "${BLUE}[6/6] Cleaning up dangling resources...${NC}"
docker system prune -f --volumes 2>/dev/null || true
echo -e "${GREEN}✓ Dangling resources cleaned${NC}"
echo ""

###############################################################################
# Verification
###############################################################################
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}Verification:${NC}"
echo ""

echo -e "${YELLOW}Remaining project containers:${NC}"
remaining_check=$(docker ps -a --filter "name=${PROJECT_NAME}" --format "table {{.Names}}\t{{.Status}}")
if [ -z "$remaining_check" ]; then
    echo -e "${GREEN}✓ None${NC}"
else
    echo "$remaining_check"
fi
echo ""

echo -e "${YELLOW}Remaining project volumes:${NC}"
remaining_volumes=$(docker volume ls --filter "name=${PROJECT_NAME}" --format "table {{.Name}}")
if [ -z "$remaining_volumes" ]; then
    echo -e "${GREEN}✓ None${NC}"
else
    echo "$remaining_volumes"
fi
echo ""

echo -e "${YELLOW}Remaining project networks:${NC}"
remaining_networks=$(docker network ls --filter "name=${PROJECT_NAME}" --format "table {{.Name}}")
if [ -z "$remaining_networks" ]; then
    echo -e "${GREEN}✓ None${NC}"
else
    echo "$remaining_networks"
fi
echo ""

###############################################################################
# Summary
###############################################################################
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Cleanup completed successfully!${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo -e "  1. Run: ${BLUE}docker-compose up -d${NC} to start fresh containers"
echo -e "  2. All databases and data will be reinitialized"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
