#!/bin/bash
# File: scripts/shutdown.sh
# Graceful shutdown script for Docker Compose

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Get the project root (parent of scripts directory)
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Change to project root
cd "$PROJECT_ROOT"

# Configuration
COMPOSE_FILE="docker-compose.yml"
PROJECT_NAME="final-automation"
SHUTDOWN_TIMEOUT=30

# Detect docker-compose command (V1 vs V2)
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    echo -e "${RED}ERROR: Neither 'docker-compose' nor 'docker compose' found!${NC}"
    exit 1
fi

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Graceful Docker Compose Shutdown Script             ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to print status
print_status() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARNING:${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date +'%H:%M:%S')] ERROR:${NC} $1"
}

# Function to check if containers are running
check_running_containers() {
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" ps -q 2>/dev/null | wc -l
}

# Function to gracefully stop a specific service
graceful_stop_service() {
    local service=$1
    local timeout=${2:-30}

    print_status "Stopping $service (timeout: ${timeout}s)..."
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" stop -t "$timeout" "$service" 2>/dev/null || true
}

# Main shutdown sequence
main() {
    print_status "Project root: $PROJECT_ROOT"
    print_status "Using command: $DOCKER_COMPOSE"
    print_status "Starting graceful shutdown sequence..."
    echo ""

    # Check if docker-compose.yml exists
    if [ ! -f "$COMPOSE_FILE" ]; then
        print_error "docker-compose.yml not found in $PROJECT_ROOT"
        exit 1
    fi

    # Check if any containers are running
    local running_count=$(check_running_containers)

    if [ "$running_count" -eq 0 ]; then
        print_warning "No containers are running."
        read -p "Do you want to remove volumes anyway? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            $DOCKER_COMPOSE -f "$COMPOSE_FILE" down -v
            print_status "Volumes removed."
        fi
        exit 0
    fi

    print_status "Found $running_count running container(s)"
    echo ""

    # Step 1: Stop application containers first
    print_status "Step 1/5: Stopping application containers..."
    graceful_stop_service "app" 20
    sleep 2

    # Step 2: Stop Airflow services
    print_status "Step 2/5: Stopping Airflow services..."
    graceful_stop_service "airflow-scheduler" 30
    sleep 2
    graceful_stop_service "airflow-webserver" 15
    sleep 2

    # Step 3: Stop databases (most important for data integrity)
    print_status "Step 3/5: Stopping databases (this may take a moment)..."
    graceful_stop_service "postgres" 45
    sleep 3
    graceful_stop_service "mongodb" 45
    sleep 3

    # Step 4: Verify all containers are stopped
    print_status "Step 4/5: Verifying all containers stopped..."
    running_count=$(check_running_containers)

    if [ "$running_count" -gt 0 ]; then
        print_warning "Some containers still running. Forcing stop..."
        $DOCKER_COMPOSE -f "$COMPOSE_FILE" stop -t 10
        sleep 5
    fi

    # Step 5: Remove containers and networks
    print_status "Step 5/5: Removing containers and networks..."
    $DOCKER_COMPOSE -f "$COMPOSE_FILE" down

    echo ""
    print_status "✓ Graceful shutdown complete!"
    echo ""

    # Ask about volume removal
    read -p "Do you want to REMOVE ALL DATA (volumes)? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        print_warning "⚠️  This will DELETE ALL DATA permanently!"
        read -p "Are you ABSOLUTELY sure? Type 'yes' to confirm: " confirm

        if [ "$confirm" = "yes" ]; then
            print_status "Removing volumes..."
            $DOCKER_COMPOSE -f "$COMPOSE_FILE" down -v

            # Extra cleanup
            print_status "Cleaning up any dangling volumes..."
            docker volume prune -f 2>/dev/null || true

            print_status "✓ All data removed!"
        else
            print_status "Volume removal cancelled. Data preserved."
        fi
    else
        print_status "Data volumes preserved."
    fi

    echo ""
    print_status "Shutdown complete! Safe to close terminal."
}

# Run main function
main
