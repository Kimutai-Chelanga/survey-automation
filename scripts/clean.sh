#!/bin/bash
# docker_cleanup.sh - Complete Docker cleanup script

set -e

echo "🧹 Starting Docker cleanup process..."

# Function to stop containers safely
stop_containers() {
    echo "📋 Listing running containers..."
    docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
    
    echo ""
    echo "🛑 Stopping all running containers..."
    
    # Get all running container IDs
    RUNNING_CONTAINERS=$(docker ps -q)
    
    if [ ! -z "$RUNNING_CONTAINERS" ]; then
        echo "Stopping containers: $RUNNING_CONTAINERS"
        docker stop $RUNNING_CONTAINERS
        echo "✅ All containers stopped"
    else
        echo "ℹ️  No running containers found"
    fi
}

# Function to remove containers
remove_containers() {
    echo ""
    echo "🗑️  Removing stopped containers..."
    
    # Get all container IDs (including stopped ones)
    ALL_CONTAINERS=$(docker ps -aq)
    
    if [ ! -z "$ALL_CONTAINERS" ]; then
        echo "Removing containers: $ALL_CONTAINERS"
        docker rm -f $ALL_CONTAINERS
        echo "✅ All containers removed"
    else
        echo "ℹ️  No containers to remove"
    fi
}

# Function to clean up volumes
cleanup_volumes() {
    echo ""
    echo "🧹 Cleaning up Docker volumes..."
    
    # List volumes
    echo "Current volumes:"
    docker volume ls
    
    echo ""
    echo "Removing unused volumes..."
    docker volume prune -f
    echo "✅ Volume cleanup completed"
}

# Function to clean up networks
cleanup_networks() {
    echo ""
    echo "🌐 Cleaning up Docker networks..."
    
    # List networks
    echo "Current networks:"
    docker network ls
    
    echo ""
    echo "Removing unused networks..."
    docker network prune -f
    echo "✅ Network cleanup completed"
}

# Function to clean up images (optional)
cleanup_images() {
    echo ""
    echo "🖼️  Cleaning up unused Docker images..."
    
    # List images
    echo "Current images:"
    docker image ls
    
    echo ""
    echo "Removing unused images..."
    docker image prune -f
    echo "✅ Image cleanup completed"
}

# Main cleanup process
main() {
    echo "🚀 Docker Cleanup Script"
    echo "======================="
    echo ""
    
    # Stop docker-compose services first
    echo "🐳 Attempting to stop docker-compose services..."
    if [ -f "docker-compose.yml" ] || [ -f "docker-compose.yaml" ]; then
        docker-compose down -v 2>/dev/null || echo "⚠️  docker-compose down failed or no services running"
    else
        echo "ℹ️  No docker-compose file found"
    fi
    
    # Stop individual containers
    stop_containers
    
    # Remove containers
    remove_containers
    
    # Clean up volumes
    cleanup_volumes
    
    # Clean up networks
    cleanup_networks
    
    # Ask before cleaning images (optional)
    read -p "🤔 Do you want to remove unused Docker images as well? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup_images
    fi
    
    # Final status
    echo ""
    echo "🎉 Docker cleanup completed!"
    echo ""
    echo "📊 Final status:"
    echo "==============="
    echo "Running containers:"
    docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}" || echo "No containers running"
    echo ""
    echo "Volumes:"
    docker volume ls || echo "No volumes"
    echo ""
    echo "Networks:"
    docker network ls | grep -v "bridge\|host\|none" || echo "Only default networks"
}

# Run main function
main