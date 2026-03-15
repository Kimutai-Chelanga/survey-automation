#!/bin/bash
# Docker cleanup script
# Usage: ./cleanup_docker.sh

cd /opt/final-automation

echo "🛑 Stopping all containers..."
docker compose -f docker-compose.prod.yml --env-file .env.production down

echo "🗑️  Removing containers, images, and volumes..."
docker compose -f docker-compose.prod.yml --env-file .env.production down -v --rmi all

echo "✅ Cleanup complete!"
echo ""
echo "To rebuild and start:"
echo "  docker compose -f docker-compose.prod.yml --env-file .env.production build"
echo "  docker compose -f docker-compose.prod.yml --env-file .env.production up -d"
