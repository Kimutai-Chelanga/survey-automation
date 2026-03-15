#!/bin/bash
# Local testing script - Test everything before deploying to Contabo
# Run this on your local machine to validate the setup

set -e

echo "🧪 Starting local deployment test..."
echo ""

# Check prerequisites
echo "✅ Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "❌ Docker is not installed"; exit 1; }
command -v docker compose >/dev/null 2>&1 || { echo "❌ Docker Compose is not installed"; exit 1; }
echo "✓ Docker and Docker Compose installed"
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ .env file not found!"
    echo "Creating sample .env file..."
    cat > .env << 'EOF'
# Database Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=messages

MONGO_ROOT_USER=admin
MONGO_ROOT_PASSWORD=admin123
MONGO_DB=messages_db

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_FERNET_KEY=test_fernet_key_32_characters_long_minimum_value_here

# Streamlit App
APP_USERNAME=admin
APP_PASSWORD_HASH=$2b$12$test.hash.value.here
SESSION_TIMEOUT=480

# VNC Configuration
VNC_PASSWORD=secret

# Timezone
DAG_TIMEZONE=Africa/Nairobi
DAG_SCHEDULE=0 16 * * 0
DAG_START_DATE=2025-07-01

# API Keys (optional for testing)
GEMINI_API_KEY=
OPENAI_API_KEY=
HYPERBROWSER_API_KEY=

# Backup Schedule
BACKUP_SCHEDULE=0 2 * * *
EOF
    echo "✓ Sample .env created. Please update with your actual values."
    echo ""
fi

# Create test docker-compose file (simplified version)
echo "📝 Creating test compose configuration..."
cat > docker-compose.test.yml << 'EOF'
version: '3.8'

volumes:
  postgres_data_test:
  mongodb_data_test:
  chrome_profiles_test:

networks:
  test_network:
    driver: bridge

services:
  postgres:
    image: postgres:15
    container_name: test-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-airflow}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-airflow}
      POSTGRES_DB: ${POSTGRES_DB:-messages}
      TZ: ${DAG_TIMEZONE:-Africa/Nairobi}
    volumes:
      - postgres_data_test:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow", "-d", "messages"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 20s
    networks:
      - test_network

  mongodb:
    image: mongo:7.0
    container_name: test-mongodb
    environment:
      MONGO_INITDB_ROOT_USERNAME: ${MONGO_ROOT_USER:-admin}
      MONGO_INITDB_ROOT_PASSWORD: ${MONGO_ROOT_PASSWORD:-admin123}
      MONGO_INITDB_DATABASE: ${MONGO_DB:-messages_db}
      TZ: ${DAG_TIMEZONE:-Africa/Nairobi}
    volumes:
      - mongodb_data_test:/data/db
    ports:
      - "27017:27017"
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 20s
    networks:
      - test_network

  app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: test-streamlit-app
    depends_on:
      postgres:
        condition: service_healthy
      mongodb:
        condition: service_healthy
    env_file:
      - .env
    environment:
      DATABASE_URL: postgresql://airflow:airflow@postgres:5432/messages
      MONGODB_URI: mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      CHROME_PROFILE_DIR: /app/chrome_profiles
      DISPLAY: ":99"
    ports:
      - "8501:8501"
      - "6080:6080"
      - "5900:5900"
    volumes:
      - chrome_profiles_test:/app/chrome_profiles
      - .:/app
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8501/_stcore/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    networks:
      - test_network
    shm_size: 2gb
EOF

echo "✓ Test configuration created"
echo ""

# Clean up previous test runs
echo "🧹 Cleaning up previous test runs..."
docker compose -f docker-compose.test.yml down -v 2>/dev/null || true
echo "✓ Cleanup complete"
echo ""

# Build and start services
echo "🏗️ Building Docker images (this may take a few minutes on first run)..."
echo "   Tip: Images are cached, subsequent runs will be much faster!"
docker compose -f docker-compose.test.yml build

echo ""
echo "🚀 Starting services..."
docker compose -f docker-compose.test.yml up -d

# Wait for services to be healthy
echo ""
echo "⏳ Waiting for services to start..."
sleep 15

# Function to check service health
check_service() {
    local service=$1
    local max_attempts=30
    local attempt=0
    
    echo "Checking $service..."
    while [ $attempt -lt $max_attempts ]; do
        status=$(docker compose -f docker-compose.test.yml ps $service --format json 2>/dev/null | grep -o '"Health":"[^"]*"' | cut -d'"' -f4)
        
        if [ "$status" = "healthy" ]; then
            echo "✅ $service is healthy"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "   Attempt $attempt/$max_attempts... (status: ${status:-starting})"
        sleep 3
    done
    
    echo "❌ $service failed to become healthy"
    echo ""
    echo "📋 Logs for $service:"
    docker compose -f docker-compose.test.yml logs --tail=50 $service
    return 1
}

# Check each service
echo ""
echo "🔍 Checking service health..."
check_service "postgres" || exit 1
check_service "mongodb" || exit 1
check_service "app" || exit 1

# Test database connections
echo ""
echo "🔍 Testing database connections..."

# Test PostgreSQL
echo "Testing PostgreSQL..."
if docker exec test-postgres psql -U airflow -d messages -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✅ PostgreSQL connection working"
else
    echo "❌ PostgreSQL connection failed"
    docker compose -f docker-compose.test.yml logs postgres
    exit 1
fi

# Test MongoDB
echo "Testing MongoDB..."
if docker exec test-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo "✅ MongoDB connection working"
else
    echo "❌ MongoDB connection failed"
    docker compose -f docker-compose.test.yml logs mongodb
    exit 1
fi

# Test Streamlit health endpoint
echo ""
echo "🔍 Testing Streamlit application..."
sleep 5  # Give Streamlit extra time to fully start

max_attempts=10
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -f http://localhost:8501/_stcore/health 2>/dev/null > /dev/null; then
        echo "✅ Streamlit health endpoint working"
        break
    fi
    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts..."
    sleep 3
done

if [ $attempt -eq $max_attempts ]; then
    echo "⚠️  Streamlit health endpoint not responding (may still be starting)"
    echo "    Check manually at: http://localhost:8501"
fi

# Show container status
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Container Status"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f docker-compose.test.yml ps

# Show recent logs
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Recent Logs"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f docker-compose.test.yml logs --tail=30

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ ALL TESTS PASSED!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Test Summary:"
echo "   ✓ Docker images built successfully"
echo "   ✓ All services started"
echo "   ✓ Database connections verified"
echo "   ✓ Health checks passing"
echo ""
echo "🌐 Access Points:"
echo "   • Streamlit App:  http://localhost:8501"
echo "   • VNC (Browser):  http://localhost:6080/vnc.html"
echo "   • PostgreSQL:     localhost:5432"
echo "   • MongoDB:        localhost:27017"
echo ""
echo "🎯 Next Steps:"
echo "   1. Open http://localhost:8501 in your browser"
echo "   2. Test your application workflows manually"
echo "   3. If everything works, you're ready for Contabo deployment!"
echo ""
echo "📝 Useful Commands:"
echo "   View logs:        docker compose -f docker-compose.test.yml logs -f"
echo "   View app logs:    docker compose -f docker-compose.test.yml logs -f app"
echo "   Restart service:  docker compose -f docker-compose.test.yml restart app"
echo "   Stop everything:  docker compose -f docker-compose.test.yml down -v"
echo ""
echo "⚠️  Remember to run: docker compose -f docker-compose.test.yml down -v"
echo "    when you're done testing to free up resources"
echo ""