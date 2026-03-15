#!/bin/bash

# Database Export Script for Docker Compose Setup
# Exports PostgreSQL and MongoDB data to local databases folder

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPORT_DIR="${SCRIPT_DIR}/databases"
POSTGRES_DIR="${EXPORT_DIR}/postgres"
MONGODB_DIR="${EXPORT_DIR}/mongodb"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Docker container names (from your compose file)
POSTGRES_CONTAINER="postgres_db"
MONGODB_CONTAINER="mongodb"

# Database configuration (from your .env and compose)
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow"
POSTGRES_DB="messages"

MONGODB_USER="admin"
MONGODB_PASSWORD="admin123"
MONGODB_DB="messages_db"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log_info() {
    log "${BLUE}[INFO]${NC} $1"
}

log_success() {
    log "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    log "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    log "${RED}[ERROR]${NC} $1"
}

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    log_info "Docker is available and running"
}

# Check if containers are running
check_containers() {
    log_info "Checking if required containers are running..."
    
    # Check PostgreSQL container
    if ! docker ps --format "table {{.Names}}" | grep -q "^${POSTGRES_CONTAINER}$"; then
        log_error "PostgreSQL container '${POSTGRES_CONTAINER}' is not running"
        log_info "Available containers:"
        docker ps --format "table {{.Names}}\t{{.Status}}"
        log_info "Please start your Docker Compose services first: docker-compose up -d"
        exit 1
    fi
    
    # Check MongoDB container
    if ! docker ps --format "table {{.Names}}" | grep -q "^${MONGODB_CONTAINER}$"; then
        log_error "MongoDB container '${MONGODB_CONTAINER}' is not running"
        log_info "Available containers:"
        docker ps --format "table {{.Names}}\t{{.Status}}"
        log_info "Please start your Docker Compose services first: docker-compose up -d"
        exit 1
    fi
    
    log_success "Both database containers are running"
}

# Wait for databases to be ready
wait_for_databases() {
    log_info "Waiting for databases to be ready..."
    
    # Wait for PostgreSQL
    log_info "Checking PostgreSQL connection..."
    local postgres_ready=false
    local attempts=0
    local max_attempts=30
    
    while [ $attempts -lt $max_attempts ]; do
        if docker exec "${POSTGRES_CONTAINER}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" &> /dev/null; then
            postgres_ready=true
            break
        fi
        ((attempts++))
        sleep 2
    done
    
    if [ "$postgres_ready" = false ]; then
        log_error "PostgreSQL is not ready after ${max_attempts} attempts"
        exit 1
    fi
    
    # Wait for MongoDB
    log_info "Checking MongoDB connection..."
    local mongodb_ready=false
    attempts=0
    
    while [ $attempts -lt $max_attempts ]; do
        if docker exec "${MONGODB_CONTAINER}" mongosh --eval "db.adminCommand('ping')" --quiet &> /dev/null; then
            mongodb_ready=true
            break
        fi
        ((attempts++))
        sleep 2
    done
    
    if [ "$mongodb_ready" = false ]; then
        log_error "MongoDB is not ready after ${max_attempts} attempts"
        exit 1
    fi
    
    log_success "Both databases are ready"
}

# Create export directories
create_directories() {
    log_info "Creating export directories..."
    
    # Remove existing directories if they exist
    if [ -d "${EXPORT_DIR}" ]; then
        log_warning "Export directory already exists. Creating backup..."
        mv "${EXPORT_DIR}" "${EXPORT_DIR}_backup_${TIMESTAMP}"
    fi
    
    mkdir -p "${POSTGRES_DIR}"
    mkdir -p "${MONGODB_DIR}"
    
    log_success "Export directories created at: ${EXPORT_DIR}"
}

# Export PostgreSQL data
export_postgresql() {
    log_info "Starting PostgreSQL data export..."
    
    local postgres_export_file="${POSTGRES_DIR}/messages_${TIMESTAMP}.sql"
    local postgres_schema_file="${POSTGRES_DIR}/schema_${TIMESTAMP}.sql"
    
    # Export full database (schema + data)
    log_info "Exporting PostgreSQL database with data..."
    if docker exec "${POSTGRES_CONTAINER}" pg_dump -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" --verbose > "${postgres_export_file}" 2>/dev/null; then
        log_success "PostgreSQL data exported to: ${postgres_export_file}"
    else
        log_error "Failed to export PostgreSQL data"
        return 1
    fi
    
    # Export schema only
    log_info "Exporting PostgreSQL schema only..."
    if docker exec "${POSTGRES_CONTAINER}" pg_dump -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" --schema-only --verbose > "${postgres_schema_file}" 2>/dev/null; then
        log_success "PostgreSQL schema exported to: ${postgres_schema_file}"
    else
        log_warning "Failed to export PostgreSQL schema"
    fi
    
    # Export each table as CSV
    log_info "Exporting individual tables as CSV..."
    local csv_dir="${POSTGRES_DIR}/csv_exports"
    mkdir -p "${csv_dir}"
    
    # Get list of tables
    local tables=$(docker exec "${POSTGRES_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -c "SELECT tablename FROM pg_tables WHERE schemaname='public';" 2>/dev/null | grep -v '^$')
    
    if [ -n "$tables" ]; then
        while IFS= read -r table; do
            table=$(echo "$table" | xargs) # Trim whitespace
            if [ -n "$table" ]; then
                log_info "Exporting table: ${table}"
                if docker exec "${POSTGRES_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\COPY ${table} TO STDOUT WITH CSV HEADER;" > "${csv_dir}/${table}_${TIMESTAMP}.csv" 2>/dev/null; then
                    log_info "Table ${table} exported successfully"
                else
                    log_warning "Failed to export table ${table}"
                fi
            fi
        done <<< "$tables"
        log_success "PostgreSQL tables exported as CSV to: ${csv_dir}"
    else
        log_warning "No tables found in PostgreSQL database"
    fi
}

# Export MongoDB data
export_mongodb() {
    log_info "Starting MongoDB data export..."
    
    local mongodb_export_dir="${MONGODB_DIR}/dump_${TIMESTAMP}"
    local mongodb_json_dir="${MONGODB_DIR}/json_exports"
    
    # Create directories
    mkdir -p "${mongodb_export_dir}"
    mkdir -p "${mongodb_json_dir}"
    
    # Export using mongodump (BSON format)
    log_info "Exporting MongoDB using mongodump..."
    if docker exec "${MONGODB_CONTAINER}" mongodump --uri="mongodb://${MONGODB_USER}:${MONGODB_PASSWORD}@localhost:27017/${MONGODB_DB}?authSource=admin" --out="/tmp/dump" &> /dev/null; then
        # Copy from container to host
        docker cp "${MONGODB_CONTAINER}:/tmp/dump/${MONGODB_DB}" "${mongodb_export_dir}/" 2>/dev/null
        docker exec "${MONGODB_CONTAINER}" rm -rf "/tmp/dump" &> /dev/null
        log_success "MongoDB BSON dump exported to: ${mongodb_export_dir}"
    else
        log_error "Failed to export MongoDB using mongodump"
        return 1
    fi
    
    # Export collections as JSON
    log_info "Exporting MongoDB collections as JSON..."
    
    # Get list of collections
    local collections=$(docker exec "${MONGODB_CONTAINER}" mongosh "${MONGODB_DB}" --username "${MONGODB_USER}" --password "${MONGODB_PASSWORD}" --authenticationDatabase admin --quiet --eval "print(JSON.stringify(db.listCollectionNames()))" 2>/dev/null | grep -o '"[^"]*"' | tr -d '"')
    
    if [ -n "$collections" ]; then
        for collection in $collections; do
            if [ -n "$collection" ]; then
                log_info "Exporting collection: ${collection}"
                if docker exec "${MONGODB_CONTAINER}" mongoexport --uri="mongodb://${MONGODB_USER}:${MONGODB_PASSWORD}@localhost:27017/${MONGODB_DB}?authSource=admin" --collection="${collection}" --out="/tmp/${collection}.json" &> /dev/null; then
                    docker cp "${MONGODB_CONTAINER}:/tmp/${collection}.json" "${mongodb_json_dir}/${collection}_${TIMESTAMP}.json" 2>/dev/null
                    docker exec "${MONGODB_CONTAINER}" rm -f "/tmp/${collection}.json" &> /dev/null
                    log_info "Collection ${collection} exported successfully"
                else
                    log_warning "Failed to export collection ${collection}"
                fi
            fi
        done
        log_success "MongoDB collections exported as JSON to: ${mongodb_json_dir}"
    else
        log_warning "No collections found in MongoDB database"
    fi
}

# Create README file with export information
create_readme() {
    local readme_file="${EXPORT_DIR}/README.md"
    
    cat > "${readme_file}" << EOF
# Database Export - ${TIMESTAMP}

This directory contains exported data from your Docker Compose database setup.

## Export Information
- **Export Date**: $(date)
- **PostgreSQL Container**: ${POSTGRES_CONTAINER}
- **MongoDB Container**: ${MONGODB_CONTAINER}
- **PostgreSQL Database**: ${POSTGRES_DB}
- **MongoDB Database**: ${MONGODB_DB}

## Directory Structure

### PostgreSQL Exports (\`postgres/\`)
- \`messages_${TIMESTAMP}.sql\` - Complete database dump (schema + data)
- \`schema_${TIMESTAMP}.sql\` - Schema-only dump
- \`csv_exports/\` - Individual tables exported as CSV files

### MongoDB Exports (\`mongodb/\`)
- \`automa_workflows.json\` - Automa workflows collection only

## Restoration Instructions

### PostgreSQL
\`\`\`bash
# Restore full database
docker exec -i ${POSTGRES_CONTAINER} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < postgres/messages_${TIMESTAMP}.sql

# Or restore schema only
docker exec -i ${POSTGRES_CONTAINER} psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < postgres/schema_${TIMESTAMP}.sql
\`\`\`

### MongoDB
\`\`\`bash
# Import automa_workflows collection
docker cp mongodb/automa_workflows.json ${MONGODB_CONTAINER}:/tmp/automa_workflows.json
docker exec ${MONGODB_CONTAINER} mongoimport --uri="mongodb://${MONGODB_USER}:${MONGODB_PASSWORD}@localhost:27017/${MONGODB_DB}?authSource=admin" --collection=automa_workflows --file=/tmp/automa_workflows.json
\`\`\`

## File Sizes
EOF

    # Add file sizes to README
    if command -v du &> /dev/null; then
        echo "### Export Sizes" >> "${readme_file}"
        du -sh "${POSTGRES_DIR}" 2>/dev/null | sed 's/\t/ - PostgreSQL: /' >> "${readme_file}" || echo "PostgreSQL: Size calculation failed" >> "${readme_file}"
        du -sh "${MONGODB_DIR}" 2>/dev/null | sed 's/\t/ - MongoDB: /' >> "${readme_file}" || echo "MongoDB: Size calculation failed" >> "${readme_file}"
        du -sh "${EXPORT_DIR}" 2>/dev/null | sed 's/\t/ - Total: /' >> "${readme_file}" || echo "Total: Size calculation failed" >> "${readme_file}"
    fi
    
    log_success "README created at: ${readme_file}"
}

# Main execution function
main() {
    log_info "Starting database export process..."
    
    # Check prerequisites
    check_docker
    check_containers
    wait_for_databases
    
    # Create export structure
    create_directories
    
    # Export data
    export_postgresql
    export_mongodb
    
    # Create documentation
    create_readme
    
    log_success "Database export completed successfully!"
    log_info "Export location: ${EXPORT_DIR}"
    log_info "PostgreSQL exports: ${POSTGRES_DIR}"
    log_info "MongoDB exports: ${MONGODB_DIR}"
    
    # Show summary
    echo
    echo "=== Export Summary ==="
    if command -v tree &> /dev/null; then
        tree "${EXPORT_DIR}" -L 3
    else
        find "${EXPORT_DIR}" -type f 2>/dev/null | head -20
        local total_files=$(find "${EXPORT_DIR}" -type f 2>/dev/null | wc -l)
        if [ "$total_files" -gt 20 ]; then
            echo "... and $(($total_files - 20)) more files"
        fi
    fi
}

# Handle script interruption
cleanup() {
    log_warning "Export process interrupted"
    exit 1
}

trap cleanup SIGINT SIGTERM

# Run main function
main "$@"