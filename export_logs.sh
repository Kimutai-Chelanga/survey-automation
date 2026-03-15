#!/bin/bash

# Complete Services Log Export Script
# Exports logs from ALL services (running, stopped, and exited)

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BASE_DIR="./exported_logs_$TIMESTAMP"

echo "========================================="
echo "Docker Services Log Export"
echo "Timestamp: $TIMESTAMP"
echo "========================================="

mkdir -p "$BASE_DIR"

# Define all expected services based on docker-compose.yml
EXPECTED_SERVICES=(
    "mongodb"
    "postgres" 
    "airflow-init"
    "airflow_webserver"
    "airflow_scheduler"
    "chrome-container"
    "streamlit-app"
)

# Get additional containers not in expected services
ADDITIONAL_CONTAINERS=$(docker ps -a --format "{{.Names}}" | grep -v -E "^($(IFS=\|; echo "${EXPECTED_SERVICES[*]}"))$" || true)

# Combine all targets
ALL_TARGETS=("${EXPECTED_SERVICES[@]}")
if [ ! -z "$ADDITIONAL_CONTAINERS" ]; then
    while read -r container; do
        ALL_TARGETS+=("$container")
    done <<< "$ADDITIONAL_CONTAINERS"
fi

echo "Attempting to export logs for these services/containers:"
printf '%s\n' "${ALL_TARGETS[@]}"
echo ""

SUCCESS_COUNT=0
TOTAL_COUNT=${#ALL_TARGETS[@]}

for TARGET in "${ALL_TARGETS[@]}"; do
    echo "Processing: $TARGET"
    
    # Create service folder
    SERVICE_DIR="$BASE_DIR/$TARGET"
    mkdir -p "$SERVICE_DIR"
    
    # Check if container exists
    if docker ps -a --format "{{.Names}}" | grep -q "^${TARGET}$"; then
        # Get container info
        STATUS=$(docker ps -a --filter "name=^${TARGET}$" --format "{{.Status}}")
        IMAGE=$(docker ps -a --filter "name=^${TARGET}$" --format "{{.Image}}")
        
        LOG_FILE="$SERVICE_DIR/${TARGET}_logs.txt"
        
        # Add header
        {
            echo "========================================"
            echo "Service: $TARGET"
            echo "Container: $TARGET"
            echo "Image: $IMAGE"
            echo "Status: $STATUS"
            echo "Export Time: $(date)"
            echo "========================================"
            echo ""
        } > "$LOG_FILE"
        
        # Export logs
        if docker logs "$TARGET" >> "$LOG_FILE" 2>&1; then
            FILE_SIZE=$(du -h "$LOG_FILE" | cut -f1)
            echo "  ✓ Success: $FILE_SIZE"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            echo "  ✗ Failed to get logs"
            rm -f "$LOG_FILE"
            rmdir "$SERVICE_DIR" 2>/dev/null
        fi
    else
        echo "  ⚠ Container does not exist (not started yet or removed)"
        rmdir "$SERVICE_DIR" 2>/dev/null
    fi
done

echo ""
echo "========================================="
echo "Export Summary:"
echo "Successfully exported: $SUCCESS_COUNT/$TOTAL_COUNT services"
echo "Export location: $BASE_DIR"
echo ""

if [ -d "$BASE_DIR" ] && [ "$(ls -A "$BASE_DIR" 2>/dev/null)" ]; then
    echo "Created log files:"
    find "$BASE_DIR" -name "*.txt" -exec echo "  {}" \; -exec du -h {} \; | sed 'N;s/\n/ - /'
    echo ""
    echo "Directory structure:"
    tree "$BASE_DIR" 2>/dev/null || find "$BASE_DIR" -type d | sed 's/[^/]*\//  /g'
else
    echo "No log files were created."
fi

# Add to .gitignore and .dockerignore
for IGNORE_FILE in ".gitignore" ".dockerignore"; do
    if [ ! -f "$IGNORE_FILE" ]; then
        touch "$IGNORE_FILE"
    fi
    
    if ! grep -q "^exported_logs_\*$" "$IGNORE_FILE"; then
        echo "exported_logs_*" >> "$IGNORE_FILE"
        echo "Added 'exported_logs_*' to $IGNORE_FILE"
    else
        echo "'exported_logs_*' already exists in $IGNORE_FILE"
    fi
done

echo ""
echo "Commands to view logs:"
echo "  View specific service: cat $BASE_DIR/<service_name>/<service_name>_logs.txt"
echo "  List all files: ls -la $BASE_DIR/*/"