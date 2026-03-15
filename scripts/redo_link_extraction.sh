#!/bin/bash

##############################################################################
# Rollback Links Extraction Script
# Purpose: Undo link insertions from the extraction pipeline
# Usage: ./rollback_extraction.sh [options]
##############################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Database connection details from .env
DB_HOST="${POSTGRES_HOST:-postgres}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-messages}"
DB_USER="${POSTGRES_USER:-airflow}"
DB_PASSWORD="${POSTGRES_PASSWORD:-airflow}"

# Container name
CONTAINER_NAME="postgres_db"

# Account ID from extraction pipeline
ACCOUNT_ID=1

##############################################################################
# Helper Functions
##############################################################################

print_header() {
    echo ""
    echo -e "${BLUE}█$(printf '█%.0s' {1..78})█${NC}"
    echo -e "${BLUE}█  $(printf '%-76s' "$1")█${NC}"
    echo -e "${BLUE}█$(printf '█%.0s' {1..78})█${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${CYAN}─$(printf '─%.0s' {1..78})─${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}─$(printf '─%.0s' {1..78})─${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# Function to execute SQL commands
exec_sql() {
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -t -c "$1" 2>/dev/null | xargs
}

# Function to check if container is running
check_container() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        print_error "PostgreSQL container '${CONTAINER_NAME}' is not running"
        print_info "Start it with: docker-compose up -d postgres"
        exit 1
    fi
    print_success "PostgreSQL container is running"
}

##############################################################################
# Main Rollback Functions
##############################################################################

show_current_state() {
    print_header "CURRENT EXTRACTION STATE"
    
    # Total links for account_id = 1
    TOTAL_LINKS=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID};")
    print_info "Total links for account_id=${ACCOUNT_ID}: ${TOTAL_LINKS}"
    
    # Links with tweet IDs containing '19'
    LINKS_WITH_19=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND tweet_id LIKE '%19%';")
    print_info "Links with '19' in tweet_id: ${LINKS_WITH_19}"
    
    # Links by workflow status
    echo ""
    print_section "Links by Workflow Status"
    
    PENDING=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND workflow_status = 'pending';")
    echo "  Pending: ${PENDING}"
    
    PROCESSING=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND workflow_status = 'processing';")
    echo "  Processing: ${PROCESSING}"
    
    COMPLETED=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND workflow_status = 'completed';")
    echo "  Completed: ${COMPLETED}"
    
    FAILED=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND workflow_status = 'failed';")
    echo "  Failed: ${FAILED}"
    
    # Recent extractions (last 24 hours)
    echo ""
    print_section "Recent Extractions"
    
    LAST_24H=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND scraped_time > NOW() - INTERVAL '24 hours';")
    echo "  Last 24 hours: ${LAST_24H}"
    
    LAST_HOUR=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND scraped_time > NOW() - INTERVAL '1 hour';")
    echo "  Last hour: ${LAST_HOUR}"
    
    # Sample of recent links
    echo ""
    print_section "Sample of Recent Links (Last 5)"
    echo ""
    
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
SELECT 
    id,
    tweet_id,
    workflow_status,
    TO_CHAR(scraped_time, 'YYYY-MM-DD HH24:MI:SS') as scraped
FROM links 
WHERE account_id = ${ACCOUNT_ID}
ORDER BY scraped_time DESC 
LIMIT 5;
EOF
    
    echo ""
}

# Option 1: Delete links from last extraction run
delete_last_run() {
    print_header "DELETE LINKS FROM LAST EXTRACTION RUN"
    
    # Get count from last hour (typical extraction run time)
    LAST_RUN_COUNT=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND scraped_time > NOW() - INTERVAL '1 hour';")
    
    if [ "$LAST_RUN_COUNT" -eq 0 ]; then
        print_warning "No links found from the last hour"
        print_info "Use option 2 or 3 for different time ranges"
        exit 0
    fi
    
    print_warning "This will delete ${LAST_RUN_COUNT} links from the last hour"
    echo ""
    
    read -p "Continue? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_info "Operation cancelled"
        exit 0
    fi
    
    echo ""
    print_info "Deleting links from last extraction run..."
    
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
DELETE FROM links 
WHERE account_id = ${ACCOUNT_ID} 
AND scraped_time > NOW() - INTERVAL '1 hour';
EOF
    
    print_success "Deleted ${LAST_RUN_COUNT} links from last extraction run"
}

# Option 2: Delete links from last 24 hours
delete_last_24h() {
    print_header "DELETE LINKS FROM LAST 24 HOURS"
    
    LAST_24H_COUNT=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND scraped_time > NOW() - INTERVAL '24 hours';")
    
    if [ "$LAST_24H_COUNT" -eq 0 ]; then
        print_warning "No links found from the last 24 hours"
        exit 0
    fi
    
    print_warning "This will delete ${LAST_24H_COUNT} links from the last 24 hours"
    echo ""
    
    read -p "Continue? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_info "Operation cancelled"
        exit 0
    fi
    
    echo ""
    print_info "Deleting links from last 24 hours..."
    
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
DELETE FROM links 
WHERE account_id = ${ACCOUNT_ID} 
AND scraped_time > NOW() - INTERVAL '24 hours';
EOF
    
    print_success "Deleted ${LAST_24H_COUNT} links from last 24 hours"
}

# Option 3: Delete all pending links
delete_pending_links() {
    print_header "DELETE ALL PENDING LINKS"
    
    PENDING_COUNT=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND workflow_status = 'pending';")
    
    if [ "$PENDING_COUNT" -eq 0 ]; then
        print_warning "No pending links found"
        exit 0
    fi
    
    print_warning "This will delete ${PENDING_COUNT} pending links"
    echo ""
    
    read -p "Continue? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_info "Operation cancelled"
        exit 0
    fi
    
    echo ""
    print_info "Deleting pending links..."
    
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
DELETE FROM links 
WHERE account_id = ${ACCOUNT_ID} 
AND workflow_status = 'pending';
EOF
    
    print_success "Deleted ${PENDING_COUNT} pending links"
}

# Option 4: Delete all links for account_id = 1
delete_all_account_links() {
    print_header "DELETE ALL LINKS FOR ACCOUNT_ID=${ACCOUNT_ID}"
    
    TOTAL_COUNT=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID};")
    
    if [ "$TOTAL_COUNT" -eq 0 ]; then
        print_warning "No links found for account_id=${ACCOUNT_ID}"
        exit 0
    fi
    
    print_error "⚠️  DESTRUCTIVE OPERATION ⚠️"
    print_warning "This will delete ALL ${TOTAL_COUNT} links for account_id=${ACCOUNT_ID}"
    print_warning "This action CANNOT be undone!"
    echo ""
    
    read -p "Type 'DELETE ALL' to confirm: " confirm
    if [[ "$confirm" != "DELETE ALL" ]]; then
        print_info "Operation cancelled"
        exit 0
    fi
    
    echo ""
    print_info "Deleting all links for account_id=${ACCOUNT_ID}..."
    
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
DELETE FROM links WHERE account_id = ${ACCOUNT_ID};
EOF
    
    print_success "Deleted ${TOTAL_COUNT} links for account_id=${ACCOUNT_ID}"
}

# Option 5: Delete links by specific tweet ID pattern
delete_by_tweet_pattern() {
    print_header "DELETE LINKS BY TWEET ID PATTERN"
    
    echo "Enter tweet ID pattern (e.g., '%19%' for tweets containing '19'):"
    read -p "Pattern: " pattern
    
    if [ -z "$pattern" ]; then
        print_error "Pattern cannot be empty"
        exit 1
    fi
    
    PATTERN_COUNT=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND tweet_id LIKE '${pattern}';")
    
    if [ "$PATTERN_COUNT" -eq 0 ]; then
        print_warning "No links found matching pattern: ${pattern}"
        exit 0
    fi
    
    print_warning "This will delete ${PATTERN_COUNT} links matching pattern: ${pattern}"
    echo ""
    
    # Show sample
    echo "Sample of links to be deleted:"
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
SELECT id, tweet_id, workflow_status 
FROM links 
WHERE account_id = ${ACCOUNT_ID} AND tweet_id LIKE '${pattern}'
LIMIT 5;
EOF
    
    echo ""
    read -p "Continue? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_info "Operation cancelled"
        exit 0
    fi
    
    echo ""
    print_info "Deleting links matching pattern: ${pattern}..."
    
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
DELETE FROM links 
WHERE account_id = ${ACCOUNT_ID} 
AND tweet_id LIKE '${pattern}';
EOF
    
    print_success "Deleted ${PATTERN_COUNT} links matching pattern: ${pattern}"
}

# Option 6: Reset workflow status to pending
reset_workflow_status() {
    print_header "RESET WORKFLOW STATUS TO PENDING"
    
    NON_PENDING=$(exec_sql "SELECT COUNT(*) FROM links WHERE account_id = ${ACCOUNT_ID} AND workflow_status != 'pending';")
    
    if [ "$NON_PENDING" -eq 0 ]; then
        print_warning "All links are already pending"
        exit 0
    fi
    
    print_info "This will reset ${NON_PENDING} links to 'pending' status"
    echo ""
    
    read -p "Continue? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_info "Operation cancelled"
        exit 0
    fi
    
    echo ""
    print_info "Resetting workflow status..."
    
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" << EOF
UPDATE links 
SET workflow_status = 'pending'
WHERE account_id = ${ACCOUNT_ID} 
AND workflow_status != 'pending';
EOF
    
    print_success "Reset ${NON_PENDING} links to pending status"
}

##############################################################################
# Menu System
##############################################################################

show_menu() {
    print_header "EXTRACTION ROLLBACK MENU"
    
    echo "Choose an option:"
    echo ""
    echo "  1) Delete links from last extraction run (last hour)"
    echo "  2) Delete links from last 24 hours"
    echo "  3) Delete all pending links"
    echo "  4) Delete ALL links for account_id=${ACCOUNT_ID} (DESTRUCTIVE)"
    echo "  5) Delete links by tweet ID pattern"
    echo "  6) Reset workflow status to pending (no deletion)"
    echo "  7) Show current state only"
    echo "  q) Quit"
    echo ""
    read -p "Enter option [1-7 or q]: " option
    
    case $option in
        1) delete_last_run ;;
        2) delete_last_24h ;;
        3) delete_pending_links ;;
        4) delete_all_account_links ;;
        5) delete_by_tweet_pattern ;;
        6) reset_workflow_status ;;
        7) show_current_state ;;
        q|Q) print_info "Exiting..."; exit 0 ;;
        *) print_error "Invalid option"; exit 1 ;;
    esac
}

##############################################################################
# Main Execution
##############################################################################

main() {
    print_header "EXTRACTION ROLLBACK SCRIPT"
    print_info "Target: PostgreSQL database '${DB_NAME}'"
    print_info "Account ID: ${ACCOUNT_ID}"
    echo ""
    
    # Check if container is running
    check_container
    echo ""
    
    # Show current state
    show_current_state
    
    # Parse command line arguments
    if [ $# -eq 0 ]; then
        show_menu
    else
        case $1 in
            --last-run) delete_last_run ;;
            --last-24h) delete_last_24h ;;
            --pending) delete_pending_links ;;
            --all) delete_all_account_links ;;
            --pattern) 
                if [ -z "$2" ]; then
                    print_error "Pattern required"
                    exit 1
                fi
                pattern=$2
                delete_by_tweet_pattern ;;
            --reset-status) reset_workflow_status ;;
            --status) show_current_state ;;
            --help|-h)
                echo "Usage: $0 [option]"
                echo ""
                echo "Options:"
                echo "  --last-run       Delete links from last extraction run"
                echo "  --last-24h       Delete links from last 24 hours"
                echo "  --pending        Delete all pending links"
                echo "  --all            Delete ALL links for account_id=${ACCOUNT_ID}"
                echo "  --pattern STR    Delete links matching tweet ID pattern"
                echo "  --reset-status   Reset workflow status to pending"
                echo "  --status         Show current state only"
                echo "  --help, -h       Show this help message"
                echo ""
                echo "If no option is provided, interactive menu is shown"
                ;;
            *)
                print_error "Unknown option: $1"
                print_info "Use --help for usage information"
                exit 1
                ;;
        esac
    fi
    
    echo ""
    print_header "OPERATION COMPLETE"
    print_success "Rollback completed successfully"
    echo ""
}

# Run main function
main "$@"