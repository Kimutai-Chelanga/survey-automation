#!/bin/sh

# backup.sh - Database backup script with email notifications
# Place this file in ./backup_scripts/backup.sh

set -e

# Configuration
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups"
PG_BACKUP="${BACKUP_DIR}/messages_${TIMESTAMP}.sql"
MONGO_BACKUP_DIR="${BACKUP_DIR}/mongodb_${TIMESTAMP}"
COMPRESSED_FILE="${BACKUP_DIR}/backup_${TIMESTAMP}.tar.gz"
MAX_SIZE_BYTES=$((${BACKUP_MAX_SIZE_MB:-25} * 1024 * 1024))

# Email configuration
MSMTP_CONFIG="/tmp/msmtprc"

# Configure msmtp for sending emails
configure_email() {
    cat > "$MSMTP_CONFIG" << EOF
defaults
auth           on
tls            on
tls_trust_file /etc/ssl/certs/ca-certificates.crt
logfile        /tmp/msmtp.log

account        default
host           ${EMAIL_HOST}
port           ${EMAIL_PORT}
from           ${EMAIL_FROM}
user           ${EMAIL_USERNAME}
password       ${EMAIL_PASSWORD}
EOF
    chmod 600 "$MSMTP_CONFIG"
}

# Send email notification
send_email() {
    local subject="$1"
    local body="$2"
    local attachment="$3"

    if [ -n "$attachment" ] && [ -f "$attachment" ]; then
        # Send with attachment using base64 encoding
        local filename=$(basename "$attachment")
        local filesize=$(stat -c%s "$attachment")
        local filesize_mb=$(echo "scale=2; $filesize / 1024 / 1024" | bc)

        (
            echo "To: ${EMAIL_TO}"
            echo "From: ${EMAIL_FROM}"
            echo "Subject: ${subject}"
            echo "MIME-Version: 1.0"
            echo "Content-Type: multipart/mixed; boundary=\"BOUNDARY\""
            echo ""
            echo "--BOUNDARY"
            echo "Content-Type: text/plain; charset=utf-8"
            echo ""
            echo "${body}"
            echo ""
            echo "Attachment: ${filename}"
            echo "Size: ${filesize_mb} MB"
            echo ""
            echo "--BOUNDARY"
            echo "Content-Type: application/gzip"
            echo "Content-Transfer-Encoding: base64"
            echo "Content-Disposition: attachment; filename=\"${filename}\""
            echo ""
            base64 "$attachment"
            echo ""
            echo "--BOUNDARY--"
        ) | msmtp -C "$MSMTP_CONFIG" -t
    else
        # Send without attachment
        (
            echo "To: ${EMAIL_TO}"
            echo "From: ${EMAIL_FROM}"
            echo "Subject: ${subject}"
            echo ""
            echo "${body}"
        ) | msmtp -C "$MSMTP_CONFIG" -t
    fi
}

# Cleanup old backups
cleanup_old_backups() {
    echo "Cleaning up old backups..."
    find /backups -name "*.sql" -mtime +7 -delete
    find /backups -name "*.tar.gz" -mtime +7 -delete
    find /backups -name "mongodb_*" -type d -mtime +7 -exec rm -rf {} +
    echo "Cleanup completed"
}

# Main backup process
echo "==================================="
echo "Starting backup process: $TIMESTAMP"
echo "==================================="

# Configure email
configure_email

# Perform PostgreSQL backup
echo "Backing up PostgreSQL database..."
if pg_dump -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" "$POSTGRES_DB" > "$PG_BACKUP"; then
    echo "✓ PostgreSQL backup completed: $PG_BACKUP"
else
    ERROR_MSG="PostgreSQL backup failed at $TIMESTAMP"
    echo "✗ $ERROR_MSG"
    send_email "❌ Backup Failed - PostgreSQL" "$ERROR_MSG"
    exit 1
fi

# Perform MongoDB backup
echo "Backing up MongoDB database..."
if mongodump --uri="$MONGODB_URI" --out="$MONGO_BACKUP_DIR" --quiet; then
    echo "✓ MongoDB backup completed: $MONGO_BACKUP_DIR"
else
    ERROR_MSG="MongoDB backup failed at $TIMESTAMP"
    echo "✗ $ERROR_MSG"
    send_email "❌ Backup Failed - MongoDB" "$ERROR_MSG"
    exit 1
fi

# Compress backups
echo "Compressing backup files..."
if tar -czf "$COMPRESSED_FILE" -C "$BACKUP_DIR" \
    "$(basename $PG_BACKUP)" \
    "$(basename $MONGO_BACKUP_DIR)"; then
    echo "✓ Compression completed: $COMPRESSED_FILE"

    # Remove uncompressed files
    rm "$PG_BACKUP"
    rm -rf "$MONGO_BACKUP_DIR"
else
    ERROR_MSG="Compression failed at $TIMESTAMP"
    echo "✗ $ERROR_MSG"
    send_email "❌ Backup Failed - Compression" "$ERROR_MSG"
    exit 1
fi

# Check file size
COMPRESSED_SIZE=$(stat -c%s "$COMPRESSED_FILE")
COMPRESSED_SIZE_MB=$(echo "scale=2; $COMPRESSED_SIZE / 1024 / 1024" | bc)

echo "Compressed backup size: ${COMPRESSED_SIZE_MB} MB"
echo "Maximum email size: ${BACKUP_MAX_SIZE_MB} MB"

# Send email with or without attachment based on size
if [ "$COMPRESSED_SIZE" -le "$MAX_SIZE_BYTES" ]; then
    echo "Sending backup via email with attachment..."
    SUBJECT="✅ Database Backup Successful - $TIMESTAMP"
    BODY="Database backup completed successfully!

Timestamp: $TIMESTAMP
PostgreSQL Database: $POSTGRES_DB
MongoDB Database: messages_db

Backup Details:
- Compressed Size: ${COMPRESSED_SIZE_MB} MB
- Location: $COMPRESSED_FILE

The backup file is attached to this email.

System Information:
- Host: $POSTGRES_HOST
- Timezone: $TZ

Old backups (>7 days) have been cleaned up automatically."

    if send_email "$SUBJECT" "$BODY" "$COMPRESSED_FILE"; then
        echo "✓ Email sent successfully with attachment"
    else
        echo "✗ Failed to send email with attachment"
    fi
else
    echo "Backup file too large for email attachment (${COMPRESSED_SIZE_MB} MB > ${BACKUP_MAX_SIZE_MB} MB)"
    echo "Sending notification without attachment..."

    SUBJECT="✅ Database Backup Successful (File Too Large) - $TIMESTAMP"
    BODY="Database backup completed successfully!

Timestamp: $TIMESTAMP
PostgreSQL Database: $POSTGRES_DB
MongoDB Database: messages_db

Backup Details:
- Compressed Size: ${COMPRESSED_SIZE_MB} MB (too large for email)
- Location: $COMPRESSED_FILE
- Server Location: /backups directory

⚠️ The backup file is too large to attach to this email.
Please access the backup directly from the server.

System Information:
- Host: $POSTGRES_HOST
- Timezone: $TZ

Old backups (>7 days) have been cleaned up automatically."

    if send_email "$SUBJECT" "$BODY"; then
        echo "✓ Email notification sent successfully"
    else
        echo "✗ Failed to send email notification"
    fi
fi

# Cleanup old backups
cleanup_old_backups

echo "==================================="
echo "Backup process completed: $TIMESTAMP"
echo "==================================="
