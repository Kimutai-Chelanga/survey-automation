#!/bin/bash

# ─────────────────────────────────────────────
# Reset Airflow Admin User
# ─────────────────────────────────────────────

CONTAINER="airflow_webserver"
USERNAME="admin"
FIRSTNAME="Admin"
LASTNAME="User"
ROLE="Admin"
EMAIL="admin@localhost"
PASSWORD="kimu"

echo "→ Resetting Airflow admin user in container: $CONTAINER"

docker exec "$CONTAINER" bash -c "
  echo '→ Deleting user: $USERNAME' &&
  airflow users delete -u $USERNAME &&
  echo '→ Creating user: $USERNAME' &&
  airflow users create \
    --username $USERNAME \
    --firstname $FIRSTNAME \
    --lastname $LASTNAME \
    --role $ROLE \
    --email $EMAIL \
    --password $PASSWORD &&
  echo '✓ Done! User $USERNAME recreated successfully.'
"
