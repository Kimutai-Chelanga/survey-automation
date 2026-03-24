# Create the messages database manually
docker exec -it postgres_db psql -U airflow -d postgres -c "CREATE DATABASE messages;"

# Verify it was created
docker exec -it postgres_db psql -U airflow -d postgres -c "\l" | grep messages

# Check if you can connect to it
docker exec -it postgres_db psql -U airflow -d messages -c "SELECT 1;"