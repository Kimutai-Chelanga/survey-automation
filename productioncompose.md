version: '3.8'

services:
  mongodb:
    image: mongo:7.0
    container_name: mongodb
    environment:
      - MONGO_INITDB_ROOT_USERNAME=admin
      - MONGO_INITDB_ROOT_PASSWORD=admin123
      - MONGO_INITDB_DATABASE=messages_db
      - TZ=${DAG_TIMEZONE:-Africa/Nairobi}
    volumes:
      - mongodb_data:/data/db
      - ./mongo-init.js:/docker-entrypoint-initdb.d/mongo-init.js:ro
    ports:
      - "27017:27017"
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 30s
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  postgres:
    image: postgres:15
    container_name: postgres_db
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=messages
      - TZ=${DAG_TIMEZONE:-Africa/Nairobi}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-db.sql:/docker-entrypoint-initdb.d/init-db.sql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow", "-d", "messages"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 30s
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  airflow-init:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    container_name: airflow_init
    depends_on:
      postgres:
        condition: service_healthy
      mongodb:
        condition: service_healthy
    environment:
      # Core Airflow Configuration
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__PARALLELISM=8
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/messages
      - AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY}
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AIRFLOW__CORE__DEFAULT_TIMEZONE=${DAG_TIMEZONE:-Africa/Nairobi}
      - AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
      - AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=8
      - PYTHONPATH=/opt/airflow/src
      
      # Webserver Configuration
      - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
      - AIRFLOW__WEBSERVER__BASE_URL=http://0.0.0.0:8080
      - AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
      - AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
      - AIRFLOW__WEBSERVER__WORKER_REFRESH_BATCH_SIZE=1
      - AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=6000
      
      # Logging Configuration
      - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
      - AIRFLOW__LOGGING__REMOTE_LOGGING=False
      - AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=
      - AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=False
      - AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=
      - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
      - AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION=/opt/airflow/logs/dag_processor_manager/dag_processor_manager.log
      - AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN
      
      # Suppress warnings
      - PYTHONWARNINGS=ignore::UserWarning
      
      # Database Configuration
      - DATABASE_URL=postgresql://airflow:airflow@postgres:5432/messages
      - MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      - AIRFLOW_VAR_MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      
      # API Keys
      - GEMINI_API_KEY=${GEMINI_API_KEY}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - HYPERBROWSER_API_KEY=${HYPERBROWSER_API_KEY}
      
      # Hyperbrowser Configuration
      - HYPERBROWSER_PROFILE_ID=${HYPERBROWSER_PROFILE_ID:-}
      - HYPERBROWSER_MAX_STEPS=${HYPERBROWSER_MAX_STEPS:-25}
      
      # Workspace and File Paths
      - WORKSPACE_PATH=/opt/airflow/src
      - AUTOMA_EXTENSION_PATH=/opt/automa-extension/automa-extension.zip
      
      # Content Processing Configuration
      - FILTER_LINKS=${FILTER_LINKS:-true}
      - REQUIRED_DIGITS=${REQUIRED_DIGITS:-19}
      - EXCLUDE_TERMS=${EXCLUDE_TERMS:-}
      - NUM_MESSAGES=${NUM_MESSAGES:-10}
      - NUM_REPLIES=${NUM_REPLIES:-10}
      - NUM_RETWEETS=${NUM_RETWEETS:-10}
      
      # System Configuration
      - AIRFLOW_UID=${AIRFLOW_UID}
      - AIRFLOW_GID=${AIRFLOW_GID}
      - TZ=${DAG_TIMEZONE:-Africa/Nairobi}
      
      # DAG Scheduling Configuration
      - DAG_TIMEZONE=${DAG_TIMEZONE:-Africa/Nairobi}
      - DAG_SCHEDULE=${DAG_SCHEDULE:-0 16 * * 0}
      - DAG_START_DATE=${DAG_START_DATE:-2025-07-01}
      - AUTOMA_DAG_SCHEDULE=${AUTOMA_DAG_SCHEDULE:-*/5 * * * *}
      - AUTOMA_DAG_START_DATE=${AUTOMA_DAG_START_DATE:-2025-06-27}
      
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - airflow_logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./.env:/opt/airflow/.env
      - ./automa-extension:/opt/automa-extension
      - ./chrome_profile:/opt/airflow/chrome_persistent_profile
    user: "${AIRFLOW_UID}:${AIRFLOW_GID}"
    ports:
      - "8080:8080"
    command: >
      bash -c "
        echo '🌐 Starting Airflow webserver...' &&
        airflow webserver
      "
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  airflow-scheduler:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    container_name: airflow_scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
      mongodb:
        condition: service_healthy
    environment:
      # Core Airflow Configuration
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__PARALLELISM=8
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/messages
      - AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY}
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AIRFLOW__CORE__DEFAULT_TIMEZONE=${DAG_TIMEZONE:-Africa/Nairobi}
      - AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
      - AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=8
      - PYTHONPATH=/opt/airflow/src
      
      # Webserver Configuration
      - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
      - AIRFLOW__WEBSERVER__BASE_URL=http://0.0.0.0:8080
      - AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
      - AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
      - AIRFLOW__WEBSERVER__WORKER_REFRESH_BATCH_SIZE=1
      - AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=6000
      
      # Logging Configuration
      - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
      - AIRFLOW__LOGGING__REMOTE_LOGGING=False
      - AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=
      - AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=False
      - AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=
      - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
      - AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION=/opt/airflow/logs/dag_processor_manager/dag_processor_manager.log
      - AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN
      
      # Suppress warnings
      - PYTHONWARNINGS=ignore::UserWarning
      
      # Database Configuration
      - DATABASE_URL=postgresql://airflow:airflow@postgres:5432/messages
      - MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      - AIRFLOW_VAR_MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      
      # API Keys
      - GEMINI_API_KEY=${GEMINI_API_KEY}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - HYPERBROWSER_API_KEY=${HYPERBROWSER_API_KEY}
      
      # Hyperbrowser Configuration
      - HYPERBROWSER_PROFILE_ID=${HYPERBROWSER_PROFILE_ID:-}
      - HYPERBROWSER_MAX_STEPS=${HYPERBROWSER_MAX_STEPS:-25}
      
      # Workspace and File Paths
      - WORKSPACE_PATH=/opt/airflow/src
      - AUTOMA_EXTENSION_PATH=/opt/automa-extension/automa-extension.zip
      
      # Content Processing Configuration
      - FILTER_LINKS=${FILTER_LINKS:-true}
      - REQUIRED_DIGITS=${REQUIRED_DIGITS:-19}
      - EXCLUDE_TERMS=${EXCLUDE_TERMS:-}
      - NUM_MESSAGES=${NUM_MESSAGES:-10}
      - NUM_REPLIES=${NUM_REPLIES:-10}
      - NUM_RETWEETS=${NUM_RETWEETS:-10}
      
      # System Configuration
      - AIRFLOW_UID=${AIRFLOW_UID}
      - AIRFLOW_GID=${AIRFLOW_GID}
      - TZ=${DAG_TIMEZONE:-Africa/Nairobi}
      
      # DAG Scheduling Configuration
      - DAG_TIMEZONE=${DAG_TIMEZONE:-Africa/Nairobi}
      - DAG_SCHEDULE=${DAG_SCHEDULE:-0 16 * * 0}
      - DAG_START_DATE=${DAG_START_DATE:-2025-07-01}
      - AUTOMA_DAG_SCHEDULE=${AUTOMA_DAG_SCHEDULE:-*/5 * * * *}
      - AUTOMA_DAG_START_DATE=${AUTOMA_DAG_START_DATE:-2025-06-27}
      
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - airflow_logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./.env:/opt/airflow/.env
      - ./automa-extension:/opt/automa-extension
      - ./chrome_profile:/opt/airflow/chrome_persistent_profile
    user: "${AIRFLOW_UID}:${AIRFLOW_GID}"
    command: >
      bash -c "
        echo '⏰ Starting Airflow scheduler...' &&
        airflow scheduler
      "
    healthcheck:
      test: ["CMD-SHELL", "airflow jobs check --job-type SchedulerJob --hostname \"${HOSTNAME}\""]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: streamlit_app
    depends_on:
      postgres:
        condition: service_healthy
      mongodb:
        condition: service_healthy
    ports:
      - "8501:8501"
    volumes:
      - .:/app
      - ./.env:/app/.env
      - ./automa-extension:/app/automa-extension
    environment:
      - STREAMLIT_SERVER_ENABLE_CORS=false
      - STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false
      - PYTHONPATH=/app
      - STREAMLIT_SERVER_HEADLESS=true
      - HYPERBROWSER_API_KEY=${HYPERBROWSER_API_KEY}
      - HYPERBROWSER_PROFILE_ID=${HYPERBROWSER_PROFILE_ID:-}
      - HYPERBROWSER_MAX_STEPS=${HYPERBROWSER_MAX_STEPS:-25}
      - DATABASE_URL=postgresql://airflow:airflow@postgres:5432/messages
      - MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      - TZ=${DAG_TIMEZONE:-Africa/Nairobi}
      - DISPLAY=:99
      - WORKSPACE_PATH=/app/src
      - FILTER_LINKS=${FILTER_LINKS:-true}
      - REQUIRED_DIGITS=${REQUIRED_DIGITS:-19}
      - EXCLUDE_TERMS=${EXCLUDE_TERMS:-}
      - NUM_MESSAGES=${NUM_MESSAGES:-10}
      
      # Suppress warnings
      - PYTHONWARNINGS=ignore::UserWarning
    cap_add:
      - SYS_ADMIN
    command: >
      bash -c "
        echo '🖥️ Starting virtual display...' &&
        Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
        export DISPLAY=:99 &&
        echo '🚀 Starting Streamlit app...' &&
        streamlit run src/streamlit/app.py --server.address=0.0.0.0 --server.port=8501 --server.fileWatcherType=auto
      "
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8501/_stcore/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  # Chrome GUI Service (DevContainer)
  chrome-gui:
    build:
      context: .devcontainer
      dockerfile: Dockerfile
    container_name: chrome_gui
    ports:
      - "6080:6080"   # noVNC web interface
      - "9222:9222"   # Chrome DevTools Protocol
    volumes:
      - .:/workspace
      - ./chrome_profile:/workspace/chrome_profile
    environment:
      - DISPLAY=:99
      - VNC_PASSWORD=secret
    cap_add:
      - SYS_ADMIN
    security_opt:
      - seccomp:unconfined
    shm_size: 2g
    ipc: host
    command: ["/usr/local/bin/start-gui.sh"]
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:6080"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  # Database Backup Service
  db-backup:
    image: alpine:latest
    container_name: db_backup
    depends_on:
      postgres:
        condition: service_healthy
      mongodb:
        condition: service_healthy
    volumes:
      - ./backups:/backups
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_HOST=postgres
      - POSTGRES_PORT=5432
      - POSTGRES_DB=messages
      - BACKUP_SCHEDULE=${BACKUP_SCHEDULE:-0 2 * * *}
      - PGPASSWORD=airflow
      - MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      - TZ=${DAG_TIMEZONE:-Africa/Nairobi}
    command: >
      sh -c "
        apk add --no-cache postgresql-client dcron mongodb-tools curl &&
        mkdir -p /backups &&
        echo '🗄️ Database backup service started' &&
        echo '$(echo \"${BACKUP_SCHEDULE:-0 2 * * *}\") /bin/sh -c \"\
          timestamp=$(date +%Y%m%d_%H%M%S); \
          echo \"📦 Starting backup at $timestamp\"; \
          pg_dump -U $POSTGRES_USER -h $POSTGRES_HOST -p $POSTGRES_PORT $POSTGRES_DB > /backups/postgres_$timestamp.sql; \
          mongodump --uri=$MONGODB_URI --out=/backups/mongodb_$timestamp; \
          find /backups -name \"*.sql\" -mtime +7 -delete; \
          find /backups -name \"mongodb_*\" -type d -mtime +7 -exec rm -rf {} + 2>/dev/null; \
          echo \"✅ Backup completed at $timestamp\"\"' > /etc/crontabs/root &&
        crond -f
      "
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    profiles:
      - backup

  # Nginx Reverse Proxy (Optional)
  nginx:
    image: nginx:alpine
    container_name: nginx_proxy
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/ssl:/etc/nginx/ssl:ro
    depends_on:
      - airflow-webserver
      - app
      - chrome-gui
    networks:
      - app_network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    profiles:
      - proxy

volumes:
  postgres_data:
    driver: local
  mongodb_data:
    driver: local
  airflow_logs:
    driver: local
  data:
    driver: local
  automa_logs:
    driver: local
  browser_logs:
    driver: local
  chrome_profile_data:
    driver: local

networks:
  app_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16/automa-extension.zip
      
      # Content Processing Configuration
      - FILTER_LINKS=${FILTER_LINKS:-true}
      - REQUIRED_DIGITS=${REQUIRED_DIGITS:-19}
      - EXCLUDE_TERMS=${EXCLUDE_TERMS:-}
      - NUM_MESSAGES=${NUM_MESSAGES:-10}
      - NUM_REPLIES=${NUM_REPLIES:-10}
      - NUM_RETWEETS=${NUM_RETWEETS:-10}
      
      # System Configuration
      - AIRFLOW_UID=${AIRFLOW_UID}
      - AIRFLOW_GID=${AIRFLOW_GID}
      - TZ=${DAG_TIMEZONE:-Africa/Nairobi}
      
      # DAG Scheduling Configuration
      - DAG_TIMEZONE=${DAG_TIMEZONE:-Africa/Nairobi}
      - DAG_SCHEDULE=${DAG_SCHEDULE:-0 16 * * 0}
      - DAG_START_DATE=${DAG_START_DATE:-2025-07-01}
      - AUTOMA_DAG_SCHEDULE=${AUTOMA_DAG_SCHEDULE:-*/5 * * * *}
      - AUTOMA_DAG_START_DATE=${AUTOMA_DAG_START_DATE:-2025-06-27}
      
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - airflow_logs:/opt/airflow/logs
      - ./airflow/plugins:/opt/airflow/plugins
      - ./.env:/opt/airflow/.env
      - ./automa-extension:/opt/automa-extension
      - ./chrome_profile:/opt/airflow/chrome_persistent_profile
    user: "${AIRFLOW_UID}:${AIRFLOW_GID}"
    command: >
      bash -c "
        echo '🚀 Setting up directories...' &&
        mkdir -p /opt/airflow/logs /opt/airflow/chrome_persistent_profile /opt/automa-extension &&
        chmod 777 /opt/airflow/logs /opt/airflow/chrome_persistent_profile  &&
        chmod 775 /opt/automa-extension &&
        echo '📊 Initializing Airflow database...' &&
        airflow db migrate &&
        echo '👤 Creating admin user...' &&
        airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin || echo 'User already exists' &&
        echo '✅ Airflow initialization completed successfully'
      "
    networks:
      - app_network
    restart: "no"

  airflow-webserver:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    container_name: airflow_webserver
    depends_on:
      airflow-init:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
      mongodb:
        condition: service_healthy
    environment:
      # Core Airflow Configuration
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__PARALLELISM=8
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/messages
      - AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY}
      - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AIRFLOW__CORE__DEFAULT_TIMEZONE=${DAG_TIMEZONE:-Africa/Nairobi}
      - AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
      - AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=8
      - PYTHONPATH=/opt/airflow/src
      
      # Webserver Configuration
      - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
      - AIRFLOW__WEBSERVER__BASE_URL=http://0.0.0.0:8080
      - AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
      - AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
      - AIRFLOW__WEBSERVER__WORKER_REFRESH_BATCH_SIZE=1
      - AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=6000
      
      # Logging Configuration
      - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
      - AIRFLOW__LOGGING__REMOTE_LOGGING=False
      - AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=
      - AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=False
      - AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=
      - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
      - AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION=/opt/airflow/logs/dag_processor_manager/dag_processor_manager.log
      - AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN
      
      # Suppress warnings
      - PYTHONWARNINGS=ignore::UserWarning
      
      # Database Configuration
      - DATABASE_URL=postgresql://airflow:airflow@postgres:5432/messages
      - MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      - AIRFLOW_VAR_MONGODB_URI=mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin
      
      # API Keys
      - GEMINI_API_KEY=${GEMINI_API_KEY}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - HYPERBROWSER_API_KEY=${HYPERBROWSER_API_KEY}
      
      # Hyperbrowser Configuration
      - HYPERBROWSER_PROFILE_ID=${HYPERBROWSER_PROFILE_ID:-}
      - HYPERBROWSER_MAX_STEPS=${HYPERBROWSER_MAX_STEPS:-25}
      
      # Workspace and File Paths
      - WORKSPACE_PATH=/opt/airflow/src
      - AUTOMA_EXTENSION_PATH=/opt/automa-extension