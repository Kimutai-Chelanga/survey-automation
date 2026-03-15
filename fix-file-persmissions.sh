cd /opt/final-automation

# This will create /root/Downloads as root, then start everything
docker compose -f docker-compose.prod.yml --env-file .env.production down && \
docker compose -f docker-compose.prod.yml --env-file .env.production up -d postgres mongodb && \
sleep 30 && \
docker compose -f docker-compose.prod.yml --env-file .env.production run --rm --user root airflow-init bash -c "
  mkdir -p /root/Downloads /opt/airflow/logs /workspace/chrome_profiles /workspace/recordings /opt/airflow/workspace/downloads /workspace/downloads /opt/airflow/backups /opt/airflow/workflows_export /opt/airflow/prompt_backups &&
  chmod -R 777 /root/Downloads /opt/airflow/logs /workspace/chrome_profiles /workspace/recordings /opt/airflow/workspace/downloads /workspace/downloads /opt/airflow/backups /opt/airflow/workflows_export /opt/airflow/prompt_backups &&
  su -c 'airflow db migrate' airflow &&
  su -c 'airflow users create --username admin --password kimu --firstname Admin --lastname User --role Admin --email admin@localhost' airflow || echo 'User exists'
" && \
docker compose -f docker-compose.prod.yml --env-file .env.production up -d && \
sleep 10 && \
echo "Verifying /root/Downloads..." && \
docker exec airflow_scheduler ls -la /root/Downloads
