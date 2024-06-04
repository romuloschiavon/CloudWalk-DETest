#!/bin/sh

# Initialize the database
airflow db migrate

# Create default connections
airflow connections create-default-connections

# Create the airflow default user
airflow users create \
    --username admin \
    --password admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email admin@example.com

# Start the scheduler in the background
if [ "$1" = "scheduler" ]; then
  exec airflow scheduler
fi

# Start the webserver
if [ "$1" = "webserver" ]; then
  exec airflow webserver
fi

# Start the celeryworker
if [ "$1" = "celery" ]; then
  if [ "$2" = "worker" ]; then
    exec airflow celery worker
  fi
fi

exec "$@"
