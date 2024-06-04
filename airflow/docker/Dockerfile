# Official latest stable release of airflow
FROM apache/airflow:2.9.1

# Default airflow home variable
ENV AIRFLOW_HOME=/opt/airflow

# Install libs and dependencies
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Expose default port for Airflow Usage (https://localhost:8080)
USER airflow
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 8080

# Start airflow, initialize the database, start the webserver and scheduler, using entrypoints.
COPY ./entrypoint.sh /entrypoint.sh 

ENTRYPOINT ["/entrypoint.sh"]