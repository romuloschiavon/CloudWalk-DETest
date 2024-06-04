# CloudWalk-DETest
Data Engineer - Technical Assestment

## Installation
To execute this application, the following installations are required:
- [Docker & Docker Compose](https://docs.docker.com/manuals/)

## Running Airflow
To reach as many users as possible, you can use either `make` or `Docker` directly in terminal (a.k.a bash) to run Airflow and the Data Engineer assessment.

Feel free to choose the method you are most comfortable with.

Navigate to the folder containing the Dockerfile and docker-compose.yaml. If you are in the root folder, simply run:
```
cd airflow/docker
```

Note: I chose to use Redis because CeleryExecutor has significantly more capabilities than LocalExecutor.

### Using Docker Commands
#### Building
To build the Airflow network, Airflow, PostgreSQL, and Redis containers, run:
```bash
docker compose build
```

#### Running
Then, run the containers using:
```bash
docker compose up
```
If you prefer not to watch logs in the terminal, use the detachment `-d` argument:
```bash
docker compose up -d
```

#### Stopping/Removing Docker Containers
To stop the containers, run:
```bash
docker compose stop
```

To stop and remove the containers, run:
```bash
docker compose down
```

#### Checking logs
If you are running in detached mode and want to view the Docker logs, run:
```bash
docker-compose logs -f
```


### Using Make
#### Building
To build the Airflow network, Airflow, PostgreSQL, and Redis containers, run:
```bash
make build
```

#### Running
Then, run the containers using:
```bash
make up
```
If you prefer not to watch logs in the terminal, use the detachment `-d` argument:
```bash
make up-detached
```

#### Stopping/Removing Docker Containers
To stop the containers, run:
```bash
make stop
```
To stop and remove the containers, run:
```bash
make down
```

#### Checking logs
If you are running in detached mode and want to view the Docker logs, run:
```bash
make logs
```

## Pipeline Extraction
The pipeline can be run using [airflow](https://localhost:8080).

- user: `admin`
- password: `admin`

If the database was previously created, please execute the drop_tables DAG first.