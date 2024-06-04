# CloudWalk-DETest
Data Engineer - Technical Assessment.

## Installation
To execute this application, the following installations are required:
- [Docker & Docker Compose](https://docs.docker.com/manuals/)

## Directory Structure
This project contains the following directory structure and files:
```
airflow/
├── dags/
│ ├── gdp/
│ │ ├── drop_tables.py # DAG to drop GDP and Country tables from the database if they exist.
│ │ ├── gdp_etl_dag.py # Main DAG that performs the ETL process for GDP data.
│ │ ├── generate_report.py # DAG to generate a pivot report for the last 5 years of GDP data.
│ │ ├── sources/
│ │ │ ├── create_gdp_table.py # Script to create necessary tables in the database for storing GDP data.
│ │ │ ├── create_pivot_table.py # Script to create the pivot table in the database.
│ │ │ ├── extract.py # Script to extract GDP data from the World Bank API.
│ │ │ ├── insert_data.py # Script to insert transformed data into the pivot table and generate reports.
│ │ │ ├── load.py # Script to load transformed GDP data into the database.
│ │ │ ├── transform.py # Script to transform extracted GDP data.
│ ├── utils/
│ │ ├── logging.py # Utility for initializing logging.
├── docker/
│ ├── Dockerfile # Dockerfile for building the Airflow image.
│ ├── docker-compose.yaml # Docker Compose file to set up the Airflow environment.
│ ├── entrypoint.sh # Entrypoint script for initializing Airflow.
│ ├── Makefile # Makefile for managing Docker Compose commands.
│ ├── requirements.txt # Python dependencies for the Airflow environment.
│ ├── variables.env # Environment variables for Airflow configuration.
├── logs/ # Directory for Airflow logs.
├── plugins/ # Directory for Airflow plugins.
```
## Cloning repository
To get started, you need to clone the repository to your local machine. Use the following command:
```bash
git clone https://github.com/romuloschiavon/CloudWalk-DETest.git
```
Navigate to the project directory:
```bash
cd CloudWalk-DETest
```

## Set Up Airflow
To reach as many users as possible, you can use either `make` or `Docker` commands directly in the terminal (a.k.a bash) to run Airflow and the Data Engineer assessment.

Feel free to choose the method you are most comfortable with.

Navigate to the folder containing the Dockerfile and `docker-compose.yaml`. If you are in the root folder, simply run:
```
cd airflow/docker
```

Note: I chose to use Redis because CeleryExecutor has significantly more capabilities than LocalExecutor.

<details>
<summary>Using Docker Commands</summary>

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
Note that if you run the Docker container without using detached mode, your terminal will be locked. Press Ctrl+C to stop Docker.

If you prefer not to watch logs in the terminal, use the detached `-d` argument:
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

#### Checking Logs
If you are running in detached mode and want to view the Docker logs, run:
```bash
docker-compose logs -f
```

</details>

<details>
<summary>Using Make</summary>

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
Note that if you run the Docker container without using detached mode, your terminal will be locked. Press Ctrl+C to stop Docker.

If you prefer not to watch logs in the terminal, use the detached `-d` argument:
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

#### Checking Logs
If you are running in detached mode and want to view the Docker logs, run:
```bash
make logs
```

</details>
Please only choose one.

## Pipeline Extraction
Now your container is up and running and you can run the data extraction using Airflow. [You can access it clicking here](http://localhost:8080) or navigating to http://localhost:8080 in your browser.

### Log In to Airflow
Use the following credentials to log in:
- user: `admin`
- password: `admin`

These credentials are set up during the initial configuration of Airflow in the entrypoint.sh script.

### Initial Setup
If you are running the pipeline for the first time or if you want to reset the database, you should execute the drop_tables DAG first.

This DAG will drop any existing tables related to the GDP data to ensure a clean slate for your data extraction process.

### Running the ETL Process
To ensure that the report data can be refreshed at any time, the `generate_report` DAG is automatically triggered when the `gdp_etl_dag` succeeds. This allows for multiple runs per day if needed by business teams, without requiring the entire pipeline to be rerun to keep the data as fresh as possible.

### Important Step
Before running the `gdp_etl_dag`, make sure the `generate_report` DAG is turned ON. It will not run automatically unless it is activated.

Switch it to the "ON" position!!!

Once the `generate_report` DAG is activated, you can proceed to run the `gdp_etl_dag`, and you should have your data extracted, transformed, loaded and pivoted into a easily queriable table.

#### GDP_ETL_DAG details:
The gdp_etl_dag DAG consists of several tasks

Create Data Folder:
- A task that ensures the necessary directories for storing extracted and transformed data are created.

Create Tables:
- A task that creates the required tables in the PostgreSQL database if they do not already exist. This includes the country and gdp tables.

Extract Data:
- A task that extracts GDP data from the World Bank API. The data is saved as a gzipped JSON file in the bronze data directory.

Transform Data:
- A task that transforms the extracted GDP data, filtering and formatting it as needed. The transformed data is saved as a gzipped JSON file in the silver data directory.

Load Data:
- A task that loads the transformed GDP data into the PostgreSQL database.

Trigger Report Generation:
- After the ETL process completes, this task triggers another DAG (generate_report_dag) to create a pivot report of the last 5 years of GDP data.

## Viewing the Report
After running the ETL process and generating the report, you can view the pivoted GDP data directly from the PostgreSQL database.

Below is a simple query to select all data from the pivot table `pivot_gdp_report`.

### SQL Query to View the Pivoted Report
To view the report, you can use the following basic SQL query:

```sql
SELECT * FROM pivot_gdp_report;
```

This query retrieves all columns and rows from the `pivot_gdp_report` table, which contains the GDP data pivoted over the last 5 years.

### Important note
**Venezuela Data Exclusion:** Venezuela is not shown in the results because it does not have public GDP data available since 2014 in the World Bank API. This decision ensures the accuracy and relevance of the data presented in the reports.

**2023 with null values:** Since the World Bank API doesn't have the data for 2023, all values are expeceted to be 0.

### Example Output

The output of the query will look like this:

| id  | name       | iso3_code | 2019     | 2020     | 2021     | 2022     | 2023     |
|-----|------------|-----------|----------|----------|----------|----------|----------|
| 1   | Argentina  | ARG       | 447.75   | 385.74   | 487.90   | 631.13   | 0.00     |
| 2   | Brazil     | BRA       | 1445.89  | 1389.52  | 1600.72  | 1839.76  | 0.00     |
| ... | ...        | ...       | ...      | ...      | ...      | ...      | ...      |

- **id**: The unique identifier for each country.
- **name**: The name of the country.
- **iso3_code**: The ISO 3166-1 alpha-3 code for the country.
- **2019**: The GDP value for the year 2019 in billions.
- **2020**: The GDP value for the year 2020 in billions.
- **2021**: The GDP value for the year 2021 in billions.
- **2022**: The GDP value for the year 2022 in billions.
- **2023**: The GDP value for the year 2023 in billions.

### How to Run the Query

You can run the SQL query using any PostgreSQL client tool, such as:

- **psql**: The PostgreSQL command-line interface.
- **pgAdmin**: A popular web-based PostgreSQL management tool.
- **DBeaver**: A universal database management tool.

#### Using psql

If you are using `psql`, the PostgreSQL command-line interface, you can run the query as follows:

1. Open your terminal.
2. Connect to your PostgreSQL database:
   `psql -h postgres -U airflow -d airflow`
3. Enter your password when prompted.
4. Run the query:
   `SELECT * FROM pivot_gdp_report;`

This will display the pivoted GDP report in your terminal.

By following these steps, you can easily view and analyze the GDP report generated by your Airflow ETL pipeline.

### Reports Folder
The `airflow/dags/gdp/reports` directory contains the generated reports in both .json.gz and .csv.gz formats.

These reports provide the same results as the SQL query executed on the pivot_gdp_report table, allowing you to access the pivoted GDP data directly.

## Document Assumptions
The following assumptions were made in the development of this project:

- Audience Expertise: The intended readers and users of this code are assumed to be data engineers or data analysts with a solid understanding of SQL, ETL processes, and Airflow. They should be comfortable with implementing, modifying, and maintaining the codebase.
- Environment Setup: It is assumed that the user has a working Docker environment and basic knowledge of Docker and Docker Compose to set up and run the Airflow containers.
- Database Management: The user is expected to have a PostgreSQL database available and understand basic database management tasks such as running SQL queries, creating tables, and handling database connections.
- Data Pipeline Knowledge: The user should be familiar with data pipeline concepts and the role of ETL (Extract, Transform, Load) processes in managing data flows from external sources to a database.
- Airflow Familiarity: It is assumed that the user has prior experience with Apache Airflow, including setting up DAGs, configuring tasks, and using the Airflow web interface for monitoring and managing workflows.

## Design Decisions
The design of this project was guided by several key decisions to ensure flexibility, reusability, and ease of use for SQL users and developers. Below is a summary of these decisions:

- Access to Pivoted Table: 
    - Since the assessment did not specify how the pivoted table should be accessed, a range of access methods were provided. This includes SQL queries for direct access by SQL users and the generation of reports in both JSON and CSV formats, making the data available via different endpoints.
- Class Orientation: 
    - Classes were used instead of a purely object-oriented approach to enhance code reuse and modularity. This decision was based on the likelihood of code reuse in different parts of the project and to maintain a clean, organized structure.
SQL Script Detachment:
    - An improvement suggestion involves detaching the SQL script for creating the pivoted table into a separate .sql file. This would make the SQL script easily editable and maintainable, allowing for quick modifications without changing the main Python code.
- Connection Class:
    - To reduce code duplication and improve maintainability, a dedicated Connection class could be created. This class would handle all database connection operations, ensuring a single source of truth for connection management and making the codebase cleaner.
- Ease of Modification:
    - The use of classes makes the code more modular and easier to change. This approach also enhances readability, allowing users to quickly understand and modify specific parts of the code as needed.
- Logging:
    - Logging is implemented throughout the ETL process to provide transparency and ease of debugging. Each class initializes logging to track the progress and status of operations, ensuring that any issues can be quickly identified and resolved.
- Querying Multiple Countries:
    - Since the assessment included all the countries in the format `ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN`, it was decided to query all countries in a single request rather than querying each one individually. Although querying each country individually might be faster, it would require more queries to the database, which could be less efficient as databases generally do not handle excessive requests well.
- Bronze/Silver Directory Structure:
    - A bronze/silver directory structure was created, organized by year, month, and day, to better understand which day the pipeline was run. This structure helps in managing 
    the data lifecycle and ensures traceability of data processing stages.