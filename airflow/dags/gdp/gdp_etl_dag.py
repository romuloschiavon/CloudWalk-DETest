from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from gdp.sources.create_gdp_table import GDPDataTableCreator
from gdp.sources.extract import GDPDataExtractor
from gdp.sources.transform import GDPDataTransformer
from gdp.sources.load import GDPDataLoader
from datetime import datetime, timedelta

docs = """
# Extract GDP ETL
This DAG performs an ETL process for GDP data. It extracts data from the World Bank API, transforms the data, and then loads it into a PostgreSQL database.

## Steps
1. Create data folder
2. Create the Postgres tables
3. Extract data Download GDP data in JSON format from World Bank API and store it in a gzip file.
4. Transform the data from the extracted file and store it in a new gzip file.
5. Load the transformed data into a PostgreSQL database.

### Author:
- Rômulo E. M. Schiavon
- Last update at: 2023-06-03
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'docs_md': docs,
    'catchup': False,
    'max_active_runs': 1
}

@dag(
    'Extract_Transform_Load_GDP_Data',
    default_args=default_args,
    description='ETL for extracting, transforming, and loading GDP data into a PostgreSQL database',
    schedule_interval=timedelta(days=1),
    tags=['gdp', 'etl', 'cloudwalk']
)
def gdp_etl_dag():
    @task
    def extract_data(**kwargs):
        logical_date = kwargs['logical_date']
        extractor = GDPDataExtractor(logical_date)
        extractor.extract_gdp_data()

    @task
    def transform_data(**kwargs):
        logical_date = kwargs['logical_date']
        transformer = GDPDataTransformer(logical_date)
        transformer.transform_gdp_data()

    @task
    def load_data(**kwargs):
        logical_date = kwargs['logical_date']
        loader = GDPDataLoader(logical_date)
        loader.load_gdp_data()

    @task
    def create_tables():
        creator = GDPDataTableCreator()
        creator.create_tables()
        
    start_dag = DummyOperator(
        task_id='start_dag'
    )
    
    end_dag = DummyOperator(
        task_id="end_dag"
    )
    
    trigger_generate_report = TriggerDagRunOperator(
        task_id='trigger_generate_report',
        trigger_dag_id='generate_report_dag'
    )
    
    start_dag  >> create_tables() >> extract_data() >> transform_data() >> load_data() >> trigger_generate_report >> end_dag

dag = gdp_etl_dag()