from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from gdp.sources.create_tables import GDPDataTableCreator
from gdp.sources.extract import GDPDataExtractor
from gdp.sources.transform import GDPDataTransformer
from gdp.sources.load import GDPDataLoader
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(logical_date):
    extractor = GDPDataExtractor(logical_date)
    extractor.extract_gdp_data()

def transform_data(logical_date):
    transformer = GDPDataTransformer(logical_date)
    transformer.transform_gdp_data()

def load_data(logical_date):
    loader = GDPDataLoader(logical_date)
    loader.load_gdp_data()
    
def create_tables():
    creator = GDPDataTableCreator()
    creator.create_tables()
    creator.close_connection()

dag = DAG(
    'Extract_Transform_Load_GDP_Data',
    default_args=default_args,
    description='ETL for extracting, transforming, and loading GDP data into a PostgreSQL database.',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['gdp', 'etl'],
    catchup=False
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

creating_tables = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag
)

extraction = PythonOperator(
    task_id='extract_gdp_data',
    python_callable=extract_data,
)

transformation = PythonOperator(
    task_id='transform_gdp_data',
    python_callable=transform_data,
)

loading = PythonOperator(
    task_id='load_gdp_data',
    python_callable=load_data,
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

start_dag >> creating_tables >> extraction >> transformation >> loading >> end_dag
