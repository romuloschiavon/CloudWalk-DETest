from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
from utils.logging import init_airflow_logging

# Initialize logging
logging = init_airflow_logging()

def drop_tables():
    logging.info("Connecting to the database")
    conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres'
    )
    cur = conn.cursor()
    logging.info("Dropping gdp and country tables if they exist")
    cur.execute("DROP TABLE IF EXISTS gdp CASCADE;")
    cur.execute("DROP TABLE IF EXISTS country CASCADE;")
    cur.execute("DROP TABLE IF EXISTS pivot_gdp_report CASCADE;")
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Tables dropped successfully")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'drop_gdp_etl_tables',
    default_args=default_args,
    description='Drop GDP and Country tables from the database if they exist.',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['gdp', 'drop', 'tables'],
    catchup=False
)

drop_tables_task = PythonOperator(
    task_id='drop_tables',
    python_callable=drop_tables,
    dag=dag,
)

drop_tables_task
