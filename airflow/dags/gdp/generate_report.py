from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from gdp.sources.create_pivot_table import GDPPivotTableCreator
from gdp.sources.insert_data import InsertPivotData 
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1
}

@dag(
    default_args=default_args,
    description='Generate a pivot report for the last 5 years of GDP data',
    schedule_interval=None,  # This DAG will be triggered by another DAG
    tags=['gdp', 'report']
)
def generate_report_dag():

    @task
    def generate_report(**kwargs):
        logical_date = kwargs['logical_date']
        inserter = InsertPivotData(logical_date)
        report_paths = inserter.pivot_report()
        return report_paths

    @task
    def create_pvt_table():
        creator = GDPPivotTableCreator()
        creator.create_pivot_table()

    start_dag = EmptyOperator(
        task_id='start_dag'
    )
    
    end_dag = EmptyOperator(
        task_id="end_dag"
    )

    start_dag >> create_pvt_table() >> generate_report() >> end_dag

report_dag = generate_report_dag()
