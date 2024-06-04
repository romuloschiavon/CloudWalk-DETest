import psycopg2
from utils.logging import init_airflow_logging

class DatabaseConnection:
    def __init__(self):
        self.conn_params = {
            'dbname': 'airflow',
            'user': 'airflow',
            'password': 'airflow',
            'host': 'postgres'
        }
        self.conn = None
        self.logging = init_airflow_logging()

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.logging.info("Database connection established.")
            return self.conn
        except Exception as e:
            self.logging.error(f"Error connecting to the database: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                self.conn.close()
                self.logging.info("Database connection closed.")
            except Exception as e:
                self.logging.error(f"Error closing the database connection: {e}")
