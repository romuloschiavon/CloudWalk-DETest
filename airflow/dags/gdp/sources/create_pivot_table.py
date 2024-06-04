import psycopg2
from utils.logging import init_airflow_logging

class GDPPivotTableCreator:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname='airflow',
            user='airflow',
            password='airflow',
            host='postgres'
        )
        self.cur = self.conn.cursor()
        self.logging = init_airflow_logging()

    def create_pivot_table(self):
        self.logging.info('Creating pivot table in PostgreSQL.')

        create_table_query = """
        CREATE TABLE IF NOT EXISTS pivot_gdp_report (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            iso3_code CHAR(3),
            "2019" NUMERIC,
            "2020" NUMERIC,
            "2021" NUMERIC,
            "2022" NUMERIC,
            "2023" NUMERIC
        );
        """

        self.cur.execute(create_table_query)
        self.conn.commit()

        self.logging.info('Pivot table creation complete.')

    def close_connection(self):
        self.cur.close()
        self.conn.close()
        self.logging.info("Database connection closed")

if __name__ == "__main__":
    creator = GDPPivotTableCreator()
    try:
        creator.create_pivot_table()
    finally:
        creator.close_connection()
