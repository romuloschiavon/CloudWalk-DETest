from utils.logging import init_airflow_logging
from utils.database_connection import DatabaseConnection

class GDPPivotTableCreator:
    def __init__(self):
        self.logging = init_airflow_logging()

    def create_pivot_table(self):
        """Creates the pivot_gdp_report table in the database if it does not exist."""
        self.logging.info('Creating pivot table in PostgreSQL.')

        create_table_query = """
        CREATE TABLE IF NOT EXISTS pivot_gdp_report (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            iso3_code CHAR(3) UNIQUE,
            "2019" NUMERIC,
            "2020" NUMERIC,
            "2021" NUMERIC,
            "2022" NUMERIC,
            "2023" NUMERIC
        );
        """

        try:
            with DatabaseConnection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_query)
                conn.commit()
            self.logging.info('Pivot table creation complete.')
        except Exception as e:
            self.logging.error(f"Error creating pivot table: {e}")
            raise

if __name__ == "__main__":
    creator = GDPPivotTableCreator()
    creator.create_pivot_table()
