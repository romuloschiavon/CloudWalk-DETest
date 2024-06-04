from utils.logging import init_airflow_logging
from utils.database_connection import DatabaseConnection

class GDPDataTableCreator:
    def __init__(self):
        self.logging = init_airflow_logging()

    def create_tables(self):
        """Creates the country and gdp tables in the database if they do not exist."""
        self.logging.info("Checking and creating tables if they do not exist")
        create_country_table = """
        CREATE TABLE IF NOT EXISTS country (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            iso3_code VARCHAR(3) UNIQUE
        );
        """
        create_gdp_table = """
        CREATE TABLE IF NOT EXISTS gdp (
            id SERIAL PRIMARY KEY,
            country_id INTEGER NOT NULL REFERENCES country(id),
            year INTEGER NOT NULL,
            value FLOAT
        );
        """
        try:
            with DatabaseConnection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_country_table)
                    cur.execute(create_gdp_table)
                conn.commit()
            self.logging.info("Tables created")
        except Exception as e:
            self.logging.error(f"Error creating tables: {e}")
            raise

if __name__ == "__main__":
    creator = GDPDataTableCreator()
    creator.create_tables()
