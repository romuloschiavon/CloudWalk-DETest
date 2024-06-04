import psycopg2
from utils.logging import init_airflow_logging

class GDPDataTableCreator:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname='airflow',
            user='airflow',
            password='airflow',
            host='postgres'
        )
        self.cur = self.conn.cursor()
        self.logging = init_airflow_logging()

    def create_tables(self):
        """Creates the country and gdp tables in the database if they do not exist."""
        self.logging.info("Checking and creating tables if they do not exist")
        create_country_table = """
        CREATE TABLE IF NOT EXISTS country (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            iso3_code VARCHAR(3)
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
        self.cur.execute(create_country_table)
        self.cur.execute(create_gdp_table)
        self.conn.commit()
        self.logging.info("Tables created")

    def close_connection(self):
        self.cur.close()
        self.conn.close()
        self.logging.info("Database connection closed")

if __name__ == "__main__":
    creator = GDPDataTableCreator()
    try:
        creator.create_tables()
    finally:
        creator.close_connection()
