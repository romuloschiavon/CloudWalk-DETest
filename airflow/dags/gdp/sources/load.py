import os
import json
import gzip
from utils.logging import init_airflow_logging
from airflow.utils.dates import days_ago
from utils.database_connection import DatabaseConnection

class GDPDataLoader:
    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        self.logical_date = logical_date
        self.filepath = self.get_filepath()
        self.logging = init_airflow_logging()

    def get_filepath(self):
        """Constructs the file path for loading the transformed data."""
        year = self.logical_date.strftime('%Y')
        month = self.logical_date.strftime('%m')
        day = self.logical_date.strftime('%d')
        dir_path = os.path.join(self.airflow_home, 'dags', 'gdp', 'data', 'silver', year, month, day)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, 'transformed_gdp_data.json.gz')
    
    def load_gdp_data(self):
        """Loads transformed GDP data from a gzipped JSON file into the database, ensuring foreign key relationships are maintained."""
        self.logging.info('Starting data load to PostgreSQL.')

        with gzip.open(self.filepath, 'rt', encoding='UTF-8') as f:
            data = json.load(f)

        countries = {}

        with DatabaseConnection() as conn:
            with conn.cursor() as cur:
                country_insert_query = """
                INSERT INTO country (name, iso3_code) 
                VALUES (%s, %s)
                ON CONFLICT (iso3_code) DO NOTHING
                RETURNING id, iso3_code;
                """
                gdp_insert_query = """
                INSERT INTO gdp (country_id, year, value) 
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
                """

                for record in data:
                    country_name = record['country']
                    iso3_code = record['country_code']
                    year = record['date']
                    value = record['gdp']

                    if iso3_code not in countries:
                        cur.execute(country_insert_query, (country_name, iso3_code))
                        country_id = cur.fetchone()[0]
                        countries[iso3_code] = country_id
                    else:
                        country_id = countries[iso3_code]

                    cur.execute(gdp_insert_query, (country_id, year, value))

            conn.commit()
        
        self.logging.info('Data load to PostgreSQL complete.')

if __name__ == "__main__":
    logical_date = days_ago(0)
    loader = GDPDataLoader(logical_date)
    loader.load_gdp_data()
