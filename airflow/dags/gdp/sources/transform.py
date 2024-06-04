import json
import gzip
import os
from utils.logging import init_airflow_logging
from utils.get_file_path import get_file_path
from airflow.utils.dates import days_ago

class GDPDataTransformer:
    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow/dags')
        self.logging = init_airflow_logging()
        self.logical_date = logical_date
        self.input_filepath = get_file_path(self.logical_date, self.airflow_home, 'bronze', 'gdp_etl')
        self.output_filepath = get_file_path(self.logical_date, self.airflow_home, 'silver', 'gdp_etl')

    def transform_gdp_data(self):
        """Transforms the extracted GDP data by filtering relevant entries and saving the result as a gzipped JSON file."""
        self.logging.info("Initializing the transformation process")

        with gzip.open(self.input_filepath, 'rt', encoding='UTF-8') as f:
            data = json.load(f)

        # Deleting all entries that are not relevant to the assessment
        transformed_data = [
            {
                'country': entry['country']['value'],
                'country_code': entry['countryiso3code'],
                'date': entry['date'],
                'gdp': entry['value']
            }
            for entry in data if entry['value'] is not None
        ]

        with gzip.open(self.output_filepath, 'wt', encoding='UTF-8') as f:
            json.dump(transformed_data, f)

        self.logging.info("Transformation process completed")

if __name__ == "__main__":
    logical_date = days_ago(0)
    transformer = GDPDataTransformer(logical_date)
    transformer.transform_gdp_data()
