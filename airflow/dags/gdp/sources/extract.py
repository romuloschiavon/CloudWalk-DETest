import requests
import json
import os
from utils.logging import init_airflow_logging
from airflow.utils.dates import days_ago

class GDPDataExtractor:
    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        self.url = 'https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json&page=1&per_page=50'
        self.logical_date = logical_date
        self.filepath = self.get_filepath()
        self.logging = init_airflow_logging()

    def get_filepath(self):
        year = self.logical_date.strftime('%Y')
        month = self.logical_date.strftime('%m')
        day = self.logical_date.strftime('%d')
        dir_path = os.path.join(self.airflow_home, 'dags', 'gdp', 'data', year, month, day)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, 'gdp_data.json')

    def extract_gdp_data(self):
        self.logging.info("Initiliazing the extraction process")
        response = requests.get(self.url)
        data = response.json()
        with open(self.filepath, 'w') as f:
            json.dump(data, f)
            
        self.logging.info("Extraction process completed")
            
if __name__ == "__main__":
    logical_date = days_ago(0)
    extractor = GDPDataExtractor(logical_date)
    extractor.extract_gdp_data()