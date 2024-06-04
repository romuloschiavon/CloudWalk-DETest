import requests
import json
import os
import gzip
from utils.logging import init_airflow_logging
from airflow.utils.dates import days_ago

class GDPDataExtractor:
    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        self.base_url = 'https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json'
        self.logical_date = logical_date
        self.filepath = self.get_filepath()
        self.logging = init_airflow_logging()

    def get_filepath(self):
        """Constructs the file path for storing the extracted data."""
        year = self.logical_date.strftime('%Y')
        month = self.logical_date.strftime('%m')
        day = self.logical_date.strftime('%d')
        dir_path = os.path.join(self.airflow_home, 'dags', 'gdp', 'data', 'bronze', year, month, day, )
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, 'gdp_data.json.gz')

    def extract_gdp_data(self):
        """Extracts GDP data from the World Bank API, processes it, and saves it as a gzipped JSON file."""
        self.logging.info("Initiliazing the extraction process")
        page = 1
        
        all_data = []
        
        while True:
            response = requests.get(f'{self.base_url}&page={page}&per_page=50')
            data = response.json()
            if not data or 'message' in data[0]:
                self.logging.error(f"Error in response: {data}")
                break

            if 'pages' not in data[0]:
                self.logging.error(f"Unexpected response format: {data}")
                break

            all_data.extend(data[1])
            self.logging.info(f"Extracted data from page {page}")
            
            self.logging.info(data)
            
            if page >= data[0]['pages']:
                break
            
            page += 1
          
        self.logging.info(all_data)  
        with gzip.open(self.filepath, 'wt', encoding='UTF-8') as f:
            json.dump(all_data, f)    
        self.logging.info("Extraction process completed")
            
if __name__ == "__main__":
    logical_date = days_ago(0)
    extractor = GDPDataExtractor(logical_date)
    extractor.extract_gdp_data()