import requests
import json
import os
import gzip
from utils.logging import init_airflow_logging
from utils.get_file_path import get_file_path
from airflow.utils.dates import days_ago

class GDPDataExtractor:
    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        self.base_url = 'https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json'
        self.logical_date = logical_date
        self.filepath = get_file_path(self.logical_date, self.airflow_home, 'bronze', 'gdp_etl')
        self.logging = init_airflow_logging()

    def extract_gdp_data(self):
        """Extracts GDP data from the World Bank API, processes it, and saves it as a gzipped JSON file."""
        self.logging.info("Initiliazing the extraction process")
        page = 1
        
        all_data = []
        
        # Run in a single request session, optimizing the connection
        with requests.Session() as session:
            while True:
                # Make a request to the World Bank API
                response = session.get(f'{self.base_url}&page={page}&per_page=50')
                if response.status_code != 200:
                    self.logging.error(f"Request failed: {response.status_code}")
                    break
                
                # Extract the JSON data from the response
                data = response.json()
                if not data or 'message' in data[0]:
                    self.logging.error(f"Error in response: {data}")
                    break
                
                # Check if the response contains the expected data
                if 'pages' not in data[0]:
                    self.logging.error(f"Unexpected response format: {data}")
                    break

                all_data.extend(data[1])
                self.logging.info(f"Extracted data from page {page}")

                # Break the loop if all pages have been processed
                if page >= data[0]['pages']:
                    break

                page += 1
        
        # Save the extracted data to a gzipped JSON file
        with gzip.open(self.filepath, 'wt', encoding='UTF-8') as f:
            json.dump(all_data, f)    
        self.logging.info("Extraction process completed")
            
if __name__ == "__main__":
    logical_date = days_ago(0)
    extractor = GDPDataExtractor(logical_date)
    extractor.extract_gdp_data()