import json
import os
from utils.logging import init_airflow_logging
from airflow.utils.dates import days_ago

class GDPDataTransformer:
    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow/dags')
        self.logging = init_airflow_logging()
        self.logical_date = logical_date
        self.input_filepath = self.get_filepath(input=True)
        self.output_filepath = self.get_filepath(input=False)
        

    def get_filepath(self, input=True):
        year = self.logical_date.strftime('%Y')
        month = self.logical_date.strftime('%m')
        day = self.logical_date.strftime('%d')
        dir_path = os.path.join(self.airflow_home, 'dags', 'gdp', 'data', year, month, day)
        os.makedirs(dir_path, exist_ok=True)
        if input:
            return os.path.join(dir_path, 'gdp_data.json')
        else:
            return os.path.join(dir_path, 'transformed_gdp_data.json')
    

    def transform_gdp_data(self):
        self.logging.info('Starting data transformation.')
        with open(self.input_filepath, 'r') as f:
            data = json.load(f)

        transformed_data = []
        for record in data[1]:
            transformed_data.append({
                "country_name": record['country']['value'],
                "iso3_code": record['country']['id'],
                "year": record['date'],
                "value": record['value']
            })

        with open(self.output_filepath, 'w') as f:
            json.dump(transformed_data, f)
            
        self.logging.info('Data transformation complete.')
        
if __name__ == "__main__":
    logical_date = days_ago(0)
    transformer = GDPDataTransformer(logical_date)
    transformer.transform_gdp_data()
