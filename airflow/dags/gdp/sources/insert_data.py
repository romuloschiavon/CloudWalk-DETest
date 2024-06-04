import psycopg2
import os
import json
import gzip
from utils.logging import init_airflow_logging
from airflow.utils.dates import days_ago

class InsertData:
    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        self.logical_date = logical_date
        self.logging = init_airflow_logging()
        self.conn = psycopg2.connect(
            dbname='airflow',
            user='airflow',
            password='airflow',
            host='postgres'
        )
        self.cur = self.conn.cursor()


    def insert_pivot_data(self, data):
        self.logging.info('Inserting data into pivot table.')

        insert_query = """
        INSERT INTO pivot_gdp_report (name, iso3_code, "2019", "2020", "2021", "2022", "2023")
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        
        pivot_data = [
            (
                row['name'], row['iso3_code'],
                row.get('2019', 0), row.get('2020', 0), row.get('2021', 0),
                row.get('2022', 0), row.get('2023', 0)
            )
            for row in data
        ]

        self.cur.executemany(insert_query, pivot_data)
        self.conn.commit()
        self.logging.info('Data insertion into pivot table complete.')

    def pivot_report(self):
        self.logging.info('Starting pivot report generation.')

        query = """
        SELECT 
            c.id,
            c.name,
            c.iso3_code,
            SUM(CASE WHEN g.year = 2019 THEN g.value ELSE 0 END) / 1e9 AS "2019",
            SUM(CASE WHEN g.year = 2020 THEN g.value ELSE 0 END) / 1e9 AS "2020",
            SUM(CASE WHEN g.year = 2021 THEN g.value ELSE 0 END) / 1e9 AS "2021",
            SUM(CASE WHEN g.year = 2022 THEN g.value ELSE 0 END) / 1e9 AS "2022",
            SUM(CASE WHEN g.year = 2023 THEN g.value ELSE 0 END) / 1e9 AS "2023"
        FROM 
            country c
        LEFT JOIN 
            gdp g ON c.id = g.country_id
        WHERE 
            g.year BETWEEN 2019 AND 2023
        GROUP BY 
            c.id, c.name, c.iso3_code
        ORDER BY 
            c.id;
        """

        self.cur.execute(query)
        result = self.cur.fetchall()

        data = [dict(zip(["name", "iso3_code", "2019", "2020", "2021", "2022", "2023"], row[1:])) for row in result]

        self.insert_pivot_data(data)

        report_path_csv = os.path.join(self.airflow_home, 'dags', 'gdp', 'reports', 'gdp_pivot_report.csv.gz')
        report_path_json = os.path.join(self.airflow_home, 'dags', 'gdp', 'reports', 'gdp_pivot_report.json.gz')
        os.makedirs(os.path.dirname(report_path_csv), exist_ok=True)

        with gzip.open(report_path_csv, 'wt', encoding='UTF-8') as f:
            f.write("name,iso3_code,2019,2020,2021,2022,2023\n")
            for row in result:
                f.write(','.join(map(str, row[1:])) + '\n')

        with gzip.open(report_path_json, 'wt', encoding='UTF-8') as f:
            json.dump(data, f)

        self.logging.info('Pivot report generation complete.')
        return report_path_csv, report_path_json
    
    def close_connection(self):
        self.cur.close()
        self.conn.close()
        self.logging.info('Database connection closed.')

if __name__ == "__main__":
    logical_date = days_ago(0)
    inserter = InsertData(logical_date)
    try:
        report = inserter.pivot_report()
    finally:
        inserter.close_connection()