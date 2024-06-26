import os
import json
import gzip
from utils.logging import init_airflow_logging
from utils.get_file_path import get_report_file_path
from airflow.utils.dates import days_ago
from utils.database_connection import DatabaseConnection


class InsertPivotData:

    def __init__(self, logical_date=days_ago(0)):
        self.airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        self.logical_date = logical_date
        self.logging = init_airflow_logging()
        self.json_report_path = get_report_file_path(logical_date, self.airflow_home, '', 'gdp', 'json.gz')
        self.csv_report_path = get_report_file_path(logical_date, self.airflow_home, '', 'gdp', 'csv.gz')

    def insert_pivot_data(self, conn, data):
        """Inserts data into the pivot_gdp_report table using batch insertion."""
        self.logging.info('Inserting data into pivot table.')

        # Insert query for the pivot table with a conflict resolution clause
        insert_query = """
        INSERT INTO pivot_gdp_report (name, iso3_code, "2019", "2020", "2021", "2022", "2023")
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (iso3_code) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            "2019" = EXCLUDED."2019",
            "2020" = EXCLUDED."2020",
            "2021" = EXCLUDED."2021",
            "2022" = EXCLUDED."2022",
            "2023" = EXCLUDED."2023";
        """
        # Prepare the data for insertion (Pivot)
        pivot_data = [(row['name'], row['iso3_code'], row.get('2019', 0),
                       row.get('2020', 0), row.get('2021', 0),
                       row.get('2022', 0), row.get('2023', 0)) for row in data]

        # Insert the data into the pivot table, using executemany for batch insertion
        with conn.cursor() as cur:
            cur.executemany(insert_query, pivot_data)
        conn.commit()
        self.logging.info('Data insertion into pivot table complete.')

    def pivot_report(self):
        """Queries the database for pivoted GDP data, inserts the data into the pivot table, and generates CSV and JSON reports."""
        self.logging.info('Starting pivot report generation.')

        # Query to pivot the GDP data
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
            c.id DESC;
        """

        with DatabaseConnection() as conn:
            # Execute the query and fetch the results
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()

            # Prepare the data for insertion into the pivot table
            data = [
                dict(
                    zip([
                        "name", "iso3_code", "2019", "2020", "2021", "2022", "2023"
                    ], row[1:])) for row in result
            ]
            self.insert_pivot_data(conn, data)

        data_sorted = sorted(data, key=lambda x: x["name"])
        del(data)
        
        # Save the pivot report as a gzipped CSV and gzipped JSON file
        os.makedirs(os.path.dirname(self.json_report_path), exist_ok=True)

        with gzip.open(self.csv_report_path, 'wt', encoding='UTF-8') as f:
            f.write("name,iso3_code,2019,2020,2021,2022,2023\n")
            for row in data_sorted:
                f.write(','.join(map(str, [row["name"], row["iso3_code"], row["2019"], row["2020"], row["2021"], row["2022"], row["2023"]])) + '\n')

        with gzip.open(self.json_report_path, 'wt', encoding='UTF-8') as f:
            json.dump(data_sorted, f)

        return self.logging.info('Pivot report generation complete.')


if __name__ == "__main__":
    logical_date = days_ago(0)
    inserter = InsertPivotData(logical_date)
    inserter.pivot_report()
