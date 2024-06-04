import os

def get_file_path(logical_date, base_path, data_type, etl_name, extension='json.gz'):
    """Constructs the file path for storing data based on logical date and data type."""
    year = logical_date.strftime('%Y')
    month = logical_date.strftime('%m')
    day = logical_date.strftime('%d')
    dir_path = os.path.join(base_path, 'data', etl_name, data_type, year, month, day)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f'{etl_name}_data.{extension}')
