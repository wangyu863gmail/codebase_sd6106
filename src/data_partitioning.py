import psycopg2
from psycopg2 import sql
import yaml

class DataPartitioner:
    def __init__(self, config_path):
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        
        db_params = config['database']
        self.conn = psycopg2.connect(**db_params)
        self.cur = self.conn.cursor()

    # ... rest of the class implementation remains the same ...

# Usage example
# partitioner = DataPartitioner('../conf/config.yml')
# partitioner.create_parent_table()
# partitioner.create_date_partition(date(2023, 1, 1), date(2023, 2, 1))
# partitioner.create_instrument_partition('financial_data_2023_01', 'AAPL')
# data = [(...), (...), ...]  # List of tuples containing data to insert
# partitioner.insert_data(data)
# partitioner.close()