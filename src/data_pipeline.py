from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from data_collection_fdata import DataCollector
from data_processing import DataProcessor
from data_partitioning import DataPartitioner
import yaml

def collect_and_process_data(**kwargs):
    config_path = '../conf/config.yml'
    
    # Initialize components
    collector = DataCollector(config_path)
    processor = DataProcessor(config_path)
    partitioner = DataPartitioner(config_path)

    # Get yesterday's date
    yesterday = kwargs['execution_date'].date() - timedelta(days=1)
    
    try:
        # ... rest of the function implementation remains the same ...

    finally:
        partitioner.close()

# Load configuration
with open('../conf/config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Define the DAG
default_args = config['airflow']['default_args']

dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='A DAG to collect and process financial and sentiment data',
    schedule_interval=timedelta(days=1),
)

process_task = PythonOperator(
    task_id='collect_and_process_data',
    python_callable=collect_and_process_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies (if any)
# process_task >> next_task