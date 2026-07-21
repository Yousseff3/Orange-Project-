from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

config = {
    "csv_input_dir": "/app/csv/inputs/",
    "csv_backup_dir": "/app/csv/backups/"
}

# Backup CSV files (idempotent)
def backup_csv_files(**kwargs):
    """Backup CSV files before processing."""
    os.makedirs(config['csv_backup_dir'], exist_ok=True)

    for csv_file in glob(os.path.join(config['csv_input_dir'], '*.csv')):
        try:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            backup_name = f"{timestamp}_{os.path.basename(csv_file)}"
            backup_path = os.path.join(config['csv_backup_dir'], backup_name)

            shutil.copy2(csv_file, backup_path)
            print(f"Backed up {csv_file} to {backup_path}")

        except Exception as e:
            print(f"Failed to backup {csv_file}: {str(e)}")

dag = DAG(
    'csv_to_kafka',
    default_args=default_args,
    description='Process CSV files to Kafka and move processed files',
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
)

backup_task = PythonOperator(
    task_id='backup_csv_files',
    python_callable=backup_csv_files,
    dag=dag,
)

spark_job = SparkSubmitOperator(
    task_id='process_csv_to_kafka',
    application='/app/mypy/preprocessproduce.py',  
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5',
        "spark.cores.max": "3",
        "spark.executor.memory": "4g",
    },
    dag=dag,
)


backup_task >> spark_job
