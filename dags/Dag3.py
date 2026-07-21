from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
import math
import os
import xml.etree.ElementTree as ET
import json
import shutil
import gzip
from glob import glob
import time
import socket 
import pandas 
import numpy 


s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
ip=s.getsockname()[0]
s.close()

default_args = {
    'owner': 'youssef',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 13),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'xml_to_kafka_pipeline',
    default_args=default_args,
    description='Process XML to JSON and stream to Kafka',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1
)

config = {
    # GZIP paths
    'gzip_input_dir': '/app/gzip/gzipinput',
    'gzip_complete_dir': '/app/gzip/gzipcomplet',
    'gzip_backup_dir': '/app/gzip/gzipbackup',
    
    # XML paths
    'xml_input_dir': '/app/gzip/xmlcoming',
    'xml_processed_dir': '/app/gzip/xmldone',
    'xml_backup_dir': '/app/gzip/xmlbackup',
    
    # JSON paths
    'json_output_dir': '/app/gzip/jsoncoming',
    'json_processed_dir': '/app/gzip/jsondone',
    'json_backup_dir': '/app/gzip/jsonbackup',
    
    # Kafka config
    'kafka_bootstrap': 'kafka:9092',
    'kafka_topic': 'xmlt',
    
    # XML namespace
    'namespace': {'ns': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec'},
    
    # Retention
    'days_to_keep': 7
}

def ensure_directories_exist():
    """Ensure all required directories exist"""
    for dir_path in [
        config['gzip_input_dir'], config['gzip_complete_dir'], config['gzip_backup_dir'],
        config['xml_input_dir'], config['xml_processed_dir'], config['xml_backup_dir'],
        config['json_output_dir'], config['json_processed_dir'], config['json_backup_dir']
    ]:
        os.makedirs(dir_path, exist_ok=True)

def process_gzip_files(**kwargs):
    """Extract XML files from gzip archives"""
    ensure_directories_exist()
    processed_count = 0
    
    gz_files = glob(os.path.join(config['gzip_input_dir'], '*.gz'))
    
    if not gz_files:
        print("No GZIP files found to process")
        return 0

    for gz_path in gz_files:
        try:
            filename = os.path.basename(gz_path)
            
            # Handle .xml.gz files properly
            if filename.endswith('.xml.gz'):
                xml_filename = filename[:-3]  # Remove .gz
            else:
                xml_filename = filename.replace('.gz', '.xml')
            
            xml_path = os.path.join(config['xml_input_dir'], xml_filename)
            
            # Extract XML
            with gzip.open(gz_path, 'rb') as f_in:
                with open(xml_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Move processed gz
            completed_path = os.path.join(config['gzip_complete_dir'], filename)
            shutil.move(gz_path, completed_path)
            
            # Create timestamped backup
            backup_filename = os.path.basename(filename)  # Keep original name
            backup_path = os.path.join(config['gzip_backup_dir'], backup_filename)

	    # Handle duplicates by adding a counter
            counter = 1
            while os.path.exists(backup_path):
                base, ext = os.path.splitext(backup_filename)
                backup_path = os.path.join(config['gzip_backup_dir'], f"{base}_{counter}{ext}")
                counter += 1
            shutil.copy2(completed_path, backup_path)
            
            processed_count += 1
            print(f"Processed {filename} → {xml_filename}")
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            continue
    
    return processed_count

def process_xml_files(**kwargs):
    """Convert XML files to timestamped JSON with robust handling"""
    ensure_directories_exist()
    processed_count = 0
    combined_data = []
    
    # Namespace definition
    NS = {'ns': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.435#measCollec'}
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_filename = f"combined_{timestamp}.json"
    combined_json_path = os.path.join(config['json_output_dir'], combined_filename)
    
    for xml_path in glob(os.path.join(config['xml_input_dir'], '*.xml')):
        try:
            filename = os.path.basename(xml_path)
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # Get file-level metadata
            file_header = root.find('ns:fileHeader', NS)
            begin_time = file_header.find('ns:measCollec', NS).get('beginTime')
            
            # Process each measurement block
            for meas_info in root.findall('ns:measData/ns:measInfo', NS):
                # Get measurement metadata
                meas_info_id = meas_info.get('measInfoId')
                job_id = meas_info.find('ns:job', NS).get('jobId')
                gran_period = meas_info.find('ns:granPeriod', NS).get('duration')
                end_time = meas_info.find('ns:granPeriod', NS).get('endTime')
                
                # Map position codes to KPI names
                meas_types = {
                    mt.get('p'): mt.text 
                    for mt in meas_info.findall('ns:measType', NS)
                }
                
                # Process each measurement value
                for meas_value in meas_info.findall('ns:measValue', NS):
                    meas_obj_ldn = meas_value.get('measObjLdn')
                    nodeid = meas_obj_ldn.split('=')[1].split(',')[0] if meas_obj_ldn else None
                    
                    for r in meas_value.findall('ns:r', NS):
                        kpi_id = r.get('p')
                        raw_value = r.text
                        
                        # Handle "NIL" values by converting to 0
                        kpi_value = 0 if pd.isna(raw_value) or raw_value == "NIL" or raw_value == "NULL" else raw_value
                        
                        combined_data.append({
                            'measInfoId': meas_info_id,
                            'jobId': job_id,
                            'granPeriod': gran_period,
                            'beginTime': begin_time,
                            'endTime': end_time,
                            'measObjLdn': meas_obj_ldn,
                            'nodeid': nodeid,
                            'kpiId': kpi_id,
                            'kpiName': meas_types.get(kpi_id, f'UNKNOWN_{kpi_id}'),
                            'kpiValue': kpi_value,
                            'sourceFile': filename  # Track origin file
                        })
            
            # Move and backup processed file
            processed_path = os.path.join(config['xml_processed_dir'], filename)
            shutil.move(xml_path, processed_path)
            
            # Create timestamped backup
            backup_name = f"{timestamp}_{filename}"
            shutil.copy2(processed_path, os.path.join(config['xml_backup_dir'], backup_name))
            
            processed_count += 1
            print(f"Processed {filename} successfully")
            
        except Exception as e:
            print(f"ERROR processing {filename}: {str(e)}")
            continue

    # Save combined output
    if combined_data:
        with open(combined_json_path, 'w') as f:
            json.dump(combined_data, f, indent=2)
        print(f"Saved {len(combined_data)} records to {combined_filename}")
    
    return processed_count

def check_files_processed(**kwargs):
    """Determine whether to run Spark or skip"""
    ti = kwargs['ti']
    file_count = ti.xcom_pull(task_ids='process_xml_files')
    return 'spark_to_kafka' if file_count > 0 else 'skip_spark'

# Tasks
setup_dirs = PythonOperator(
    task_id='ensure_directories_exist',
    python_callable=ensure_directories_exist,
    dag=dag,
)

gzip_task = PythonOperator(
    task_id='process_gzip_files',
    python_callable=process_gzip_files,
    dag=dag,
)

xml_task = PythonOperator(
    task_id='process_xml_files',
    python_callable=process_xml_files,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='check_files_processed',
    python_callable=check_files_processed,
    dag=dag,
)

skip_spark = DummyOperator(
    task_id='skip_spark',
    dag=dag,
)

spark_task = SparkSubmitOperator(
    task_id='spark_to_kafka',
    application='/app/mypy/streaming.py',
    conn_id='spark_default',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5',
    conf={
        'spark.sql.streaming.forceDeleteTempCheckpointLocation': 'false',
        "spark.cores.max": "3",
        "spark.executor.memory": "4g",
        'spark.driver.extraJavaOptions': 
            f'-Djson.input.dir={config["json_output_dir"]} '
            f'-Djson.processed.dir={config["json_processed_dir"]} '
            f'-Djson.backup.dir={config["json_backup_dir"]}'
    },
    dag=dag
)



success_task = DummyOperator(
    task_id='pipeline_success',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# Workflow
setup_dirs >> gzip_task >> xml_task >> branch_task
branch_task >> [spark_task, skip_spark]
[spark_task, skip_spark] >>  success_task
