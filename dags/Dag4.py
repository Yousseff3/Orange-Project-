from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
import os
import xml.etree.ElementTree as ET
import json
import shutil
import gzip
from glob import glob
import time
import pandas 
import numpy 


default_args = {
    'owner': 'youssef',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 13),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'hardware_monitoring',
    default_args=default_args,
    description='Process XML to JSON and stream to Kafka',
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1
)

config = {
    # XML paths
    'xml_input_dir': '/app/xmlhard/xmlin',
    'xml_processed_dir': '/app/xmlhard/xmldone',
    'xml_backup_dir': '/app/xmlhard/xmlbackup',
    
    # JSON paths
    'json_output_dir': '/app/xmlhard/jsonout',
    'json_processed_dir': '/app/xmlhard/jsondone',
    'json_backup_dir': '/app/xmlhard/jsonbackup',
    
    # Kafka config
    'kafka_bootstrap': 'kafka:9092',
    'kafka_topic': 'xmlhard',
  
}

def ensure_directories_exist():
    """Ensure all required directories exist"""
    for dir_path in [
        config['xml_input_dir'], config['xml_processed_dir'], config['xml_backup_dir'],
        config['json_output_dir'], config['json_processed_dir'], config['json_backup_dir']
    ]:
        os.makedirs(dir_path, exist_ok=True)

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
            
            # Get the localDn from managedElement
            managed_element = root.find('ns:measData/ns:managedElement', NS)
            local_dn = managed_element.get('localDn') if managed_element is not None else None
            
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
                    meas_obj_ldn = meas_value.get('measObjLdn')  # Keep original value without processing
                    
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
                            'measObjLdn': meas_obj_ldn,  # Original value
                            'localDn': local_dn,
                            'kpiId': kpi_id,
                            'kpiName': meas_types.get(kpi_id, f'UNKNOWN_{kpi_id}'),
                            'kpiValue': kpi_value,
                            'sourceFile': filename
                        })
            
            # Move and backup processed file
            processed_path = os.path.join(config['xml_processed_dir'], filename)
            shutil.move(xml_path, processed_path)
            
            # Create timestamped backup
            backup_name = f"{timestamp}_{filename}"
            shutil.copy2(processed_path, os.path.join(config['xml_backup_dir'], backup_name))
            
            processed_count += 1
            print(f"Processed {filename} successfully")
            
        except ET.ParseError as e:
            print(f"XML PARSE ERROR in {filename}: {str(e)}")
            continue
        except Exception as e:
            print(f"UNEXPECTED ERROR processing {filename}: {str(e)}")
            continue
    
    # Save combined data to JSON
    if combined_data:
        with open(combined_json_path, 'w') as f:
            json.dump(combined_data, f, indent=2)
        print(f"Saved combined data to {combined_filename}")
    
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
    application='/app/mypy/xmlhard.py',
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
setup_dirs >> xml_task >> branch_task
branch_task >> [spark_task, skip_spark]
[spark_task, skip_spark] >>  success_task
