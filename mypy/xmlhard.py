from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, when, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import hashlib
import shutil
from datetime import datetime
from glob import glob

def get_config_value(spark, key):
    """Helper to get config values from Spark conf"""
    return spark.sparkContext.getConf().get(f"spark.driver.extraJavaOptions", "").split(f"-D{key}=")[-1].split(" ")[0]

def ensure_dirs(*dirs):
    """Ensure directories exist"""
    for d in dirs:
        os.makedirs(d, exist_ok=True)

def backup_file(src_path, backup_dir):
    """Create timestamped backup with duplicate detection"""
    if not os.path.exists(src_path):
        return False
    
    with open(src_path, 'rb') as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
    
    existing_backups = glob(os.path.join(backup_dir, f"*_{os.path.basename(src_path)}"))
    for backup in existing_backups:
        try:
            with open(backup, 'rb') as f:
                if hashlib.md5(f.read()).hexdigest() == file_hash:
                    return False
        except:
            continue
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"backup_{timestamp}_{os.path.basename(src_path)}")
    shutil.copy2(src_path, backup_path)
    return True

def move_to_processed(src_path, processed_dir):
    """Move file to processed directory"""
    if not os.path.exists(src_path):
        return False
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processed_path = os.path.join(processed_dir, f"processed_{timestamp}_{os.path.basename(src_path)}")
    shutil.move(src_path, processed_path)
    return True

def main():
    spark = SparkSession.builder \
        .appName("xml-json-to-kafka").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "false") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.files.maxPartitionBytes", "64MB") \
        .config("spark.sql.shuffle.partitions", "4")\
        .config("spark.task.maxFailures", "6") \
        .getOrCreate()

    # Get config from Spark properties
    json_input_dir = get_config_value(spark, "json.input.dir")
    json_processed_dir = get_config_value(spark, "json.processed.dir")
    json_backup_dir = get_config_value(spark, "json.backup.dir")
    checkpoint_dir = "/app/checkpointxmlhard"
    
    ensure_dirs(json_processed_dir, json_backup_dir, checkpoint_dir)

    # Schema definition (removed sourceFile field)
    schema = StructType([
        StructField("measInfoId", StringType(), nullable=False),
        StructField("jobId", StringType(), nullable=False),
        StructField("granPeriod", StringType(), nullable=False),
        StructField("beginTime", StringType(), nullable=False),
        StructField("endTime", StringType(), nullable=False),
        StructField("measObjLdn", StringType(), nullable=False),
        StructField("localDn", StringType(), nullable=False),
        StructField("kpiId", StringType(), nullable=False),
        StructField("kpiName", StringType(), nullable=False),
        StructField("kpiValue", StringType(), nullable=False)
    ])

    # Track processed files
    processed_files = set()

    # Process each batch
    def process_batch(df, batch_id):
        nonlocal processed_files
        if df.isEmpty():
            print("No data in batch")
            return
        
        # Get unique source files in this batch using input_file_name()
        input_files_df = df.select(input_file_name().alias("file_path")).distinct()
        input_files = [row.file_path for row in input_files_df.collect()]
        
        clean_files = []
        for file_path in input_files:
            # Remove Spark file:// prefix if present
            clean_path = file_path.replace("file://", "") if file_path.startswith("file://") else file_path
            clean_files.append(clean_path)
            
            # Backup first
            if os.path.exists(clean_path):
                backup_file(clean_path, json_backup_dir)
            else:
                print(f"Warning: Source file not found at {clean_path}")
                continue
        
        # Transform and write to Kafka
        (df.withColumn("kpiValue", when(col("kpiValue") == "NIL", 0).otherwise(col("kpiValue").cast("double")))
          .select(to_json(struct([col(c) for c in df.columns if c != "file_path"])).alias("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("topic", "xmlhard")
          .save()
        )
        
        # Move to processed immediately
        for clean_path in clean_files:
            if os.path.exists(clean_path) and move_to_processed(clean_path, json_processed_dir):
                processed_files.add(clean_path)
        
        print(f"Processed batch {batch_id} from files: {clean_files}")

    # Create streaming DataFrame with file path tracking
    streaming_df = spark.readStream \
        .schema(schema) \
        .option("multiline", "true") \
        .option("maxFilesPerTrigger", 1) \
        .json(json_input_dir) \
        .withColumn("file_path", input_file_name())

    # Start streaming query
    query = streaming_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(once=True) \
        .start()

    # Wait for completion
    query.awaitTermination()
    
    # Verify all expected files were processed
    input_files = set(glob(os.path.join(json_input_dir, "*.json")))
    if input_files - processed_files:
        print(f"Warning: Some files weren't processed: {input_files - processed_files}")
    
    spark.stop()
    print(f"Stream processing completed. Processed {len(processed_files)} files.")

if __name__ == "__main__":
    main()
