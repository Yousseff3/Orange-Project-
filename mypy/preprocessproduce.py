from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, when, lit, struct, to_json
import os
import shutil
import sys
from datetime import datetime
from glob import glob
import logging

# ===== Configuration =====
INPUT_DIR = "/app/csv/inputcsv"
ARCHIVE_DIR = "/app/csv/jobdone"
BACKUP_DIR = "/app/csv/backups"
CHECKPOINT_DIR = "/app/csv/check50"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "csv"


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Ensure directories exist
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# ===== Stream Processing =====
def process_batch(df: DataFrame, batch_id: int) -> None:
    """Process each micro-batch with file handling."""
    try:
        if df.isEmpty():
            logger.info(f"Batch {batch_id} was empty. Skipping.")
            return

        input_files = df.inputFiles()
        logger.info(f"Processing batch {batch_id} | Files: {input_files}")

        # Process data
        cleaned_df = (
		    df
		    .withColumn("Time", to_timestamp(col("Time"), "MM-dd-yyyy HH:mm"))
		    .na.fill(0, subset=["Downlink EARFCN", "LocalCell Id" ,"Downlink bandwidth"])
		    .na.fill("N/A", subset=["eNodeB Name", "Cell Name"])
		    #Replace null values for longitude and latitude with 123456
		    .withColumn("Longitude", when(col("Longitude").isNull(), 999).otherwise(col("Longitude")))
		    .withColumn("Latitude", when(col("Latitude").isNull(), 999).otherwise(col("Latitude")))
		    #Replace Null values for the other columns with the minimum
		    .na.fill(0)
		    #Rename before replacement
		    .withColumnRenamed("FT_UL.Interference", "FT_UL_Interference")
		    .withColumn("FT_UL_Interference", 
				when(trim(lower(col("FT_UL_Interference"))) == "nil", 0)
				.otherwise(col("FT_UL_Interference")))
		    #Removing column Integrity 
		    .drop("Integrity")
		)

        # Write to Kafka
        (
            cleaned_df
            .select(
                lit(str(batch_id)).alias("key"),
                to_json(struct(*cleaned_df.columns)).alias("value")
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", KAFKA_TOPIC)
            .mode("append")
            .save()
        )
        logger.info(f"Successfully wrote batch {batch_id} to Kafka")
        
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}", exc_info=True)
        raise

# ===== Main Execution =====
def main():
    spark = None
    query = None
    
    try:
        spark = (
            SparkSession.builder
            .appName("CSV-to-Kafka Processor")\
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "false")\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .config("spark.sql.files.maxPartitionBytes", "64MB") \
            .config("spark.sql.streaming.schemaInference", "true")\
            .config("spark.sql.shuffle.partitions", "4")\
            .config("spark.task.maxFailures", "4")\
            .getOrCreate()
            
        )
        spark.sparkContext.setLogLevel("WARN")

        # Define schema
        schema = StructType([
    	StructField("Time", StringType(), True),
    	StructField("eNodeB Name", StringType(), True),
    	StructField("Frequency band", StringType(), True),
    	StructField("Cell FDD TDD Indication", StringType(), True),
    	StructField("Cell Name", StringType(), True),
    	StructField("Downlink EARFCN", IntegerType(), True),
    	StructField("Downlink bandwidth", IntegerType(), True),
    	StructField("LTECell Tx and Rx Mode", StringType(), True),
    	StructField("LocalCell Id", IntegerType(), True),
    	StructField("eNodeB Function Name", StringType(), True),
    	StructField("Latitude", DoubleType(), True),
    	StructField("Longitude", DoubleType(), True),
    	StructField("Integrity", StringType(), True),
    	StructField("FT_AVE 4G/LTE DL USER THRPUT without Last TTI(ALL) (KBPS)(kbit/s)", DoubleType(), True),
    	StructField("FT_AVERAGE NB OF USERS (UEs RRC CONNECTED)", IntegerType(), True),
    	StructField("FT_PHYSICAL RESOURCE BLOCKS LOAD DL(%)", DoubleType(), True),
    	StructField("FT_PHYSICAL RESOURCE BLOCKS LOAD UL", DoubleType(), True),
    	StructField("FT_4G/LTE DL TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    	StructField("FT_4G/LTE DL&UL TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    	StructField("FT_4G/LTE UL TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    	StructField("FT_4G/LTE CONGESTED CELLS RATE", DoubleType(), True),
    	StructField("FT_4G/LTE CALL SETUP SUCCESS RATE", DoubleType(), True),
    	StructField("FT_4G/LTE AVERAGE REPORTED CQI", DoubleType(), True),
    	StructField("FT_4G/LTE PAGING DISCARD RATE", DoubleType(), True),
    	StructField("FT_4G/LTE RADIO DOWNLINK DELAY(ms)", DoubleType(), True),
    	StructField("FT_4G/LTE VOLTE TRAFFIC VOLUME (GBYTES)", DoubleType(), True),
    	StructField("FT_AVE 4G/LTE DL USER THRPUT (ALL) (KBPS)(kB/s)", DoubleType(), True),
    	StructField("FT_AVE 4G/LTE DL THRPUT (ALL) (KBITS/SEC)", DoubleType(), True),
    	StructField("FT_AVERAGE NB OF CA UEs RRC CONNECTED(number)", IntegerType(), True),
    	StructField("FT_AVERAGE NUMBER OF UE QUEUED DL", IntegerType(), True),
    	StructField("FT_AVERAGE NUMBER OF UE QUEUED UL", IntegerType(), True),
    	StructField("FT_S1 SUCCESS RATE", DoubleType(), True),
    	StructField("FT_UL_Interference", DoubleType(), True),
    	StructField("Average Nb of e-RAB per UE", DoubleType(), True),
    	StructField("Average Nb of PRB used per Ue", DoubleType(), True),
    	StructField("Average Nb of Used PRB for SRB", DoubleType(), True),
    	StructField("FT_AVERAGE NUMBER OF UE SCHEDULED PER ACTIVE TTI DL (FDD)(number)", IntegerType(), True),
    	StructField("FT_AVERAGE NUMBER OF UE SCHEDULED PER ACTIVE TTI UL (TDD)", IntegerType(), True),
    	StructField("FT_CS FALLBACK SUCCESS RATE (4G SIDE ONLY)", DoubleType(), True),
    	StructField("FT_CS FALLBACK TO WCDMA RATIO", DoubleType(), True),
    	StructField("FT_ERAB SETUP SUCCESS RATE", DoubleType(), True),
    	StructField("FT_ERAB SETUP SUCCESS RATE (ALL)(%)", DoubleType(), True),
    	StructField("FT_ERAB SETUP SUCCESS RATE (init)", DoubleType(), True),
    	StructField("FT_RRC SUCCESS RATE", DoubleType(), True),
    	StructField("Nb e-RAB Setup Fail", IntegerType(), True),
    	StructField("Nb HO fail to GERAN", IntegerType(), True),
    	StructField("Nb HO fail to UTRA FDD", IntegerType(), True),
    	StructField("Nb initial e-RAB Setup Fail", IntegerType(), True),
    	StructField("Nb initial e-RAB Setup Succ", IntegerType(), True),
    	StructField("Nb initial e-RAB Sucess rate(%)", DoubleType(), True),
    	StructField("Nb of HO over S1 for e-RAB Fail", IntegerType(), True),
    	StructField("Nb of HO over S1 for e-RAB Req", IntegerType(), True),
    	StructField("Nb of HO over S1 for e-RAB Succ", IntegerType(), True),
    	StructField("Nb of HO over X2 for e-RAB Fail", IntegerType(), True),
    	StructField("Nb of HO over X2 for e-RAB Succ", IntegerType(), True),
    	StructField("Nb of RRC connection release", IntegerType(), True),
    	StructField("Nb S1 Add e-RAB Setup fail", IntegerType(), True),
    	StructField("RRC Emergency SR", DoubleType(), True),
    	StructField("RRC High Priority SR(%)", DoubleType(), True),
    	StructField("RRC MOC SR(%)", DoubleType(), True),
    	StructField("RRC MTC SR(%)", DoubleType(), True),
    	StructField("RRC Succ rate(%)", DoubleType(), True),
    	StructField("CSFB failure rate(%)", DoubleType(), True),
    	StructField("E-RAB Resource Congestion Rate(%)", DoubleType(), True),
    	StructField("RRC Resource Congestion Rate(%)", DoubleType(), True),
    	StructField("Average TA", DoubleType(), True),
    	StructField("AVE 4G/LTE UL USER THRPUT without Last TTI (Kbps)", DoubleType(), True)
    	])
    	

        streaming_df = (
            spark.readStream
            .schema(schema)
            .option("header", "true")
            .option("maxFilesPerTrigger", 1)
            .option("sourceArchiveDir", ARCHIVE_DIR)
            .option("cleanSource", "archive")
            .csv(INPUT_DIR)
        )

        query = (
            streaming_df
            .writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .outputMode("append")
            .start()
        )

        logger.info("Streaming started. Waiting for new files...")
        query.awaitTermination()

    except Exception as e:
        logger.critical(f"Fatal error in main execution: {str(e)}", exc_info=True)
        if query and query.isActive:
            query.stop()
        if spark:
            spark.stop()
        sys.exit(1)
        
    finally:
        if query and query.isActive:
            query.stop()
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
