"""
raygun_ip_processing.py
2024-06-24 | CR
Raygun data ingestion
"""

# Import libraries for Step 1.1
# import sys

# Import libraries for Step 1.2
import os
from dotenv import load_dotenv

# Import libraries for Step 1.3

# import os
from pyspark.sql import SparkSession
# from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id, concat, lit
from pyspark.storagelevel import StorageLevel
import gc
# from pyspark.sql.functions import explode
import boto3
from botocore.client import Config

from minio import Minio

# Libraries for Step 4
import shutil

# Libraries for Step 5
# from trino.dbapi import connect

# Libraries for date/time function
from datetime import datetime, timedelta


def show_curr_datetime(timestamp: float = None, message: str = None) -> float:
    """
    Show the current date and time in Eastern time zone and the message.
    If the message is not passed, assumes "Start" when timestamp is not passed.
    otherwise assumes "Finished..." and print it along with the time elapsed
    in secords.

    Ags:
        timestamp (float): Timestamp to calculate the time elapsed
        message (str): Message to print

    Returns:
        float: New timestamp
    """
    # Get the current date and time in Eastern time zone
    eastern_tz = timezone(timedelta(hours=-5), name='EST')
    current_datetime = datetime.now(eastern_tz)
    # Format the date and time as YYYY-MM-DD HH:MM:SS
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    if message is None:
        if timestamp:
            new_message = "Finished"
        else:
            new_message = "Start"
    else:
        new_message = message
    print("")
    print(f"{new_message} at: {formatted_datetime}")
    new_timestamp = current_datetime.timestamp()
    if timestamp:
        time_elapsed = new_timestamp - timestamp
        print(f"Time elapsed: {time_elapsed} seconds")
    print("")
    return new_timestamp

########################
########################
# Step 1: Setup
########################
########################

########################
# Step 1.1: Setup Environment
########################

# Init Minio
# sh /home/PyCon2024/Project/Scripts/1.init_minio.sh data/raygun

# Install necessary packages
# !{sys.executable} -m pip install pyspark
# !{sys.executable} -m pip install s3fs
# !{sys.executable} -m pip install minio
# !{sys.executable} -m pip install pyhive
# !{sys.executable} -m pip install trino

# Install dotenv to load environment variables
# !{sys.executable} -m pip install python-dotenv

########################
# Step 1.2: Read .env file
########################

# Load environment variables

print("")
print("Loading environment variables...")

load_dotenv('minio.env')

# Access the environment variables
minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
minio_endpoint = os.getenv('MINIO_ENDPOINT', "http://minio:9000")
minio_bucket_name = os.getenv('MINIO_BUCKET_NAME', "data-lakehouse")

print("")
print("Minio Access Key:", minio_access_key)
print("Minio Secret Key:", minio_secret_key)
print("Minio Endpoint:", minio_endpoint)
print("Minio Bucket Name:", minio_bucket_name)

########################
# Step 1.3: Setup spark
########################

# Initialize Spark session

print("")
print(">> Initializing Spark session...")
print("")

spark = SparkSession.builder \
    .appName("RaygunErrorTraceAnalysis") \
    .config(
        "spark.driver.host",
        "localhost") \
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.3") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension") \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.cataloDeltaCatalog") \
    .config(
        "hive.metastore.uris",
        "thrift://metastore:9083") \
    .config(
        "spark.driver.memory",
        "15g") \
    .enableHiveSupport() \
    .getOrCreate()

# Check spark configuration

print("")
print(">> Checking spark configuration...")
spark.sparkContext.getConf().getAll()

########################
# Step 1.4: Upload Multiple JSON Files to MinIO
########################

print("")
print(">> Uploading Multiple JSON Files to MinIO...")
print("")

# Local directory path containing JSON files
local_directory = "../data/raygun"

# MinIO bucket name
bucket_name = minio_bucket_name

# Path to the JSON files in MinIO
json_files_path = f"s3a://{minio_bucket_name}/Raw/raygun/"

# Minio endpoint (only domian and port)
minio_endpoint_domain_port = minio_endpoint \
    .replace('http://', '') \
    .replace('https://', '')

print("Local directory:", local_directory)
print("Minio bucket name:", bucket_name)
print("JSON files path:", json_files_path)
print("Minio endpoint (domain and port only):", minio_endpoint_domain_port)

# Initialize Minio client
minio_client = Minio(
    minio_endpoint_domain_port,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)

# Create the bucket if it doesn't exist
if minio_client.bucket_exists(bucket_name):
    print(f"Bucket {bucket_name} already exists")
else:
    minio_client.make_bucket(bucket_name)
    # Upload JSON files to MinIO
    for filename in os.listdir(local_directory):
        if filename.endswith(".json"):
            file_path = os.path.join(local_directory, filename)
            minio_client.fput_object(bucket_name, filename, file_path)
            print(f"Uploaded {filename} to {bucket_name}")

########################
########################
# Step 2: Read Multiple JSON Files from MinIO
########################
########################

# Read JSON files into DataFrame

print("")
print(">> Reading files into DataFrame")
print(f"(Set 'df' from: {json_files_path})")
print("")

# df = spark.read.option("multiline", "true").json(json_files_path)

# Read JSON files into DataFrame and select only the desired attribute

# Specify the desired attribute and alias
desired_attribute = "Request.IpAddress"
desired_alias = "RequestIpAddress"

# df = spark.read.option("multiline", "true") \
#     .json(json_files_path) \
#     .select(desired_attribute)

# Optimize the spark.read.option().json() file processing
# by first getting a list of all file paths in the directory,
# then use spark.read.json() to read all files IN CHUCKS of `batch_size`

# import boto3
# from botocore.client import Config


def list_files_minio(bucket_name, prefix, minio_endpoint, access_key,
                     secret_key):
    """
    List files in a Minio bucket under a specific prefix using boto3.
    """
    session = boto3.session.Session()
    s3 = session.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version='s3v4',
            retries={
                'max_attempts': 3,
                'mode': 'standard'
            },
            read_timeout=120  # Set read timeout to 120 seconds
        ),
        # region_name='us-east-1',
    )
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix,
                                       PaginationConfig={'PageSize': 1000})

    files = []
    for page in page_iterator:
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files

print("")
print(">> List files in a Minio bucket under a specific prefix using boto3...")

prefix = 'Raw/raygun/'

init_ts = show_curr_datetime()
file_list = list_files_minio(minio_bucket_name, prefix, minio_endpoint,
                             minio_access_key, minio_secret_key)

# Convert to the format read by Spark
file_list = [f"s3a://{bucket_name}/{file_name}" for file_name in file_list]
show_curr_datetime(init_ts)

print(f"Number of files: {len(file_list)}")

# Define the batch size
# (Adjust the batch size based on your memory capacity and data size)
batch_size = 5000


# from pyspark.sql import DataFrame
#
# Function to process DataFrame
# def process_data(df: DataFrame):
#     # Insert the processing steps here, e.g., transformations, aggregations,
#     # writing to sink, etc.
#     pass


print("")
print(">> Reading JSON files into DataFrame...")
print(f"(In chucks of: {batch_size})")

# Process the files in batches
init_ts = show_curr_datetime()
for i in range(0, len(file_list), batch_size):
    batch_files = file_list[i:i+batch_size]
    # df = spark.read.option("multiline", "true").json(batch_files)
    df = spark.read.option("multiline", "true") \
        .json(batch_files) \
        .select(
            col(desired_attribute)
            .alias(desired_alias)
        )
    # process_data(df)
show_curr_datetime(init_ts)

# Show schema structure
print("")
print("Schema structure")
print(
    df.printSchema()
)

# Show dataframe content
# df.show(truncate=False)

########################
########################
# Step 3: Process JSON Data
########################
########################

print("")
print(">> Flattening the nested structure for easier analysis...")
print("")

# Flatten the nested structure for easier analysis

# df_flattened = df.select(
#     col("Error.Message").alias("ErrorMessage"),
#     col("Error.ClassName").alias("ErrorClassName"),
#     col("Error.FileName").alias("ErrorFileName"),
#     explode("Error.StackTrace").alias("StackTrace"),
#     col("MachineName"),
#     col("Request.HostName").alias("RequestHostName"),
#     col("Request.Url").alias("RequestUrl"),
#     col("Request.HttpMethod").alias("RequestHttpMethod"),
#     col("Request.IpAddress").alias("RequestIpAddress"),
#     col("Request.QueryString"),
#     col("Request.Headers"),
#     col("Request.Data"),
# )

# from pyspark.sql.functions import col
# from pyspark.storagelevel import StorageLevel

num_partitions = 200  # Adjust based on your workload and cluster setup

# Repartition the DataFrame to optimize parallel processing and memory usage
# (Adjust the number of partitions based on your environment and data size)
df = df.repartition(num_partitions)

# Persist the DataFrame if it's going to be used multiple times
df.persist(StorageLevel.MEMORY_AND_DISK)

# Processing the DataFrame
init_ts = show_curr_datetime()
df_flattened = df.select(
    col(desired_alias)
)
show_curr_datetime(init_ts)

# Process your data here, and then you can unpersist when done
# process_data(df_flattened)

# Unpersist the DataFrame to free up memory
df.unpersist()

# Optional: Trigger garbage collection if you're facing memory issues
# import gc
gc.collect()

# Show flattened data frame schema structure

print("")
print(">> Flattened data frame schema structure...")
print("")

df_flattened.printSchema()

# Show flattened data frame content
# df_flattened.show(truncate=False)

########################
########################
# Step 4: Save Data into Apache Hive
########################
########################

print("")
print(">> Saving Data into Apache Hive...")
print("")

# hive_location = "/opt/hive/data/warehouse"
hive_location = "/home/PyCon2024/Project/Storage/hive/"
dest = f"{hive_location}/raygun_error_traces"

shutil.rmtree(dest, ignore_errors=True)

df_flattened = df_flattened.repartition(num_partitions)

# from pyspark.sql.functions import monotonically_increasing_id, concat, lit

# Add a salt column for high-cardinality column to distribute writes
df_flattened = df_flattened.withColumn(
    "salt",
    concat(col("HighCardinalityColumn"), lit("_"),
           (monotonically_increasing_id() % num_partitions).cast("string")))
df_flattened = df_flattened.repartition("salt")

# Set properties to manage memory better during shuffle and write
# align with the number of repartitions
spark.conf.set("spark.sql.shuffle.partitions", num_partitions)
# adjust the number of records in memory before spilling to disk
spark.conf.set("spark.sql.files.maxRecordsPerBatch", "500")


# Save the processed data into Hive table
# Adjust the number of partitions as necessary in .repartition(num_partitions)
# df_flattened \
#     .repartition(num_partitions) \
#     .write \
#     .mode("overwrite") \
#     .saveAsTable("raygun_error_traces")

# Write in batches if applicable (pseudo-code)
init_ts = show_curr_datetime()
batches = 10  # Splits data into 10 batches
for batch_df in df_flattened.randomSplit([0.1] * batches):
    batch_df.write.mode("append").saveAsTable("raygun_error_traces")
show_curr_datetime(init_ts)

# Verify the data is saved correctly

print("")
print(">> Verifying the data is saved correctly...")
print("   (showing only first 10 rows)")
print("")

print(
    spark.sql("SELECT * FROM raygun_error_traces LIMIT 10")
    .show(truncate=False)
)

# Get the IP addresses summary

print("")
print(">> Get the IP addresses summary")

init_ts = show_curr_datetime()
print(
    spark.sql("SELECT RequestIpAddress FROM raygun_error_traces" +
              " GROUP BY RequestIpAddress",
              showtruncate=False)
)
show_curr_datetime(init_ts)

print("")
print("Done!")

########################
########################
# Step 5: Query with Trino
########################
########################

# Connect to Trino

# With trino-python-client
# https://github.com/trinodb/trino-python-client

# conn = connect(
#     host='trino',
#     port=8081,
#     user='admin',
#     catalog='hive',
#     schema='default',
# )

# # Create a cursor object using the cursor() method
# cursor = conn.cursor()

# # Execute a query
# cursor.execute("SELECT * FROM raygun_error_traces LIMIT 10")

# # Fetch the data
# rows = cursor.fetchall()

# # Display the data
# for row in rows:
#     print(row)
