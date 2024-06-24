"""
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
from pyspark.sql.functions import col, explode
from minio import Minio

# Libraries for Step 4
import shutil

# Libraries for Step 5
# from trino.dbapi import connect

# Libraries for date/time function
from datetime import datetime


def show_curr_datetime():
    # Get the current date and time
    current_datetime = datetime.now()
    # Format the date and time as YYYY-MM-DD HH:MM:SS
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    print(formatted_datetime)


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
    .config("spark.driver.host", "localhost") \
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.3") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension") \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.cataloDeltaCatalog") \
    .config("hive.metastore.uris", "thrift://metastore:9083") \
    .config("spark.driver.memory", "15g") \
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
print(">> Reading JSON files into DataFrame...")
print("")
print(f"Set 'df' from: {json_files_path}")
show_curr_datetime()
print("")
print("Start...")
print("")

# df = spark.read.option("multiline", "true").json(json_files_path)
# Read JSON files into DataFrame and select only the desired attribute
# Specify the desired attribute
desired_attribute = "Request.IpAddress"
df = spark.read.option("multiline", "true") \
    .json(json_files_path) \
    .select(desired_attribute)

print("")
print("Finished...")
show_curr_datetime()

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
df_flattened = df.select(
    # col("Error.Message").alias("ErrorMessage"),
    # col("Error.ClassName").alias("ErrorClassName"),
    # col("Error.FileName").alias("ErrorFileName"),
    # explode("Error.StackTrace").alias("StackTrace"),
    # col("MachineName"),
    # col("Request.HostName").alias("RequestHostName"),
    # col("Request.Url").alias("RequestUrl"),
    # col("Request.HttpMethod").alias("RequestHttpMethod"),
    col("Request.IpAddress").alias("RequestIpAddress"),
    # col("Request.QueryString"),
    # col("Request.Headers"),
    # col("Request.Data"),
)

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

hive_location = "/opt/hive/data/warehouse"
dest = f"{hive_location}/raygun_error_traces"

shutil.rmtree(dest, ignore_errors=True)

# Save the processed data into Hive table
df_flattened \
    .write \
    .mode("overwrite") \
    .saveAsTable("raygun_error_traces")

# Verify the data is saved correctly

print("")
print(">> Verifying the data is saved correctly...")
print("   (showing only first 10 rows)")
print("")

print(
    spark.sql("SELECT * FROM raygun_error_traces LIMIT 10").show(truncate=False)
)

# Get the IP addresses summary

print("")
print(">> Get the IP addresses summary")
print("")

print("")
print(
    spark.sql("SELECT RequestIpAddress FROM raygun_error_traces" +
              " GROUP BY RequestIpAddress",
              showtruncate=False)
)

print("")
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
