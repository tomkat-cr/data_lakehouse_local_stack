"""
raygun_ip_processing.py
2024-06-24 | CR
Raygun data ingestion
"""

# Import libraries for Step 1.1
# import sys

# Import libraries for Step 1.2
import os
import sys
import pprint
from dotenv import load_dotenv

# Import libraries for Step 1.3

# import os
from pyspark.sql import SparkSession
# from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id, concat, lit
from pyspark.storagelevel import StorageLevel
import gc
from pyspark.sql.functions import explode
import boto3
from botocore.client import Config

from minio import Minio

# Libraries for Step 4
import shutil

# Libraries for Step 5
from trino.dbapi import connect

# Libraries for date/time function
from datetime import datetime, timedelta, timezone


DEFAULT_SQL = "SELECT " + \
    "RequestIpAddress, COUNT(*) AS IpCount" + \
    " FROM raygun_error_traces" + \
    " GROUP BY RequestIpAddress" + \
    " ORDER BY IpCount DESC"


def get_config() -> dict:
    print("Loading environment variables...")

    env_files = ['processing.env', 'minio.env']
    for env_file in env_files:
        load_dotenv(env_file)

    config = {}

    # Access the environment variables
    config["minio_access_key"] = os.getenv('MINIO_ACCESS_KEY')
    config["minio_secret_key"] = os.getenv('MINIO_SECRET_KEY')
    config["minio_endpoint"] = os.getenv('MINIO_ENDPOINT', "http://minio:9000")
    config["minio_bucket_name"] = os.getenv('MINIO_BUCKET_NAME', "data-lakehouse")

    # try:
    #     load_dotenv('processing.env')
    # except FileNotFoundError:
    #     print("Warning: processing.env file not found. Using default values.")

    config["debug"] = os.getenv('DEBUG', '1')
    config["debug"] = False if config["debug"] == '0' else True

    # Root directory path
    config["base_path"] = os.getenv('BASE_PATH')
    # config["base_path"] = os.getenv('BASE_PATH', "/home/PyCon2024/Project")

    if not config["base_path"]:
        print("Error: BASE_PATH environment variable is not set.")
        return

    # To process all JSON files:
    # testing_iteractions = None
    # To process a small set of JSON files:
    config["testing_iteractions"] = os.getenv('TESTING_ITERACTIONS', '1')
    if config["testing_iteractions"]:
        config["testing_iteractions"] = int(config["testing_iteractions"])
    else:
        config["testing_iteractions"] = None

    # S3 pagination page size: 1000 files chunks
    config["s3_page_size"] = int(os.getenv('S3_PAGE_SIZE', '1000'))

    # S3 read timeout: 120 seconds
    config["s3_read_timeout"] = int(os.getenv('S3_READ_TIMEOUT', '120'))

    # Local directory path containing JSON files
    # input_local_directory = os.getenv('INPUT_LOCAL_DIRECTORY', "data")
    config["input_local_directory"] = os.getenv('INPUT_LOCAL_DIRECTORY')
    config["local_directory"] = f"{config['base_path']}/{config['input_local_directory']}"

    # S3 prefix (directory path in the bucket) to store raw data read from
    # the local directory
    # s3_prefix = os.getenv('S3_PREFIX', 'Raw')
    config["s3_prefix"] = os.getenv('S3_PREFIX')

    # Desired attribute and alias to filter one column
    config["desired_attribute"] = os.getenv('DESIRED_ATTRIBUTE', "Request.IpAddress")
    config["desired_alias"] = os.getenv('DESIRED_ALIAS', "RequestIpAddress")

    # Repartition the DataFrame to optimize parallel processing and memory usage
    # (Adjust the number of partitions based on your environment and data size,
    #  workload and cluster setup)
    config["df_num_partitions"] = int(os.getenv('DF_NUM_PARTITIONS', '200'))

    # Define the batch size to read into dataframe the JSON files in batches
    # (Adjust the batch size based on your memory capacity and data size)
    config["df_read_batch_size"] = int(os.getenv('DF_READ_BATCH_SIZE', '5000'))

    # Number of batches to Save Data into Apache Hive
    config["hive_batches"] = int(os.getenv('HIVE_BATCHES', '10'))  # Splits data into 10 batches

    # Final output result file
    config["sql_results_path"] = f"{config['base_path']}/Outputs/" + \
        os.getenv('RESULTS_SUB_DIRECTORY')
        # os.getenv('RESULTS_SUB_DIRECTORY', "raygun_ip_addresses_summary")

    # Hive location
    # config["hive_location"] = config["base_path"] + "/Storage/hive/"
    config["hive_location"] = "/opt/hive/data/warehouse"
    # Hive destination
    config['hive_dest'] = f"{config['hive_location']}/raygun_error_traces"
    # Hive metastore URIs
    config["hive_metastore_uri"] = "thrift://metastore:9083"

    print("")
    print("Minio Access Key:", config["minio_access_key"])
    print("Minio Secret Key:", config["minio_secret_key"])
    print("Minio Endpoint:", config["minio_endpoint"])
    print("Minio Bucket Name:", config["minio_bucket_name"])

    print("")
    print("Base path:", config["base_path"])
    print("Testing iteractions:", config["testing_iteractions"] or "PRODUCTION")
    print("Input local directory:", config["local_directory"])
    print("S3 page size:", config["s3_page_size"])
    print("S3 read timeout:", config["s3_read_timeout"])
    print("S3 prefix:", config["s3_prefix"])
    print("Desired attribute:", config["desired_attribute"])
    print("Desired alias:", config["desired_alias"])
    print("Dataframe number of partitions (for repartitioning):", config["df_num_partitions"])
    print("Dataframe read batch size:", config["df_read_batch_size"])
    print("Hive batches:", config["hive_batches"])
    print("Hive location:", config["hive_location"])
    print("Hive destination:", config["hive_dest"])
    print("Results directory path:", config["sql_results_path"])
    print("")

    return config


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
        time_elapsed = "{:.4f}".format((new_timestamp - timestamp)/60)
        print(f"Time elapsed: {time_elapsed} minutes")
    print("")
    return new_timestamp


def get_spark_session(config: dict):
    """
    Get a Spark session.
    """
    return SparkSession.builder \
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
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(
            "hive.metastore.uris",
            config["hive_metastore_uri"]) \
        .config(
            "spark.driver.memory",
            "15g") \
        .enableHiveSupport() \
        .getOrCreate()


def list_files_minio(config: dict) -> list:
    """
    List files in a Minio bucket under a specific prefix using boto3.
    """
    session = boto3.session.Session()
    s3 = session.client(
        's3',
        endpoint_url=config["minio_endpoint"],
        aws_access_key_id=config["minio_access_key"],
        aws_secret_access_key=config["minio_secret_key"],
        config=Config(
            signature_version='s3v4',
            retries={
                'max_attempts': 3,
                'mode': 'standard'
            },
            read_timeout=config["s3_read_timeout"]
        ),
        # region_name='us-east-1',
    )
    print("Starting paginator...")
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=config["minio_bucket_name"],
        Prefix=f"{config['s3_prefix']}/",
        PaginationConfig={'PageSize': config["s3_page_size"]}
    )

    print("Starting files append...")
    files = []
    j = 0
    for page in page_iterator:
        j += 1
        print(f"{j}) Current files: {len(files)}" +
              f" | Files to append: {len(page.get('Contents', []))}")
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
        if config["testing_iteractions"] and j >= config["testing_iteractions"]:
            break
    return files


# Function to process DataFrame
# def process_data(df: DataFrame):
#     # Insert the processing steps here, e.g., transformations, aggregations,
#     # writing to sink, etc.
#     pass


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

def ingest():

    ########################
    # Step 1.2: Read .env file
    ########################

    # Load environment variables
    config = get_config()

    # Start the timer
    print("")
    print("********************************")
    print("*** Ingestion Process begins ***")
    print("********************************")
    start_time = show_curr_datetime()


    ########################
    # Step 1.3: Setup spark
    ########################

    # Initialize Spark session

    print("")
    print(">> Initializing Spark session...")
    print("")

    spark = get_spark_session(config)

    # Check spark configuration

    if config["debug"]:
        print("")
        print(">> Checking spark configuration...")
        pprint.pprint(spark.sparkContext.getConf().getAll())

    ########################
    # Step 1.4: Upload Multiple JSON Files to MinIO
    ########################

    print("")
    print(">> Uploading Multiple JSON Files to MinIO...")
    print("")

    # MinIO bucket name
    bucket_name = config["minio_bucket_name"]

    # Path to the JSON files in MinIO
    json_files_path = f"s3a://{bucket_name}/{config['s3_prefix']}/"

    # Minio endpoint (only domian and port)
    minio_endpoint_domain_port = config["minio_endpoint"] \
        .replace('http://', '') \
        .replace('https://', '')

    print("Local directory:", config["local_directory"])
    print("Minio bucket name:", bucket_name)
    print("JSON files path:", json_files_path)
    print("Minio endpoint (domain and port only):", minio_endpoint_domain_port)

    # Initialize Minio client
    minio_client = Minio(
        minio_endpoint_domain_port,
        access_key=config["minio_access_key"],
        secret_key=config["minio_secret_key"],
        secure=False
    )

    # Create the bucket if it doesn't exist
    if minio_client.bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} already exists. Skipping...")
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
    print(">> READING FILES INTO DATAFRAME")
    print("")

    # df = spark.read.option("multiline", "true").json(json_files_path)

    # Read JSON files into DataFrame and select only the desired attribute

    # df = spark.read.option("multiline", "true") \
    #     .json(json_files_path) \
    #     .select(config["desired_attribute"])

    # Optimize the spark.read.option().json() file processing
    # by first getting a list of all file paths in the directory,
    # then use spark.read.json() to read all files IN CHUCKS of `df_read_batch_size`

    print("")
    print(">> List files in a Minio bucket under a specific prefix using boto3...")

    init_ts = show_curr_datetime()
    file_list = list_files_minio(config)

    # Convert to the format read by Spark
    file_list = [f"s3a://{bucket_name}/{file_name}" for file_name in file_list]
    show_curr_datetime(init_ts)

    print(f"Number of files: {len(file_list)}")

    print("")
    print(">> Reading JSON files into DataFrame...")
    print(f"(Set 'df' from: {json_files_path})")
    print(f"(In chucks of: {config['df_read_batch_size']})")

    # Process the files in batches
    init_ts = show_curr_datetime()
    j = 0
    for i in range(0, len(file_list), config["df_read_batch_size"]):
        j += 1
        print(f"{j}) From: {i} | To: {i+config['df_read_batch_size']}")
        batch_files = file_list[i:i+config["df_read_batch_size"]]
        if config["desired_attribute"]:
            df = spark.read.option("multiline", "true") \
                .json(batch_files) \
                .select(
                    col(config["desired_attribute"])
                    .alias(config["desired_alias"])
                )
        else:
            df = spark.read.option("multiline", "true") \
                .json(batch_files)
        # Call specific function to process data
        # process_data(df)
        if config["testing_iteractions"] and j >= config["testing_iteractions"]:
            break
    show_curr_datetime(init_ts)

    # Show schema structure
    print("")
    print("Dataframe schema structure:")
    df.printSchema()

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

    # Repartition the DataFrame to optimize parallel processing and memory usage
    # (Adjust the number of partitions based on your environment and data size)
    df = df.repartition(config["df_num_partitions"])

    # Persist the DataFrame if it's going to be used multiple times
    df.persist(StorageLevel.MEMORY_AND_DISK)

    # Processing the DataFrame
    print("Processing the DataFrame...")
    init_ts = show_curr_datetime()

    if config["desired_attribute"]:
        df_flattened = df.select(
            col(config["desired_alias"])
        )
    else:
        df_flattened = df.select(
            col("Error.Message").alias("ErrorMessage"),
            col("Error.ClassName").alias("ErrorClassName"),
            col("Error.FileName").alias("ErrorFileName"),
            explode("Error.StackTrace").alias("StackTrace"),
            col("MachineName"),
            col("Request.HostName").alias("RequestHostName"),
            col("Request.Url").alias("RequestUrl"),
            col("Request.HttpMethod").alias("RequestHttpMethod"),
            col("Request.IpAddress").alias("RequestIpAddress"),
            col("Request.QueryString"),
            col("Request.Headers"),
            col("Request.Data"),
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

    print("Flattened dataframe schema structure:")
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
    # dest = f"{hive_location}/raygun_error_traces"

    print(f"Hive destination: {config['hive_dest']}")
    print("")

    print("Recursively delete a directory tree...")
    shutil.rmtree(config['hive_dest'], ignore_errors=True)

    df_flattened = df_flattened.repartition(config["df_num_partitions"])

    # from pyspark.sql.functions import monotonically_increasing_id, concat, lit

    # Add a salt column for high-cardinality column to distribute writes
    # df_flattened = df_flattened.withColumn(
    #     "salt",
    #     # concat(col("HighCardinalityColumn"), lit("_"),
    #     concat(col(config["desired_alias"]), lit("_"),
    #            (monotonically_increasing_id() % config["df_num_partitions"]).cast("string")))
    # df_flattened = df_flattened.repartition("salt")

    # Set properties to manage memory better during shuffle and write
    # align with the number of repartitions
    spark.conf.set("spark.sql.shuffle.partitions", config["df_num_partitions"])

    # adjust the number of records in memory before spilling to disk
    spark.conf.set("spark.sql.files.maxRecordsPerBatch", "500")

    # Save the processed data into Hive table

    # Adjust the number of partitions as necessary in .repartition(config["df_num_partitions"])
    # df_flattened \
    #     .repartition(config["df_num_partitions"]) \
    #     .write \
    #     .mode("overwrite") \
    #     .saveAsTable("raygun_error_traces")

    # Write in batches
    print(f"Splits data into {config['hive_batches']} batches")
    print("Starting write in batches...")
    init_ts = show_curr_datetime()
    j = 0
    for batch_df in df_flattened.randomSplit([0.1] * config["hive_batches"]):
        j += 1
        print(f"{j}) {batch_df.count()} rows")
        batch_df.write.mode("append").saveAsTable("raygun_error_traces")
    show_curr_datetime(init_ts)

    # Verify the data is saved correctly

    print("")
    print(">> Verifying the data is saved correctly...")
    print("   (showing only first 10 rows)")
    print("")

    spark.sql("SELECT * FROM raygun_error_traces LIMIT 10") \
        .show(truncate=False)

    # Stop the timer
    print("")
    print("*************************")
    print("* Ingestion Process end *")
    print("*************************")
    show_curr_datetime(start_time)


def get_spark_query(sql: str = None):
    """
    This function should return a summary of the data processing performed.
    Get the IP addresses summary
    """

    config = get_config()
    spark = get_spark_session(config)

    if not sql:
        sql = DEFAULT_SQL

    print("")
    print(">> Get Spark Query")
    print(sql)

    init_ts = show_curr_datetime()
    ip_addresses_summary = spark.sql(sql)

    # Save the IP addresses summary as a CSV file
    ip_addresses_summary \
        .coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(config["sql_results_path"])

    print("IP addresses summary:")
    ip_addresses_summary.show(truncate=False)

    print("")
    print(f"IP addresses summary saved to {config['sql_results_path']}")

    show_curr_datetime(init_ts)

    print("")
    print("Done!")


def get_trino_query(sql: str = None):
    ########################
    ########################
    # Step 5: Query with Trino
    ########################
    ########################

    # Connect to Trino

    # With trino-python-client
    # https://github.com/trinodb/trino-python-client

    config = get_config()

    if not sql:
        sql = DEFAULT_SQL

    print("")
    print(">> Get Trino Query")
    print(sql)

    conn = connect(
        host='trino',
        port=8081,
        user='admin',
        catalog='hive',
        schema='default',
    )

    # Create a cursor object using the cursor() method
    cursor = conn.cursor()

    # Execute a query
    cursor.execute(sql)

    # Fetch the data
    rows = cursor.fetchall()

    # Display the data
    for row in rows:
        print(row)


if __name__ == "__main__":
    # Get mode parameter
    mode = sys.argv[1] if len(sys.argv) > 1 else None
    sql = sys.argv[1] if len(sys.argv) > 2 else None
    if mode == "ingest":
        ingest()
        get_spark_query(sql)
    elif mode == "spark_sql":
        get_spark_query(sql)
    elif mode == "trino_sql":
        get_trino_query(sql)
    else:
        print("")
        print("Invalid mode parameter.")
        print("Available options: ingest, spark_sql, trino_sql")
        print("E.g.")
        print("MODE=ingest make raygun_ip_processing")
        print("MODE=spark_sql make raygun_ip_processing")
        print(
            "SQL='SELECT RequestIpAddress FROM raygun_error_traces " + 
            "GROUP BY RequestIpAddress' MODE=spark_sql " +
            "make raygun_ip_processing")
        print("")
        sys.exit(1)
