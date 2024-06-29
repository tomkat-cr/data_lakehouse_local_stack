"""
raygun_ip_processing.py
2024-06-24 | CR
Raygun data ingestion
"""

# Import libraries for Step 1.1
# import sys

# Import libraries for Step 1.2
import io
import os
import sys
import pprint
from dotenv import load_dotenv

# Import libraries for Step 1.3
# import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
# from pyspark.sql.functions import monotonically_increasing_id, concat, lit
from pyspark.errors.exceptions.captured import AnalysisException
# from pyspark.storagelevel import StorageLevel

import gc

import boto3
from botocore.client import Config

from minio import Minio
from minio.error import S3Error

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


def get_config(show_params: bool = True) -> dict:

    print("")
    print("Loading environment variables...")

    env_files = ['processing.env', 'minio.env']
    for env_file in env_files:
        load_dotenv(env_file)

    config = {}

    # Access the environment variables
    config["minio_access_key"] = os.getenv('MINIO_ACCESS_KEY')
    config["minio_secret_key"] = os.getenv('MINIO_SECRET_KEY')
    config["minio_endpoint"] = os.getenv('MINIO_ENDPOINT', "http://minio:9000")
    config["minio_bucket_name"] = os.getenv(
        'MINIO_BUCKET_NAME', "data-lakehouse")

    config["debug"] = os.getenv('DEBUG', '1')
    config["debug"] = False if config["debug"] == '0' else True

    # Root directory path
    config["base_path"] = os.getenv('BASE_PATH')
    # config["base_path"] = \
    #   os.getenv('BASE_PATH', "/home/LocalLakeHouse/Project")

    if not config["base_path"]:
        print("Error: BASE_PATH environment variable is not set.")
        return None

    # To process all JSON files:
    # testing_iteractions = None
    # To process a small set of JSON files:
    config["testing_iteractions"] = os.getenv('TESTING_ITERACTIONS', '1')
    if config["testing_iteractions"]:
        config["testing_iteractions"] = int(config["testing_iteractions"])
    else:
        config["testing_iteractions"] = None

    # S3 protocol (s3 / s3a)
    config["s3_protocol"] = os.getenv('S3_PROTOCOL', 's3a')

    # S3 pagination page size: 1000 files chunks
    config["s3_page_size"] = int(os.getenv('S3_PAGE_SIZE', '1000'))

    # S3 read timeout: 120 seconds
    config["s3_read_timeout"] = int(os.getenv('S3_READ_TIMEOUT', '120'))

    # Local directory path containing JSON files
    # input_local_directory = os.getenv('INPUT_LOCAL_DIRECTORY', "data")
    config["input_local_directory"] = os.getenv('INPUT_LOCAL_DIRECTORY')
    config["local_directory"] = \
        f"{config['base_path']}/{config['input_local_directory']}"

    # S3 prefix (directory path in the bucket) to store raw data read from
    # the local directory
    # s3_prefix = os.getenv('S3_PREFIX', 'Raw')
    config["s3_prefix"] = os.getenv('S3_PREFIX')

    # Desired attribute and alias to filter one column
    config["desired_attribute"] = \
        os.getenv('DESIRED_ATTRIBUTE', "Request.IpAddress")
    config["desired_alias"] = os.getenv('DESIRED_ALIAS', "RequestIpAddress")

    # Number of batches to Save Data into Apache Hive
    config["hive_batches"] = \
        int(os.getenv('HIVE_BATCHES', '10'))  # Splits data into 10 batches

    # Hive location
    # config["hive_location"] = config["base_path"] + "/Storage/hive/"
    config["hive_location"] = os.getenv(
        'HIVE_LOCATION', "/opt/hive/data/warehouse")

    # Hive destination
    config['hive_dest'] = os.getenv('HIVE_DEST', "raygun_error_traces")
    config['hive_dest'] = f"{config['hive_location']}/{config['hive_dest']}"

    # Final output result file
    config["sql_results_path"] = f"{config['base_path']}/Outputs/" + \
        os.getenv('RESULTS_SUB_DIRECTORY')
    # os.getenv('RESULTS_SUB_DIRECTORY', "raygun_ip_addresses_summary")

    # Hive metastore URIs
    config["hive_metastore_uri"] = os.getenv(
        'HIVE_METASTORE_URI', "thrift://metastore:9083")

    # Spark App name
    config["spark_appname"] = os.getenv(
        'SPARK_APPNAME', "LakehouseLocalStack")

    # Spark driver memory
    # "3g" for small files it's better 2-3g
    # "12g" for big files with more data it's better 4-5g
    config["spark_driver_memory"] = os.getenv('SPARK_DRIVER_MEMORY', "3g")

    # Repartition the DataFrame to optimize parallel processing
    # and memory usage.
    # (Adjust the number of partitions based on your environment and data size,
    #  workload and cluster setup)
    config["df_num_partitions"] = int(os.getenv('DF_NUM_PARTITIONS', '200'))

    # Define the batch size to read into dataframe the JSON files in batches
    # (Adjust the batch size based on your memory capacity and data size)
    config["df_read_batch_size"] = int(os.getenv('DF_READ_BATCH_SIZE', '5000'))

    # Dataframe cluster storage bucket prefix (directory)
    config["df_cluster_storage_bucket_prefix"] = os.getenv(
        'df_cluster_storage_bucket_prefix', "ClusterData/RaygunIpSummary")

    config["df_output_s3_path"] = f"{config['s3_protocol']}://" + \
        f"{config['minio_bucket_name']}/" + \
        config['df_cluster_storage_bucket_prefix']

    # Dataframe output format
    config["df_output_format"] = os.getenv('DF_OUTPUT_FORMAT', "parquet")

    # Dataframe compression format
    config["df_compression_format"] = os.getenv(
        'DF_COMPRESSION_FORMAT', "snappy")

    # Include header in the Dataframe
    config["df_input_header"] = os.getenv('DF_INPUT_HEADER', True)
    config["df_input_header"] = \
        False if config["df_input_header"] == '0' else True

    if show_params:
        print("")
        print("Minio Access Key:", config["minio_access_key"])
        print("Minio Secret Key:", config["minio_secret_key"])
        print("Minio Endpoint:", config["minio_endpoint"])
        print("Minio Bucket Name:", config["minio_bucket_name"])

        print("")
        print("Base path:", config["base_path"])
        print("Testing iteractions:",
              config["testing_iteractions"] or "PRODUCTION")
        print("Input local directory:", config["local_directory"])
        print("S3 page size:", config["s3_page_size"])
        print("S3 read timeout:", config["s3_read_timeout"])
        print("S3 prefix:", config["s3_prefix"])
        print("Desired attribute:", config["desired_attribute"])
        print("Desired alias:", config["desired_alias"])
        print("")
        print("Hive batches:", config["hive_batches"])
        print("Hive location:", config["hive_location"])
        print("Hive destination:", config["hive_dest"])
        print("Hive metastore URI:", config["hive_metastore_uri"])
        print("Results directory path:", config["sql_results_path"])
        print("")

        print("Spark App name:", config["spark_appname"])
        print("Spark driver memory:", config["spark_driver_memory"])
        print("Dataframe number of partitions (for repartitioning):",
              config["df_num_partitions"])
        print("Dataframe read batch size:", config["df_read_batch_size"])
        print("Minio dataframe cluster bucket prefix:",
              config["df_cluster_storage_bucket_prefix"])
        print("Dataframe output S3 path:", config["df_output_s3_path"])
        print("Dataframe output format:", config["df_output_format"])
        print("Dataframe compression format:", config["df_compression_format"])
        print("Dataframe input header:", config["df_input_header"])
        print("")

        config["mode"] = os.environ.get('MODE', None)
        config["resume_from"] = os.environ.get('FROM', "")
        if config["mode"]:
            print("Mode:", config["mode"])
        if config["resume_from"]:
            print("Ingest from:", config["resume_from"])

    return config


def get_datetime():
    """
    Get the current date and time in Eastern time zone and format it as
    YYYY-MM-DD HH:MM:SS.
    """
    # Get the current date and time in Eastern time zone
    eastern_tz = timezone(timedelta(hours=-5), name='EST')
    current_datetime = datetime.now(eastern_tz)
    # Format the date and time as YYYY-MM-DD HH:MM:SS
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_datetime


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


def get_spark_session(config: dict) -> SparkSession.Builder:
    """
    Get a Spark session.
    """
    spark = SparkSession.builder \
        .appName(config["spark_appname"]) \
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
            config["spark_driver_memory"]) \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    # IMPORTANT:
    # .master("local[*]") \
    # Is equivalent to:
    #
    # Iver:
    # https://sparkbyexamples.com/spark/what-does-setmaster-local-mean-in-spark/
    # from pyspark.conf import SparkConf
    # from pyspark.context import SparkContext
    # conf = SparkConf()
    # conf.setMaster("local[*]")

    return spark


def close_spark_session(spark):
    """
    Close a Spark session.
    """
    spark.stop()


def list_files_minio(config: dict, resume_from: int) -> list:
    """
    List files in a Minio bucket under a specific prefix using boto3.
    """

    own_resume_from = resume_from

    # Iver: To filter the files fro a specific Key
    # kwargs = {‘Bucket’: bucket,
    #           ‘Prefix’: prefix,
    #           ‘StartAfter’: start_after}
    # message_entries = []
    # try:
    #     response = S3_CLIENT.list_objects_v2(**kwargs)

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
    j = -1
    for page in page_iterator:
        j += 1
        print(f"{j}) Current files: {len(files)}" +
              f" | Files to append: {len(page.get('Contents', []))}")
        first_file = 1
        if own_resume_from > 0:
            if own_resume_from > ((j+1)*config["s3_page_size"]):
                print(f"{j}) Skipping from {(j*config['s3_page_size'])+1}" +
                      f"-{(j+1)*config['s3_page_size']}" +
                      f" until {own_resume_from}...")
                continue
            else:
                first_file = own_resume_from - \
                    ((j)*config["s3_page_size"])
                print(f"Starting from {first_file} | " +
                      f"originally: {own_resume_from} | " +
                      f"initial sequence: {(j)*config['s3_page_size']} ...")
                own_resume_from = -1
        curr_file = 1
        for obj in page.get('Contents', []):
            if curr_file < first_file:
                # print(f"Skipping {curr_file} | {obj['Key']}")
                curr_file += 1
                continue
            # print(f"Appending {curr_file} | {obj['Key']}")
            files.append(obj['Key'])
            first_file += 1
            curr_file += 1
        if config["testing_iteractions"] and \
           j >= config["testing_iteractions"]:
            break

    print(f"Final total files to process: {len(files)}")
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
# sh /home/LocalLakeHouse/Project/Scripts/1.init_minio.sh data/raygun

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
    if not config:
        return False

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
    if not spark:
        return False

    # Check spark configuration

    if config["debug"]:
        print("")
        print(">> Checking spark configuration...")
        pprint.pprint(spark.sparkContext.getConf().getAll())

    # Verify resuming options
    resume_from = config["resume_from"]
    if config["mode"] == "resume":
        resume_from = hive_verification(spark)
        # Start from the next file after the total files already processed
        resume_from += 1
    if not resume_from:
        # Process from scratch
        resume_from = -1
    else:
        # Resume from the specified file number
        resume_from = int(resume_from)

    ########################
    # Step 1.4: Upload Multiple JSON Files to MinIO
    ########################

    print("")
    print(">> Uploading Multiple JSON Files to MinIO...")
    print("")

    # MinIO bucket name for Raw files
    bucket_name = config["minio_bucket_name"]

    # Path to the JSON files in MinIO
    json_files_path = f"{config['s3_protocol']}://{bucket_name}/" + \
        f"{config['s3_prefix']}/"

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

    # Create the bucket for Raw files if it doesn't exist
    try:
        create_bucket = not minio_client.bucket_exists(bucket_name)
    except S3Error as err:
        print(err)
        print(f"Creating bucket {bucket_name}")
        create_bucket = True
    except Exception as err:
        print(err)
        return False

    if not create_bucket:
        print(f"Bucket {bucket_name} already exists. Skipping...")
    else:
        minio_client.make_bucket(bucket_name)
        # Upload JSON files to MinIO
        for filename in os.listdir(config["local_directory"]):
            if filename.endswith(".json"):
                file_path = os.path.join(config["local_directory"], filename)
                minio_client.fput_object(bucket_name, filename, file_path)
                print(f"Uploaded {filename} to {bucket_name}")

    # Verify Dataframes bucket prefix existence
    df_cluster_bucket_prefix = config['df_cluster_storage_bucket_prefix']
    # Verify if the prefix exists in the MinIO bucket
    objects = minio_client.list_objects(
        bucket_name,
        prefix=df_cluster_bucket_prefix,
        recursive=False)
    create_bucket = not any(objects)

    if resume_from <= 0:
        if not create_bucket:
            print(f"Bucket {config['df_cluster_storage_bucket_prefix']}" +
                  " already exists. Erasing its content...")
            minio_client.remove_object(
                bucket_name,
                df_cluster_bucket_prefix,
            )
            create_bucket = True

    if create_bucket:
        # Create the prefix directory in the Minio bucket
        try:
            minio_client.put_object(
                bucket_name,
                f"{df_cluster_bucket_prefix}/.gitkeep",
                data=io.BytesIO(b""),
                length=0
            )
            print(f"Created prefix directory {df_cluster_bucket_prefix}" +
                  " in bucket {bucket_name}")
        except S3Error as err:
            print("")
            print("Failed to create prefix directory " +
                  f"{df_cluster_bucket_prefix} in bucket {bucket_name}: {err}")
            print("")
            return False
        except Exception as err:
            print("")
            print("An unexpected error occurred creating prefix directory" +
                  f" {df_cluster_bucket_prefix} in bucket {bucket_name}:")
            print(err)
            print("")
            return False

    ########################
    ########################
    # Step 2: Read Multiple JSON Files from MinIO
    ########################
    ########################

    # IMPORTANT: It's recomended to perform this step using "1.init_minio.sh"
    # for a better performance for a large number of files.

    # Read JSON files into DataFrame

    print("")
    print(">> READING FILES INTO DATAFRAME")
    print("")

    # Read JSON files into DataFrame selecting all attributes
    # df = spark.read.option("multiline", "true").json(json_files_path)

    # Read JSON files into DataFrame and select only the desired attribute
    # df = spark.read.option("multiline", "true") \
    #     .json(json_files_path) \
    #     .select(config["desired_attribute"])

    # Read JSON files into DataFrame by batches (better for large file sets)
    # Optimize the spark.read.option().json() file processing
    # by first getting a list of all file paths in the directory,
    # then use spark.read.json() to read all files IN CHUCKS
    # of `df_read_batch_size`

    print("")
    print(">> List files in Minio bucket under a prefix using boto3...")

    init_ts = show_curr_datetime()
    file_list = list_files_minio(config, resume_from)

    # Convert to the format read by Spark
    file_list = [f"{config['s3_protocol']}://{bucket_name}/{file_name}"
                 for file_name in file_list]
    show_curr_datetime(init_ts)

    print(f"Number of files: {len(file_list)}")

    print("")
    print(">> Reading JSON files into DataFrame...")
    print(f"(Set 'df' from: {json_files_path})")
    print(f"(In chucks of: {config['df_read_batch_size']})")

    # Create a new schema from the raygun json structure
    # https://sparkbyexamples.com/pyspark/pyspark-create-an-empty-dataframe/
    # from pyspark.sql.types import StructType, StructField, StringType
    # if config["desired_attribute"]:
    #     df_schema = StructType([
    #         StructField(config["desired_attribute"], StringType(), True),
    #     ])
    # else:
    #     df_schema = StructType([
    #         StructField("Error.Message", StringType(), True),
    #         StructField("Error.ClassName", StringType(), True),
    #         StructField("Error.FileName", StringType(), True),
    #         StructField("Error.StackTrace", StringType(), True),
    #         StructField("MachineName", StringType(), True),
    #         StructField("Request.HostName", StringType(), True),
    #         StructField("Request.Url", StringType(), True),
    #         StructField("Request.HttpMethod", StringType(), True),
    #         StructField("Request.IpAddress", StringType(), True),
    #         StructField("Request.QueryString", StringType(), True),
    #         StructField("Request.Headers", StringType(), True),
    #         StructField("Request.Data", StringType(), True),
    #     ])
    # df_final = spark.createDataFrame([], df_schema)

    # Process the files in batches
    init_ts = show_curr_datetime()
    j = 0
    for i in range(0, len(file_list), config["df_read_batch_size"]):
        j += 1
        print(f"{get_datetime()} - {j}) From: {i} | " +
              f"To: {i+config['df_read_batch_size']}")
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

        # Write the chunck to the spark cluster disk (when cluster dies,
        # disk will be erased unless it's written on S3)
        print(f"{get_datetime()} - Persisting DataFrame to disk" +
              f" (round {j})...")
        df \
            .write \
            .mode("append") \
            .format(config["df_output_format"]) \
            .option("compression", config["df_compression_format"]) \
            .save(config["df_output_s3_path"],
                  header=config["df_input_header"])

        # Call specific function to process data
        # process_data(df)

        # Finish the eventaul test
        if config["testing_iteractions"] and \
           j >= config["testing_iteractions"]:
            break

    print("")
    print("*************************")
    print("* Ingestion Process end *")
    print("*************************")
    show_curr_datetime(init_ts)

    # Finish the process by creating the Hive metastore
    hive_process(spark, None, start_time)

    close_spark_session(spark)

    return True


def hive_verification(spark: SparkSession.Builder = None):

    # Load environment variables
    config = get_config()
    if not config:
        return False

    # Start the timer
    print("")
    print("********************************************")
    print("*** HIVE Process pre-verification begins ***")
    print("********************************************")
    start_time = show_curr_datetime()

    # Initialize Spark session
    if not spark:
        close_spark = True
        spark = get_spark_session(config)
        if not spark:
            return False
    else:
        close_spark = False

    print("")
    print(f"{get_datetime()} - Calculating processed rows...")
    df = spark.read.load(config["df_output_s3_path"])
    if config["desired_alias"]:
        rows_count = df.select(config["desired_alias"]).count()
    else:
        rows_count = df.select("*").count()
    print("")
    print(f"{get_datetime()} - Processed rows until now:")
    print(rows_count)
    print("")

    show_curr_datetime(start_time)

    if close_spark:
        close_spark_session(spark)

    return rows_count


def hive_process(spark: SparkSession.Builder = None, df: DataFrame = None,
                 start_time: float = None):

    # Load environment variables
    config = get_config(df is None)
    if not config:
        return False

    # Start the timer
    print("")
    print("***************************")
    print("*** HIVE Process begins ***")
    print("***************************")
    if not start_time:
        start_time = show_curr_datetime()

    ########################
    # Step 1.3: Setup spark
    ########################

    # Initialize Spark session

    print("")
    print(">> Initializing Spark session...")
    print("")

    # Initialize Spark session
    if not spark:
        close_spark = True
        spark = get_spark_session(config)
        if not spark:
            return False
    else:
        close_spark = False

    # # Load a table as a DataFrame
    # default_db = spark.catalog.listDatabases()[0]
    # table_name = "raygun_error_traces"
    # df = spark.table(f"{default_db.name}.{table_name}")

    # Check if any previous data exists
    if df:
        print("Resume spark processing from the last Dataframe...")
    else:
        # if spark.catalog.isCached("cached_df"):
        #     # Retrieve the cached DataFrame
        #     print("Loading cached DataFrame...")
        #     df = spark.catalog.getCachedDataFrame("cached_df")
        #     # You can now work with the cached_df DataFrame without
        #     # re-reading the JSON files
        # else:
        #     print("No cached DataFrame found...")
        #     return False
        #     # Initialize an empty DataFrame
        #     # df = spark.createDataFrame([], schema=None)

        # Para retormar....
        df = spark.read.load(config["df_output_s3_path"])

        # Repartition the DataFrame to optimize parallel processing and
        # memory usage.
        # (Adjust the number of partitions based on your environment and
        #  data size)
        df = df.repartition(config["df_num_partitions"])

    if not start_time:
        start_time = show_curr_datetime()

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

    # Adjust the number of partitions as necessary
    df_flattened = df_flattened.repartition(config["df_num_partitions"])

    # from pyspark.sql.functions \
    #    import monotonically_increasing_id, concat, lit

    # Add a salt column for high-cardinality column to distribute writes
    # df_flattened = df_flattened.withColumn(
    #     "salt",
    #     # concat(col("HighCardinalityColumn"), lit("_"),
    #     concat(col(config["desired_alias"]), lit("_"),
    #            (monotonically_increasing_id() % \
    #            config["df_num_partitions"]).cast("string")))
    # df_flattened = df_flattened.repartition("salt")

    # Set properties to manage memory better during shuffle and write
    # align with the number of repartitions
    spark.conf.set("spark.sql.shuffle.partitions", config["df_num_partitions"])

    # adjust the number of records in memory before spilling to disk
    spark.conf.set("spark.sql.files.maxRecordsPerBatch", "500")

    # Save the processed data into Hive table

    # Write in batches
    print(f"Split data processing into {config['hive_batches']} batches")
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

    print("Rows count")
    print("")
    spark.sql("SELECT count(*) FROM raygun_error_traces") \
        .show(truncate=False)

    # Stop the timer
    print("")
    print("****************************")
    print("* Save to Hive Process end *")
    print("****************************")
    show_curr_datetime(start_time)

    if close_spark:
        close_spark_session(spark)

    return True


def get_spark_query(sql: str = None):
    """
    This function should return a summary of the data processing performed.
    Get the IP addresses summary
    """

    config = get_config(False)
    if not config:
        return False
    spark = get_spark_session(config)
    if not spark:
        return False

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

    close_spark_session(spark)


def get_trino_query(sql: str = None):
    ########################
    ########################
    # Step 5: Query with Trino
    ########################
    ########################

    # Connect to Trino

    # With trino-python-client
    # https://github.com/trinodb/trino-python-client

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


def get_spark_content():

    # To get the database/schema name, you can use:
    # 1. spark.catalog.listDatabases() to list all databases
    # 2. spark.catalog.listTables("database_name") to list tables in a database
    # 3. df.printSchema() to print the schema of a DataFrame

    config = get_config()
    if not config:
        return False
    spark = get_spark_session(config)
    if not spark:
        return False

    # Get the current_schema() from spark?
    current_database = spark.catalog.currentDatabase()
    print(f"Current database: {current_database}")

    try:
        # Check if any previous cached data exists
        if spark.catalog.isCached("cached_df"):
            # Retrieve the cached DataFrame
            print("Loading cached DataFrame...")
            df = spark.catalog.getCachedDataFrame("cached_df")
            # You can now work with the cached_df DataFrame without re-reading
            # the JSON files
            print("")
            print("Dataframe schema structure:")
            df.printSchema()
            # Show content (only a few)
            # df = spark.sql("SELECT * FROM cached_df LIMIT 10")
            df.show()
        else:
            print("No cached DataFrame found [1]...")
            # Initialize an empty DataFrame
            # df = spark.createDataFrame([], schema=None)
    except AnalysisException as e:
        print(f"Error: {e}")
        print("No cached DataFrame found [2]...")
    except Exception as e:
        print(f"Error: {e}")
        print("No cached DataFrame found [3]...")

    # Hive metastore databases
    db_list = spark.catalog.listDatabases()
    print(f"Databases: {db_list}")

    # R:
    # Databases: [Database(name='default', catalog='spark_catalog',
    #    description='Default Hive database',
    #    locationUri='file:/opt/hive/data/warehouse')]

    # Get the default database
    default_db = spark.catalog.listDatabases()[0]
    print(f"Default database: {default_db.name}")

    # List tables in the default database
    tables = spark.catalog.listTables(default_db.name)
    print(f"Tables in {default_db.name}:")
    for table in tables:
        print(f"- {table.name}")

    close_spark_session(spark)


if __name__ == "__main__":
    # Get mode parameter
    # mode = sys.argv[1] if len(sys.argv) > 1 else None
    mode = os.environ.get('MODE', None)
    sql = os.environ.get('SQL', None)
    if mode == "ingest" or mode == "resume":
        if ingest():
            get_spark_query(sql)
    elif mode == "spark_sql":
        get_spark_query(sql)
    elif mode == "trino_sql":
        get_trino_query(sql)
    elif mode == "hive_process":
        hive_process()
    elif mode == "hive_verification":
        hive_verification()
    elif mode == "spark_content":
        get_spark_content()
    else:
        print("")
        print("Invalid mode parameter.")
        print("Available options: ingest, resume, hive_process, " +
              "hive_verification, spark_sql, trino_sql")
        print("E.g.")
        print("To execute the complete ingestion process:")
        print("  MODE=ingest make raygun_ip_processing")
        print("To resume the ingestion from the last file processed")
        print("  MODE=resume make raygun_ip_processing")
        print("To resume the ingestion process from the file number: 70000")
        print("  MODE=ingest FROM=70000 make raygun_ip_processing")
        print("To check how many files has been processed iin the Dataframe" +
              " (before the Hive tables population):")
        print("  MODE=hive_verification make raygun_ip_processing")
        print("To execute the Hive tables population process" +
              " (after a stopped ingestion):")
        print("  MODE=hive_process make raygun_ip_processing")
        print("To get the ingestion results using the default SQL query" +
              " and spark:")
        print("  MODE=spark_sql make raygun_ip_processing")
        print("To get the ingestion results using a pecified SQL query" +
              " and spark:")
        print("  SQL='SELECT RequestIpAddress FROM raygun_error_traces " + 
              "GROUP BY RequestIpAddress' MODE=spark_sql " +
              "make spark_sql")
        print("To get the ingestion results using the default SQL query" +
              " and trino:")
        print("  MODE=trino_sql make raygun_ip_processing")
        print("")
        sys.exit(1)
