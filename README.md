# Data Lakehouse local stack

**Data Lakehouse local stack with PySpark, Trino, and Minio**

This repository aims to introduce the Data Lakehouse pattern as a suitable and flexible solution to transit small companies to established enterprises, allowing to implement a local Data Lakehouse from OpenSource solutions, compatible with Cloud production grade tools.

## Introduction

In the world of ML and AI, **data** is the crown jewel, but it's normally lost in [Swamps](https://www.superannotate.com/blog/data-lakes-vs-data-swamps-vs-data-warehouse) due to bad practices with **Data Lakes** when companies try to productionize their data.

**Data Warehouses** are costly solutions for this problem, and increase the complexity of simple Lakes.

Here's where **Data Lakehouses** come into action, being a hybrid solution with the best of both worlds. ([source](https://2024.pycon.co/en/talks/23)).

**Data Lakehouses** aim to combine elements of data warehousing with core elements of the data lake. Put simply, they are designed to provide the lower costs of cloud storage even for large amounts of raw data alongside support for certain analytics concepts – such as SQL access to curated and structured data tables stored in relational databases, or support for large scale processing of Big Data analytics or machine learning workloads ([source](https://www.exasol.com/resource/data-lake-warehouse-or-lakehouse/)).

<!--
![Common DatalakeHouse technologies](./images/1676637608474.png)<BR/>
([Image source](https://www.linkedin.com/pulse/lakehouse-convergence-data-warehousing-science-dr-mahendra/))
-->

![Common DatalakeHouse technologies](./images/data_lakehouse_new.png)<BR/>
([Image source](https://www.databricks.com/glossary/data-lakehouse))

## The Medallion Architecture

A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.

![The Medallion Architecture](./images/building-data-pipelines-with-delta-lake-120823.png)<BR/>
([Image source](https://www.databricks.com/glossary/medallion-architecture))

* Bronze: ingestion tables (raw data, originals).

* Silver: refined/cleaned tables.

* Gold: feature/aggregated data store.

* Platinum (optional): in a faster format like a high-speed DBMS, because `gold` is stored in cloud bucket storage (like AWS S3(), and it's slow for e.g. real-time dashboard.

Readings:

* https://www.databricks.com/glossary/medallion-architecture
* https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion
* https://medium.com/@junshan0/medallion-architecture-what-why-and-how-ce07421ef06f

## Data Lakehouse Components

![DatalakeHouse components](./images/1_xuE8_N_LxoP49S1Pu5Wn5A.webp)<BR/>
([Image source](https://medium.com/adfolks/data-lakehouse-paradigm-of-decade-caa286f5b7a1))

<!--
![Common DatalakeHouse technologies](./images/Data_Lake_vs_Data_Warehouse.webp)<BR/>
([Image source](https://www.montecarlodata.com/blog-data-lake-vs-data-warehouse))
-->

* **Apache Spark**<BR/>
  Processing Layer.<BR/>
  [https://spark.apache.org](https://spark.apache.org)<BR/>
  [https://spark.apache.org/docs/latest/api/python/index.html](https://spark.apache.org/docs/latest/api/python/index.html)<BR/>

* **Minio**<BR/>
  Landing buckets and data storage layer.<BR/>
  [https://min.io](https://min.io)

* **Apache Hive**<BR/>
  Data catalog.<BR/>
  [https://hive.apache.org](https://hive.apache.org)

* **Postgres Database**<BR/>
  Data catalog persistence.<BR/>
  [https://www.postgresql.org](https://www.postgresql.org)

* **Delta Lake** (open table format)<BR/>
  [https://delta.io](https://delta.io)<BR/>
  Open source framework developed by Databricks. Like other modern table formats, it employs file-level listings, improving the speed of queries considerably compared to the  directory-level listing of Hive. Offers enhanced CRUD operations, including the ability to update and delete records in a data lake which would previously have been immutable.<BR/>
  (Click [here](https://www.starburst.io/data-glossary/open-table-formats/) for more information about Open Table Formats).<BR/>

* **Trino**<BR/>
  Query engine and data governance.<BR/>
  [https://trino.io](https://trino.io)

## Other Components

* **SQL Alchemy**<BR/>
  [https://docs.sqlalchemy.org](https://docs.sqlalchemy.org)

* **Pandas**<BR/>
  [https://pandas.pydata.org](https://pandas.pydata.org)

* **Jupiter Lab**<BR/>
  [https://docs.jupyter.org](https://docs.jupyter.org)

## Requirements

* [Git](https://www.atlassian.com/git/tutorials/install-git)
* Make: [Mac](https://formulae.brew.sh/formula/make) | [Windows](https://stackoverflow.com/questions/32127524/how-to-install-and-use-make-in-windows)
* [Docker and Docker Composer](https://www.docker.com/products/docker-desktop)
* [wget](https://www.jcchouinard.com/wget-install/)

## Usage

Clone the respository:

```bash
git clone https://github.com/tomkat-cr/data_lakehouse_local_stack.git
cd data_lakehouse_local_stack
```

Download the required packages:

```bash
make install
```

**IMPORTANT**: this process will take a long time, depending on your Internet connection speed.

Start the local stack (or `spark stack`):

```bash
make run
```

Run the local Jupiter engine:

```bash
make open_local_jupiter
```

Run the local Minio explorer:

```bash
make open_local_minio
```

## Raygun Data Processing

To process a single Raygun error you need to download the Raygun data, composed by a series of JSON files for each time  the error happend with all data shown in Raygun, upload the data to S3 (managed locally by Minio), ingest the data using Spark, build the Hive metastore and finally run the SQL queries to get the data analytics.

### Raygun Data Preparation

1. Go to Raygun ([https://app.raygun.com](https://app.raygun.com)), and select the corresponding Application.

2. Put the check mark in the error you want to analyze.

3. Click on the `Export selected groups` button.

4. Click on the `Add to export list` button.

5. A message like this will be shown:

```
Great work! Those error groups have been added to the export list.
View your exports by clicking here, or by using the "Export" link under "Crash Reporting" in the sidebar.
```

6. Under `Crash Reporting` in the sidebar, click on the `Export` link.

7. Click on the `Start export` button.

```
Confirm export

Export all errors between XXXX and YYYY.

Exports can take some time depending on the volume of errors being exported. You will be notified when your export is ready to be downloaded. Once an export is started, another cannot begin until the first has completed.

Exports are generated as a single compressed file. [Learn how to extract them](https://raygun.com/documentation/product-guides/crash-reporting/exporting-data/)

Recipients:
example@address.com
```

8. Click on the `Start export` button.

9. Wait until the compressed file arraives to your email inbox.

10. A message arrives to your inbox like this:

```
Subject: Your Raygun Crash Reporting export is complete for "XXXX"

Your error export has been generated
We have completed the error group export for your application "XXXX". You can now download it below.
Download export - XXX MB
Learn how to extract 7z files
```

11. Click on the `Download export - XXX MB` link.

12. Put the compressed file in a local directory like: `${HOME}/Downloads/raygun-data`

13. Uncompress the file.

14. A new directory will be created with the JSON files, each one with a error for a date/time, in the directory: `${HOME}/Downloads/raygun-data/assets`

15. To check the input files size:

Set an environment variable with the path:

```sh
ORIGINAL_JSON_FILES_PATH="${HOME}/Downloads/raygun-data/assets"
```

Get the total size for all files downloaded:

```sh
du -sh ${ORIGINAL_JSON_FILES_PATH}
```

Count the number of files in that directory:

```sh
ls -l ${ORIGINAL_JSON_FILES_PATH} | wc -l
```

16. Move the files to the `data/raygun` directory in the Project, or perform the `Large number of input data files` procedure (se next section) to link the `${HOME}/Downloads/raygun-data/assets` to the `data/raygun` directory.

### Large number of input data files

If you have more than 1000 raw data input files, you can use the following procedure to mount the input files directory in the local stack `data` directory:

1. Edit the docker-compose configuration file in the project's root:

```sh
vi ./docker-compose.yml
```

2. Add the `data/any_directory_name` input files directory in the `volumnes` section, changing `any_directory_name` with the name of yours, e.g. `raygun`:

File: `./docker-compose.yml`

```yaml
version: "3.9"
services:
  spark:
      .
      .
    volumes:
      - /path/to/input/directory/:/home/PyCon2024/Project/data/any_directory_name
```

So your massive input files will be under the `data/any_directory_name` directory.

3. You can also do it by a symbolic link:

```sh
ln -s /path/to/input/directory/ data/any_directory_name
```

4. Once you finish the ingestion process (see `Raygun Data Ingestion` section), the link can be removed:

Exit the `spark` docker container pressing Ctrl-D (or running the `exit` command), and run:

```sh
unlink data/any_directory_name
```

### Raygun Data Copy to S3 / Minio

4. In a terminal window, restart the `spark stack`:

If the local stack is already running, press Ctrl-C to stop it.

Run this command:

```sh
make run
```

The spark stak docker container will have a `/home/PyCon2024/Project` directory that's the Project's root directory in your local computer.

5. Open a second terminal window and enter to the `spark` docker container:

```sh
docker exec -ti spark bash
```

6. Then run the load script:

```sh
cd /home/PyCon2024/Project
sh ./Scripts/1.init_minio.sh data/raygun
```

### Raygun Data Ingestion

1. Run the ingest process:

Open a terminal window and enter to the `spark` docker container:

```sh
docker exec -ti spark bash
```

Run the injestion process from scratch:

```sh
cd Project
MODE=ingest make raygun_ip_processing
```

When the ingestion process ends, the `Build the Hive Metastore` and `Raygun Data Reporting` will run as well.


2. If the process stops, copy the counter XXXX after the last `Persisting...` message:

For example:

```
Persisting DataFrame to disk (round X)...
3) From: XXXX | To: YYYY
```

Then run:

```sh
MODE=ingest FROM=XXXX make raygun_ip_processing
```

### Build the Hive Metastore

If the igestion process stops and you don't want to resume/finish it, you can run the Hive metastore building, and be able to run the SQL queries and build the reports:

```sh
MODE=hive_process make raygun_ip_processing
```

### Raygun Data Reporting

1. To run the default SQL query using Spark:

```sh
MODE=spark_sql make raygun_ip_processing
```

This process generates a report in the Project's `Outputs/raygun_ip_addresses_summary` directory, where you will find a `part-00000-<some-headecimal-code>.csv` file with the results.

The default SQL query that generates the results file is:

```sql
SELECT RequestIpAddress, COUNT(*) AS IpCount
  FROM raygun_error_traces
  GROUP BY RequestIpAddress
  ORDER BY IpCount DESC
```

The result will be the list of IP addresses and number of times those IPs are in all the Requests evaluated.

2. To run a custom SQL query using Spark:

```sh
SQL='SELECT RequestIpAddress FROM raygun_error_traces GROUP BY RequestIpAddress' MODE=spark_sql make raygun_ip_processing
```

3. To run the default SQL query using Trino:

```sh
MODE=trino_sql make raygun_ip_processing
```

### Ingest process stats

1. To check the Dataframe cluster S3 (Minio) files:

This way you can do the ingestion process follow-up, because during the dataframe build step in in the ingest process, files are processed in batchs (groups of 5,000 files) and each time a batch finises, the result is written to the S3 cluster directory. If the ingestion process stops, it can be resumed from the ending point of last executed batch, and the processed files will be appended to the S3 cluster directory.

Set an environment variable with the path:

```sh
CLUSTER_DIRECTORY="/home/PyCon2024/Project/Storage/minio/data-lakehouse/ClusterData/RaygunIpSummary"
```

The parquet files resulting from the dataframe build part in the ingestion process. are in the path assigned to `CLUSTER_DIRECTORY`.

Get the total size for all files currently processed in the Dataframe:

```sh
du -sh ${CLUSTER_DIRECTORY}
```

Count the number of files in that directory:

```sh
ls -l ${CLUSTER_DIRECTORY} | wc -l
```

2. To check the raw input JSON files uploaded to S3 (Minio):

Set an environment variable with the path:

```sh
S3_JSON_FILES_PATH="/home/PyCon2024/Project/Storage/minio/data-lakehouse/Raw/raygun"
```

Get the total size for all files:

```sh
du -sh ${S3_JSON_FILES_PATH}
```

Count the number of files in that directory:

```sh
ls -l ${S3_JSON_FILES_PATH} | wc -l
```

## Minio UI

1. Run the local Minio explorer:

```bash
make open_local_minio
```

2. Automatically this URL will be opened in a Browser: [http://127.0.0.1:9001](http://127.0.0.1:9001)

3. The credentials for the login are in the `minio.env` file:<BR/>
   Username (`MINIO_ACCESS_KEY`): minio_ak<BR/>
   Password (`MINIO_SECRET_KEY`): minio_sk<BR/>

## Monitor Spark processes

To access the `pyspark` web UI:

1. Run the following command in a terminal window:

```sh
make open_pyspark_ui
```

It runs the following command:

```sh
docker exec -ti spark pyspark
```

2. Go this URL in a Browser: [http://127.0.0.1:4040](http://127.0.0.1:4040)

## Run the local Jupiter Notebooks

1. Run the local Jupiter engine:

```bash
make open_local_jupiter
```

2. Automatically this URL will be opened in a Browser: [http://127.0.0.1:8888/lab](http://127.0.0.1:8888/lab)

3. A screen will appear asking for the  `Password or token` to authenticate.<BR/>
   It can be found in the  `docker attach` screen (the one that stays running when you execute `make run` to start the `spark stack`).

3. Seach for a message like this:<BR/>
    `http://127.0.0.1:8888/lab?token=xxxx`

4. The `xxxx` is the `Password or token` to authenticate.

## Connect to the Jupiter Server in VSC

To connect the Jupiter Server in VSC (Visual Studio Code):

1. In the docker attach screen, look for a message like this:<BR/>
    `http://127.0.0.1:8888/lab?token=xxxx`

2. The `xxxx` is the password to be used when the Jupyter Kernel Connection ask for it...

2. Then select the `Existing Jupiter Server` option.

3. Specify the URL: `http://127.0.0.1:8888`

4. Specify the password copied in seconf step: `xxxx`

5. Select the desired Kernel from the list

The VSC will be connected to the Jupiter Server.

## Pokemon Data preparation

To prepare the data for the Pockemon Data Ingestion:

1. Download the compressed files from: [https://github.com/alejogm0520/lakehouses-101-pycon2024/tree/main/data](https://github.com/alejogm0520/lakehouses-101-pycon2024/tree/main/data)

2. Copy those files to the `data` directory.

3. Decompress the example data files:

```bash
cd data
unzip moves.zip
unzip pokemon.zip
unzip types.zip
```

### Jupiter notebooks

* [Raygun data ingestion](notebooks/Raygun-data-ingestion.ipynb)

* [Pockemon data ingestion](notebooks/Pokemon-data-ingestion.ipynb)

## License

This is a open-sourced software licensed under the [MIT](LICENSE) license.

## Credits

This project is maintained by [Carlos J. Ramirez](https://www.carlosjramirez.com).

It was forked from the [Data Lakehouse 101](https://github.com/alejogm0520/lakehouses-101-pycon2024) repository made by [Alejandro Gómez Montoya](https://github.com/alejogm0520).

For more information or to contribute to the project, visit [Data Lakehouse 101 on GitHub](https://github.com/tomkat-cr/lakehouses-101-pycon2024).

Happy Coding!
