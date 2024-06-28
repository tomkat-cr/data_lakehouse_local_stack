# CHANGELOG

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/) and [Keep a Changelog](http://keepachangelog.com/).



## Unreleased
---

### New

### Changes

### Fixes

### Breaks


## 0.0.6 (2024-06-28)
---

### New
Automatic resume using MODE=resume.
"hive_verification" process to check how many files has been processed iin the Dataframe (before the Hive tables population).

### Changes
/home/PyCon2024 directory renamed to /home/LocalLakeHouse.
Include the data lakehouse layers description in README.


## 0.0.5 (2024-06-26)
---

### New
Add resume options.
Open 4040 port to run pyspark web UI with `docker exec -ti spark pyspark` in the docker-composer configuration.
Add `make open_pyspark_ui` option.
Add instructions to preare data and run the raygun ingestion process.


## 0.0.4 (2024-06-25)
---

### New
Add: parametrize raygun_ip_processing
Add: processing.env and processing.example.env
Add: Output directory for results file(s).
Add: TESTING_ITERACTIONS to test all raygun_ip_processing procedire before release and live data run.
Add: show date/time and time elapsed for each step.

### Changes
get_data was renamed to get_pockemon_data.
raygun_ip_processing divided in functions, separating the ingestion process from the queries.

### Fixes
Fix: Optimize the spark.read.json() file processing by first getting a list of all file paths in the directory, then use spark.read.json() to read files IN CHUCKS of `batch_size`.
Fix: flatten the nested structure for easier analysis operation was optimized by repartition the DataFrame to optimize parallel processing and memory usage, persist the DataFrame to be used multiple times, then unpersist the DataFrame to free up memory, and finally trigger garbage collection to avoid memory issues.
Fix: optimize save the processed data into Hive table by perofmr the write in batches.


## 0.0.3 (2024-06-24)
---

### New
Add: missing directories
Add: batch processing of massive Raygun JSON files to be processed outside the Jupiter notebook


## 0.0.2 (2024-06-21)
---

### New
Add: Raygun data ingestion


## 0.0.1 (2024-06-11)
---

### New
Initial fork from https://github.com/alejogm0520/lakehouses-101-pycon2024
