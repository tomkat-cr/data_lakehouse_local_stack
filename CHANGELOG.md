# CHANGELOG

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/) and [Keep a Changelog](http://keepachangelog.com/).



## Unreleased
---

### New

### Changes

### Fixes

### Breaks


## 0.0.4 (2024-06-25)
---

### New
Add: show date/time and time elapsed for each step.

### Changes
Parametrize raygun_ip_processing, set processing.env and stablish Output directory for results file(s).
get_data was renamed to get_pockemon_data.

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
