# .DEFAULT_GOAL := local
# .PHONY: tests
SHELL := /bin/bash

# General Commands
help:
	cat Makefile

install:
	sh ./Scripts/0.download_dependencies.sh

data_unzip:
	cd data
	unzip moves.zip
	unzip pokemon.zip
	unzip types.zip

run:
	docker-compose up --attach spark

open_local_jupiter:
	open http://127.0.0.1:8888/lab

open_local_minio:
	open http://127.0.0.1:9001

open_local_trino:
	open http://127.0.0.1:8081

open_pyspark_ui:
	docker exec -ti spark pyspark

raygun_ip_processing:
	bash batch_processing/010_raygun_ip_processing.sh
