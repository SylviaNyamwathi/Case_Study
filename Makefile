# Convenience targets. `make all` runs everything end to end from a clean clone.
#
# PySpark needs a JVM (Java 17 or 21). Spark memory is kept modest so this runs
# on a laptop; override with DRIVER_MEMORY=4g make all.

SHELL         := /bin/bash
PYTHON        ?= python3
DBT_DIR       := dbt/kenya_airways
DRIVER_MEMORY ?= 2g
SHUFFLE_PARTITIONS ?= 8

export DRIVER_MEMORY
export SHUFFLE_PARTITIONS

.PHONY: all install bronze silver pipeline dbt-build dbt-docs test demo rerun clean help warehouse-dir

all: pipeline dbt-build test ## Run the whole pipeline, build the models, run the tests

install: ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements.txt

bronze: ## Bronze ingestion only
	$(PYTHON) pyspark/bronze/bronze_ingest.py

silver: ## Silver transform only
	$(PYTHON) pyspark/silver/silver_transform.py

pipeline: ## Bronze + Silver
	$(PYTHON) pyspark/run_pipeline.py

warehouse-dir: # DuckDB will not create its parent directory itself
	@mkdir -p data/warehouse

dbt-build: warehouse-dir ## Build every dbt model and run all tests
	cd $(DBT_DIR) && DBT_PROFILES_DIR=. dbt build

dbt-docs: warehouse-dir ## Generate dbt documentation
	cd $(DBT_DIR) && DBT_PROFILES_DIR=. dbt docs generate

test: ## Unit tests (Silver rules + schema contract)
	$(PYTHON) -m pytest tests -v

demo: ## Prove the reject path and the incremental path with a second snapshot
	$(PYTHON) scripts/make_dq_demo_file.py
	$(PYTHON) pyspark/run_pipeline.py \
		--source-file data/raw/samples/flights_dq_demo.csv \
		--as-of-date 2026-09-10
	$(MAKE) dbt-build

rerun: ## Rerun day one - proves idempotency (row count must not change)
	$(PYTHON) pyspark/run_pipeline.py --as-of-date 2026-09-09 --force

clean: ## Remove the generated lake, warehouse and dbt artefacts
	rm -rf data/lake data/warehouse
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
