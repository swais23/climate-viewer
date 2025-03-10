# NOAA Airflow Pipeline

## Overview
This project sets up an Apache Airflow pipeline to ingest, transform, and store NOAA climate data. The pipeline consists of multiple DAGs that handle daily ingestion, backfilling, and lookup table management.

## Docker Setup
This project uses Docker Compose to manage Airflow and a PostgreSQL database for storing transformed data.

The `docker-compose.yaml` file is based on the official Airflow documentation ([link](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)), with modifications to include a `db` service for storing data in PostgreSQL.

### Commands
- **Build the containers:** `make build`
- **Start services:** `make up`
- **Stop services:** `make down`
- **Purge the database:** `make purge-database`
  - This stops and removes the database container, rebuilds it, and starts it again.
- **Compile Python requirements:** `make compile-requirements`
  - Uses `pip-compile` to generate an updated `requirements.txt`, ensuring compatibility with Airflow version `2.9.2`.

## Database Initialization
A PostgreSQL initialization script is included in this project to set up the database schema and required tables. This script:
- Creates necessary schemas (`reporting`, `raw`, `lookup`).
- Defines tables for NOAA daily data, reporting views, and lookup information (stations, states, and countries).
- Adds relevant indexes for optimized query performance.
- Enables PostGIS for geospatial data support.

The script runs automatically when the PostgreSQL container starts for the first time, ensuring that the database is ready to store NOAA climate data.

## DAGs

### `noaa_daily_dag`
This DAG processes NOAA raw climate data from an S3 bucket, extracts relevant information, and transforms it into a structured format.

**Tasks:**
1. `noaa_raw_load` - Loads raw daily NOAA data using DuckDB.
2. `noaa_pivot_data` - Transforms raw data into a structured format in PostgreSQL.

### `noaa_backfill_dag`
This DAG allows historical NOAA data backfilling of a user-defined date range.

**Tasks:**
1. `noaa_raw_load` - Loads raw historical NOAA data using DuckDB.
2. `noaa_pivot_data` - Transforms raw data into a structured format in PostgreSQL.

### `noaa_lookup_dag`
This DAG manages lookup tables (stations, states, and countries) to support NOAA data enrichment.

**Tasks:**
1. `noaa_raw_sensor` - Waits for raw data availability.
2. Freshness checks for `stations`, `countries`, and `states` tables.
3. Conditional loading of missing lookup data.

## Configuration
Key configurations include:
- `NOAA_RAW_COLUMNS`: Defines schema for raw NOAA data.
- `NOAA_ELEMENTS`: Maps NOAA climate elements to user-friendly column names and conversion factors.
- `NOAA_LOOKUP_CONFIG`: Defines lookup table structure and sources.

## Utilities
- `get_noaa_url(year)`: Returns the NOAA data URL for a given year.
- `get_db_conn()`: Generates a database connection string for DuckDB.
- `get_noaa_pivot_case_statements(elements)`: Generates SQL case statements for pivot transformation.