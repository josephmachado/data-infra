#!/bin/bash
set -e

# Activate virtual environment
source /home/airflow/.venv/bin/activate

# Start Airflow in the background
airflow standalone &

# Create data 

cd /home/airflow/datagen/ && uv sync && uv run datagen --customers 1000 --start 2025-01-01 --end 2025-12-08 --output postgres://dataengineer:datapipeline@postgres:5432/ecommerce

cd /home/airflow

# Start Jupyter Lab in the foreground
exec jupyter lab --allow-root --ip=0.0.0.0 --no-browser --IdentityProvider.token=''
