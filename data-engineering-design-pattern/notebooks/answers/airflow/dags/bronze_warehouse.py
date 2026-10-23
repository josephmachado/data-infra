# file: airflow/dags/bronze_warehouse.py
import time

import pendulum
from airflow.sdk import Asset, Metadata, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pyspark.sql import Row, SparkSession

from notebooks.bronze.warehouse import run as warehouse_run

bronze_warehouse = Asset(
    "file://opt/spark/warehouse/local/bronze/warehouse/part-*.parquet"
)  # Defines bronze.order_lines as a data asset


@dag(
    dag_id="bronze.warehouse",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze", "event-driven", "multi-event-driven"],
)
def bronze_warehouse_pipeline():
    @task(retries=2, retry_delay=2, outlets=[bronze_warehouse])
    def run_bronze_warehouse_etl() -> None:
        spark = (
            SparkSession.builder.appName("bronze.warehouse")
            .master("local[*]")
            .getOrCreate()
        )

        warehouse_run(spark)

    run_bronze_warehouse_etl()


bronze_warehouse_pipeline()
