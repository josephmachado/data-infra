# file: airflow/dags/bronze_customer.py
import time

import pendulum
from airflow.sdk import Asset, Metadata, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pyspark.sql import Row, SparkSession

from notebooks.bronze.order_lines import run as order_lines_run

bronze_order_lines = Asset(
    "file://opt/spark/warehouse/local/bronze/order_lines/part-*.parquet"
)  # Defines bronze.order_lines as a data asset


@dag(
    dag_id="bronze.order_lines",
    schedule=CronDataIntervalTimetable(
        "0 * * * *",  # 0th minute of every hour
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze", "event-driven", "multi-event-driven"],
)
def bronze_order_lines_pipeline():
    @task(
        outlets=[bronze_order_lines]
    )  # <-- marks the asset as updated on task success
    def run_bronze_order_lines_etl() -> None:
        spark = (
            SparkSession.builder.appName("bronze.order_lines")
            .master("local[*]")
            .getOrCreate()
        )

        order_lines_run(spark)
        # getting start and end time to send downstream to act as its time range
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        yield Metadata(
            bronze_order_lines, {"start_time": start_time, "end_time": end_time}
        )

    run_bronze_order_lines_etl()


bronze_order_lines_pipeline()
