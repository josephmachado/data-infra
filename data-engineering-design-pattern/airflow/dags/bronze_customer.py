# file: airflow/dags/bronze_customer.py
import time

import pendulum
from airflow.sdk import dag, task
from pyspark.sql import Row, SparkSession

from notebooks.bronze.customer import run as customer_run


@dag(
    dag_id="bronze.customer",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze"],
)
def bronze_customer_pipeline():
    @task(retries=2, retry_delay=2)
    def run_bronze_customer_etl() -> None:
        spark = (
            SparkSession.builder.appName("bronze.customer")
            .master("local[*]")
            .getOrCreate()
        )

        customer_run(spark)

    run_bronze_customer_etl()


bronze_customer_pipeline()
