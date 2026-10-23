import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pyspark.sql import Row, SparkSession

from notebooks.silver.fct_orders import run as fct_orders_run


@dag(
    dag_id="silver.fct_orders",
    schedule=CronDataIntervalTimetable(
        "0 * * * *",  # every minute
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["silver", "orders"],
)
def silver_fct_orders_pipeline():
    @task(retries=2, retry_delay=2)
    def run_silver_fct_orders_etl() -> None:
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        spark = (
            SparkSession.builder.appName("silver_fct_orders")
            .master("local[*]")
            .getOrCreate()
        )
        print(f"Starting pipeline for time range {start_time} to {end_time}")

        fct_orders_run(spark, start_time, end_time)

    run_silver_fct_orders_etl()


silver_fct_orders_pipeline()
