import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pyspark.sql import Row, SparkSession

from notebooks.silver.fct_order_lines import run as fct_order_lines_run


@dag(
    dag_id="silver.fct_order_lines",
    schedule=CronDataIntervalTimetable(
        "0 0 1 * *",  # on 1st of every month at 00:00 time
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,  # only one run at a time, in order
    tags=["silver", "order_lines"],
)
def silver_fct_order_lines_pipeline():
    @task(retries=2, retry_delay=2)
    def run_silver_fct_order_lines_etl() -> None:
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        spark = (
            SparkSession.builder.appName("silver_fct_orders")
            .master("local[*]")
            .getOrCreate()
        )
        print(f"Starting pipeline for time range {start_time} to {end_time}")

        fct_order_lines_run(spark, start_time, end_time)

    run_silver_fct_order_lines_etl()


silver_fct_order_lines_pipeline()

