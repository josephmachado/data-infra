import pendulum
from airflow.sdk import Asset, dag, get_current_context, task
from pyspark.sql import Row, SparkSession

from notebooks.silver.fct_order_lines import run as fct_order_lines_run

bronze_order_lines = Asset(
    "file://opt/spark/warehouse/local/bronze/order_lines/part-*.parquet"
)  # Defines bronze.order_lines as a data asset


@dag(
    dag_id="silver.fct_order_lines_et",
    schedule=[bronze_order_lines],  # <-- triggered by asset updates, not by time
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,  # only one run at a time, in order
    tags=["silver", "order_lines", "event-driven"],
)
def silver_fct_order_lines_pipeline():
    @task(retries=2, retry_delay=2, inlets=[bronze_order_lines])
    def run_silver_fct_order_lines_etl() -> None:
        spark = (
            SparkSession.builder.appName("silver_fct_orders")
            .master("local[*]")
            .getOrCreate()
        )

        # Get start and end times from the upstream's metadata
        context = get_current_context()
        inlet_events = context["inlet_events"][bronze_order_lines]
        latest_event = inlet_events[-1]
        start_time = latest_event.extra.get("start_time")
        end_time = latest_event.extra.get("end_time")
        print(f"Starting pipeline for time range {start_time} to {end_time}")

        fct_order_lines_run(spark, start_time, end_time)

    run_silver_fct_order_lines_etl()


silver_fct_order_lines_pipeline()
