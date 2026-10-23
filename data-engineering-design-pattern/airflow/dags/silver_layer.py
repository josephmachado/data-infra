import pendulum
from airflow.sdk import Asset, Metadata, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pyspark.sql import Row, SparkSession

from notebooks.capstone_project.silver.dim_campaign import run as dim_campaign_run
from notebooks.capstone_project.silver.fct_ad_clicks import run as fct_ad_clicks_run

from notebooks.capstone_project.silver.dim_advertiser import (
    run as dim_advertiser_run,
)
from notebooks.capstone_project.silver.fct_ad_conversions import (
    run as fct_ad_conversions_run,
)
from notebooks.capstone_project.silver.fct_ad_impressions import (
    run as fct_ad_impressions_run,
)


@dag(
    dag_id="capstone_silver_layer",
    schedule=CronDataIntervalTimetable(
        "0 0 1 1 *",  # every year
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["silver", "capstone_project"],
)
def silver_capstone_pipeline():
    @task(retries=2, retry_delay=2)
    def run_dim_advertiser():
        spark = (
            SparkSession.builder.appName("run_dim_advertiser")
            .master("local[*]")
            .getOrCreate()
        )
        dim_advertiser_run(spark)

    @task(retries=2, retry_delay=2)
    def run_dim_campaign():
        spark = (
            SparkSession.builder.appName("run_dim_campaign")
            .master("local[*]")
            .getOrCreate()
        )
        dim_campaign_run(spark)

    @task(retries=2, retry_delay=2)
    def run_fct_ad_impressions():
        spark = (
            SparkSession.builder.appName("run_fct_ad_impressions")
            .master("local[*]")
            .getOrCreate()
        )
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        fct_ad_impressions_run(spark, start_time, end_time)

    @task(retries=2, retry_delay=2)
    def run_fct_ad_clicks():
        spark = (
            SparkSession.builder.appName("run_fct_ad_clicks")
            .master("local[*]")
            .getOrCreate()
        )
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        fct_ad_clicks_run(spark, start_time, end_time)

    @task(retries=2, retry_delay=2)
    def run_fct_ad_conversions():
        spark = (
            SparkSession.builder.appName("run_fct_ad_conversions")
            .master("local[*]")
            .getOrCreate()
        )
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        fct_ad_conversions_run(spark, start_time, end_time)

    # Listing them without dependencies = parallel execution
    [
        run_dim_advertiser(),
        run_dim_campaign(),
        run_fct_ad_impressions(),
        run_fct_ad_clicks(),
        run_fct_ad_conversions(),
    ]


silver_capstone_pipeline()
