import pendulum
from airflow.sdk import Asset, Metadata, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pyspark.sql import Row, SparkSession

from notebooks.capstone_project.gold.obt_ad_impressions import (
    run as obt_ad_impressions_run,
)
from notebooks.capstone_project.gold.ad_funnel_summary import (
    run as ad_funnel_summary_run,
)
from notebooks.capstone_project.gold.ad_campaign_performance import (
    run as ad_campaign_performance_run,
)


@dag(
    dag_id="capstone_gold_layer",
    schedule=CronDataIntervalTimetable(
        "0 0 1 1 *",  # every year
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,
    tags=["silver", "capstone_project"],
)
def gold_capstone_pipeline():
    @task(retries=2, retry_delay=2)
    def run_obt_ad_impressions():
        spark = (
            SparkSession.builder.appName("run_fct_ad_impressions")
            .master("local[*]")
            .getOrCreate()
        )
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        obt_ad_impressions_run(spark, start_time, end_time)

    @task(retries=2, retry_delay=2)
    def run_ad_funnel_summary():
        spark = (
            SparkSession.builder.appName("run_fct_ad_clicks")
            .master("local[*]")
            .getOrCreate()
        )
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        ad_funnel_summary_run(spark, start_time, end_time)

    @task(retries=2, retry_delay=2)
    def run_ad_campaign_performance():
        spark = (
            SparkSession.builder.appName("run_fct_ad_conversions")
            .master("local[*]")
            .getOrCreate()
        )
        context = get_current_context()

        start_time = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        ad_campaign_performance_run(spark, start_time, end_time)

    run_obt_ad_impressions() >> [run_ad_campaign_performance(), run_ad_funnel_summary()]


gold_capstone_pipeline()
