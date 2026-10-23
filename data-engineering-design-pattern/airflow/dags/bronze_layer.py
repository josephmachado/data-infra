import pendulum
from airflow.sdk import dag, task
from pyspark.sql import Row, SparkSession
from notebooks.capstone_project.bronze.ad_click import run as ad_click_run
from notebooks.capstone_project.bronze.ad_conversion import run as ad_conversion_run
from notebooks.capstone_project.bronze.ad_creative import run as ad_creative_run
from notebooks.capstone_project.bronze.ad_group import run as ad_group_run
from notebooks.capstone_project.bronze.ad_impression import run as ad_impression_run
from notebooks.capstone_project.bronze.advertiser import run as advertiser_run
from notebooks.capstone_project.bronze.campaign import run as campaign_run


@dag(
    dag_id="capstone_bronze_layer",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze", "capstone_project"],
)
def bronze_capstone_pipeline():
    @task(retries=2, retry_delay=2)
    def run_ad_click():
        spark = (
            SparkSession.builder.appName("run_ad_click")
            .master("local[*]")
            .getOrCreate()
        )
        ad_click_run(spark)

    @task(retries=2, retry_delay=2)
    def run_ad_conversion():
        spark = (
            SparkSession.builder.appName("run_ad_conversion")
            .master("local[*]")
            .getOrCreate()
        )
        ad_conversion_run(spark)

    @task(retries=2, retry_delay=2)
    def run_ad_creative():
        spark = (
            SparkSession.builder.appName("run_ad_creative")
            .master("local[*]")
            .getOrCreate()
        )
        ad_creative_run(spark)

    @task(retries=2, retry_delay=2)
    def run_ad_group():
        spark = (
            SparkSession.builder.appName("run_ad_group")
            .master("local[*]")
            .getOrCreate()
        )
        ad_group_run(spark)

    @task(retries=2, retry_delay=2)
    def run_ad_impression():
        spark = (
            SparkSession.builder.appName("run_ad_impression")
            .master("local[*]")
            .getOrCreate()
        )
        ad_impression_run(spark)

    @task(retries=2, retry_delay=2)
    def run_advertiser():
        spark = (
            SparkSession.builder.appName("run_advertiser")
            .master("local[*]")
            .getOrCreate()
        )
        advertiser_run(spark)

    @task(retries=2, retry_delay=2)
    def run_campaign():
        spark = (
            SparkSession.builder.appName("run_campaign")
            .master("local[*]")
            .getOrCreate()
        )
        campaign_run(spark)

    # Listing them without dependencies = parallel execution
    [
        run_ad_click(),
        run_ad_conversion(),
        run_ad_creative(),
        run_ad_group(),
        run_ad_impression(),
        run_advertiser(),
        run_campaign(),
    ]


bronze_capstone_pipeline()
