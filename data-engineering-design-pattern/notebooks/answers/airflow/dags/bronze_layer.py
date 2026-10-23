import pendulum
from airflow.sdk import dag, task
from pyspark.sql import Row, SparkSession

from notebooks.bronze.customer import run as customer_run
from notebooks.bronze.customer_address import run as customer_address_run
from notebooks.bronze.order_lines import run as order_lines_run
from notebooks.bronze.orders import run as orders_run
from notebooks.bronze.payment import run as payment_run
from notebooks.bronze.product import run as product_run
from notebooks.bronze.product_attribute import run as product_attribute_run
from notebooks.bronze.product_variant import run as product_variant_run


@dag(
    dag_id="bronze_layer",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze"],
)
def bronze_customer_pipeline():
    @task(retries=2, retry_delay=2)
    def run_customer():
        spark = (
            SparkSession.builder.appName("run_customer")
            .master("local[*]")
            .getOrCreate()
        )
        customer_run(spark)

    @task(retries=2, retry_delay=2)
    def run_customer_address():
        spark = (
            SparkSession.builder.appName("run_customer_address")
            .master("local[*]")
            .getOrCreate()
        )
        customer_address_run(spark)

    @task(retries=2, retry_delay=2)
    def run_order_lines():
        spark = (
            SparkSession.builder.appName("run_order_lines")
            .master("local[*]")
            .getOrCreate()
        )
        order_lines_run(spark)

    @task(retries=2, retry_delay=2)
    def run_orders():
        spark = (
            SparkSession.builder.appName("run_orders").master("local[*]").getOrCreate()
        )
        orders_run(spark)

    @task(retries=2, retry_delay=2)
    def run_payment():
        spark = (
            SparkSession.builder.appName("run_payment").master("local[*]").getOrCreate()
        )
        payment_run(spark)

    @task(retries=2, retry_delay=2)
    def run_product_attribute():
        spark = (
            SparkSession.builder.appName("run_product_attribute")
            .master("local[*]")
            .getOrCreate()
        )
        product_attribute_run(spark)

    @task(retries=2, retry_delay=2)
    def run_product_variant():
        spark = (
            SparkSession.builder.appName("run_product_variant")
            .master("local[*]")
            .getOrCreate()
        )
        product_variant_run(spark)

    @task(retries=2, retry_delay=2)
    def run_product():
        spark = (
            SparkSession.builder.appName("run_product").master("local[*]").getOrCreate()
        )
        product_run(spark)

    # Listing them without dependencies = parallel execution
    [
        run_customer(),
        run_customer_address(),
        run_order_lines(),
        run_orders(),
        run_payment(),
        run_product_attribute(),
        run_product_variant(),
        run_product(),
    ]


bronze_customer_pipeline()
