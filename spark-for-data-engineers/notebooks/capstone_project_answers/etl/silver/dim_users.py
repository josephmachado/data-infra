import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def extract(spark):
    logger.info("Getting Upstream Data")
    return {"users": spark.read.table("stackoverflow.users")}


def transform(spark, upstream_data):
    logger.info("Transforming Upstream Data")

    return spark.sql(
        """
    Select *
    from {users}
    """,
        users=upstream_data["users"],
    )


def load(spark, table_df, table_name):
    logger.info(f"Loading Data into {table_name}")
    spark.sql(f"drop table if exists stackoverflow.{table_name}")
    table_df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table_name}")

def data_quality_check(spark, transformed_data):
    logger.info("No DQ Checks")
    return True

def run(spark, table_name="dim_users"):
    upstream_data = extract(spark)
    transformed_data = transform(spark, upstream_data)
    if not data_quality_check(spark, transformed_data):
        raise Exception("Data Quality Check Failed")
    load(spark, transformed_data, table_name)


if __name__ == "__main__":
    executor_memory = "8g"
    executor_cores = 4
    num_executors = 2
    table_name = "dim_users"

    spark = (
        SparkSession.builder.appName(table_name)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", executor_cores)
        .config("spark.executor.instances", num_executors)
        .config("spark.cores.max", executor_cores * num_executors)
        .getOrCreate()
    )

    run(spark, table_name)
