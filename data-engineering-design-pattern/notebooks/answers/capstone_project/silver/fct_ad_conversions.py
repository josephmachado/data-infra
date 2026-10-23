# fct_ad_conversions.py
import argparse
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.silver.fct_ad_conversions"


def extract(
    spark: SparkSession, start_time: str, end_time: str
) -> dict[str, DataFrame]:
    return {
        "ad_conversion": spark.table("local.bronze.ad_conversion").filter(
            (F.col("created_at") >= start_time) & (F.col("created_at") < end_time)
        )
    }


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    return input_dfs["ad_conversion"].select(
        F.col("conversion_id"),
        F.col("click_id"),
        F.col("order_id"),
        F.col("revenue"),
        F.col("attribution"),
        F.col("converted_at"),
        F.col("created_at"),
        (F.col("revenue") * 100).cast("long").alias("revenue_in_cents"),
    )


def load(output_df: DataFrame, spark: SparkSession) -> None:
    if not spark.catalog.tableExists(TABLE_NAME):
        (
            output_df.writeTo(TABLE_NAME)
            .partitionedBy(F.partitioning.days("created_at"))
            .createOrReplace()
        )
    else:
        output_df.writeTo(TABLE_NAME).overwritePartitions()


def run(spark: SparkSession, start_time: str, end_time: str) -> None:
    load(transform(extract(spark, start_time, end_time)), spark)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"{TABLE_NAME} ETL")
    parser.add_argument(
        "--start-time",
        required=True,
        help="Start time (inclusive), format: YYYY-MM-DD HH:MM:SS",
    )
    parser.add_argument(
        "--end-time",
        required=True,
        help="End time (exclusive), format: YYYY-MM-DD HH:MM:SS",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName(TABLE_NAME).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    run(spark, args.start_time, args.end_time)
