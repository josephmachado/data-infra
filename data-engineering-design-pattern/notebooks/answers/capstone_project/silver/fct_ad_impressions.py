# fct_ad_impressions.py
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.silver.fct_ad_impressions"


def extract(
    spark: SparkSession, start_time: str, end_time: str
) -> dict[str, DataFrame]:
    impressions_df = spark.table("local.bronze.ad_impression").filter(
        (F.col("created_at") >= start_time) & (F.col("created_at") < end_time)
    )
    ad_creative_df = spark.table("local.bronze.ad_creative")
    return {
        "ad_impression": impressions_df,
        "ad_creative": ad_creative_df,
    }


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    ad_impression_df = input_dfs["ad_impression"]
    ad_creative_df = input_dfs["ad_creative"]

    return (
        ad_impression_df.alias("ai")
        .join(ad_creative_df.alias("ac"), on="creative_id", how="left")
        .select(
            F.col("ai.impression_id"),
            F.col("ai.creative_id"),
            F.col("ac.ad_group_id"),
            F.col("ai.session_id"),
            F.col("ai.customer_id"),
            F.col("ai.placement"),
            F.col("ai.cost"),
            F.col("ai.impressed_at"),
            F.col("ai.created_at"),
            (F.col("ai.cost") * 100).cast("long").alias("cost_in_cents"),
        )
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
