import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.silver.fct_order_lines"


def extract(
    spark: SparkSession,
    start_time: str,
    end_time: str,
) -> dict[str, DataFrame]:
    order_line_df = spark.sql(f"""
    SELECT * 
    FROM local.bronze.order_lines
    WHERE created_at >= '{start_time}'
    AND created_at < '{end_time}'
    """)
    return {"order_line": order_line_df}


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    return input_dfs["order_line"].select(
        F.col("order_line_id"),
        F.col("order_id"),
        F.col("variant_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amt"),
        F.col("line_total"),
        F.col("created_at"),
        F.col("updated_at"),
        (F.col("discount_amt") > 0).alias("is_discounted"),
        (F.col("unit_price") - F.col("discount_amt")).alias("effective_unit_price"),
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
