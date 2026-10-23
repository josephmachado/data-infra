import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.silver.fct_orders"


def extract(
    spark: SparkSession,
    start_time: str,
    end_time: str,
) -> dict[str, DataFrame]:
    order_df = spark.sql(f"""
    SELECT * 
    FROM local.bronze.orders
    WHERE created_at >= '{start_time}'
    AND created_at < '{end_time}'
    """)

    payment_df = spark.sql(f"""
    SELECT * 
    FROM local.bronze.payment
    """)

    return {"order": order_df, "payment": payment_df}


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:

    payment_df = input_dfs["payment"].select(
        F.col("order_id"),
        F.col("payment_id"),
        F.col("method").alias("payment_method"),
        F.col("amount").alias("payment_amount"),
        F.col("status").alias("payment_status"),
        F.col("provider_ref"),
        F.col("paid_at"),
    )
    return (
        input_dfs["order"]
        .join(payment_df, on="order_id", how="left")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("shipping_addr_id"),
            F.col("billing_addr_id"),
            F.col("status").alias("order_status"),
            F.col("total_amount"),
            F.col("placed_at"),
            F.col("payment_id"),
            F.col("payment_method"),
            F.col("payment_amount"),
            F.col("payment_status"),
            F.col("provider_ref"),
            F.col("paid_at"),
            F.col("created_at"),
            F.col("updated_at"),
            (F.col("payment_status") == "completed").alias("is_paid"),
            F.datediff(F.col("paid_at"), F.col("placed_at")).alias("days_to_payment"),
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
