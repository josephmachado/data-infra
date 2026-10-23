import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.silver.dim_product"


def extract(
    spark: SparkSession
) -> dict[str, DataFrame]:
    product_df = spark.table("local.bronze.product")
    return {"product": product_df}


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    return input_dfs["product"].select(
        F.col("product_id"),
        F.col("name").alias("product_name"),
        F.col("brand"),
        F.col("base_price"),
        F.col("status"),
        F.col("updated_at"),
        F.col("created_at"),
    )


def load(output_df: DataFrame, spark: SparkSession) -> None:
    if not spark.catalog.tableExists(TABLE_NAME):
        output_df.withColumn("valid_from", F.col("created_at")).withColumn(
            "valid_to", F.lit(None).cast("timestamp")
        ).withColumn("is_current", F.lit(True)).writeTo(TABLE_NAME).createOrReplace()
        return

    output_df.createOrReplaceTempView("staged")

    spark.sql(f"""
        WITH products_with_updates AS (
            SELECT b.*
            FROM staged b
            JOIN {TABLE_NAME} d
                ON b.product_id = d.product_id
            WHERE b.updated_at > d.updated_at
            AND d.is_current = true
        )
        MERGE INTO {TABLE_NAME} t
        USING (
            SELECT product_id AS join_key, * FROM staged
            UNION ALL
            SELECT NULL AS join_key, * FROM products_with_updates
        ) s
        ON t.product_id = s.join_key

        WHEN MATCHED
            AND t.is_current = true
            AND s.updated_at > t.updated_at
        THEN UPDATE SET
            t.is_current = false,
            t.valid_to = s.updated_at

        WHEN NOT MATCHED
        THEN INSERT (
            product_id,
            product_name,
            brand,
            base_price,
            status,
            updated_at,
            created_at,
            valid_from,
            valid_to,
            is_current
        ) VALUES (
            s.product_id,
            s.product_name,
            s.brand,
            s.base_price,
            s.status,
            s.updated_at,
            s.created_at,
            s.updated_at,
            NULL,
            TRUE
        )

        WHEN NOT MATCHED BY SOURCE
            AND t.is_current = true
        THEN UPDATE SET
            t.is_current = false,
            t.valid_to = current_timestamp()
    """)


def run(spark: SparkSession) -> None:
    output_df = transform(extract(spark))
    load(output_df, spark)


if __name__ == "__main__":
    spark = SparkSession.builder.appName(TABLE_NAME).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    run(spark)
