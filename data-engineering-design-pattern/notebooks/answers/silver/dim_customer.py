import argparse
from pyspark.sql import DataFrame, SparkSession

TABLE_NAME = "local.silver.dim_customer"


def extract(spark: SparkSession) -> dict[str, DataFrame]:
    customer_df = spark.table("local.bronze.customer")
    customer_address_df = spark.table("local.bronze.customer_address")
    return {"customer_df": customer_df, "customer_address_df": customer_address_df}


def transform(input_dfs: dict[str, DataFrame], spark: SparkSession) -> DataFrame:
    input_dfs["customer_df"].createOrReplaceTempView("customer")
    input_dfs["customer_address_df"].createOrReplaceTempView("customer_address")

    return spark.sql("""
        SELECT
            c.customer_id,
            c.email,
            c.full_name,
            c.phone,
            c.status,
            c.created_at,
            c.updated_at,
            COLLECT_LIST(
                STRUCT(
                    ca.is_default,
                    CONCAT(ca.line1, ', ', ca.city, ', ', ca.state, ', ', ca.country) AS address
                )
            ) AS addresses
        FROM customer c
        LEFT JOIN customer_address ca USING (customer_id)
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    """)


def load(output_df: DataFrame) -> None:
    output_df.writeTo(TABLE_NAME).createOrReplace()


def run(spark: SparkSession) -> None:
    load(transform(extract(spark), spark))


if __name__ == "__main__":
    spark = SparkSession.builder.appName(TABLE_NAME).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    run(spark)
