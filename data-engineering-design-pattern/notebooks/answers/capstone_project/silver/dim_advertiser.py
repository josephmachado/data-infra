from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.silver.dim_advertiser"


def extract(spark: SparkSession) -> dict[str, DataFrame]:
    return {"advertiser": spark.table("local.bronze.advertiser")}


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    return input_dfs["advertiser"].select(
        F.col("advertiser_id"),
        F.col("name").alias("advertiser_name"),
        F.col("billing_email"),
        F.col("status"),
        (F.col("status") == "active").alias("is_active"),
        F.col("created_at"),
        F.col("updated_at"),
    )


def load(output_df: DataFrame) -> None:
    output_df.writeTo(TABLE_NAME).createOrReplace()


def run(spark: SparkSession) -> None:
    load(transform(extract(spark)))


if __name__ == "__main__":

    spark = SparkSession.builder.appName(TABLE_NAME).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    run(spark)  # add start and end time for incremental pipelines
