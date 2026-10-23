from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.silver.dim_campaign"


def extract(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "campaign": spark.table("local.bronze.campaign"),
        "ad_group": spark.table("local.bronze.ad_group"),
    }


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    campaign = input_dfs["campaign"].select(
        F.col("campaign_id"),
        F.col("advertiser_id"),
        F.col("name").alias("campaign_name"),
        F.col("objective"),
        F.col("budget_total"),
        F.col("budget_daily"),
        F.col("start_date"),
        F.col("end_date"),
        (
            (F.current_date() >= F.col("start_date"))
            & (F.current_date() <= F.col("end_date"))
        ).alias("is_active"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias(
            "campaign_duration_days"
        ),
        F.col("created_at"),
        F.col("updated_at"),
    )

    ad_group = input_dfs["ad_group"].select(
        F.col("ad_group_id"),
        F.col("campaign_id"),
        F.col("name").alias("ad_group_name"),
        F.col("targeting_type"),
        F.col("bid_strategy"),
        F.col("max_cpc"),
        F.col("status"),
        (F.col("status") == "active").alias("is_active"),
        F.col("bid_strategy").isin("cpc", "manual_cpc").alias("is_cpc_bidding"),
        F.col("created_at"),
        F.col("updated_at"),
    )

    ad_groups_struct = ad_group.groupBy("campaign_id").agg(
        F.collect_list(
            F.struct(
                F.col("ad_group_id"),
                F.col("ad_group_name"),
                F.col("targeting_type"),
                F.col("bid_strategy"),
                F.col("max_cpc"),
                F.col("status"),
                F.col("is_active"),
                F.col("is_cpc_bidding"),
                F.col("created_at"),
                F.col("updated_at"),
            )
        ).alias("ad_groups")
    )

    return campaign.join(ad_groups_struct, on="campaign_id", how="left")


def load(output_df: DataFrame) -> None:
    output_df.writeTo(TABLE_NAME).createOrReplace()


def run(spark: SparkSession) -> None:
    load(transform(extract(spark)))


if __name__ == "__main__":
    spark = SparkSession.builder.appName(TABLE_NAME).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    run(spark)
