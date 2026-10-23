import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "local.gold.obt_ad_impressions"


def extract(
    spark: SparkSession, start_time: str, end_time: str
) -> dict[str, DataFrame]:
    impressions_df = spark.table("local.silver.fct_ad_impressions").filter(
        (F.col("created_at") >= start_time) & (F.col("created_at") < end_time)
    )
    clicks_df = spark.table("local.silver.fct_ad_clicks").filter(
        (F.col("created_at") >= start_time) & (F.col("created_at") < end_time)
    )
    conversions_df = spark.table("local.silver.fct_ad_conversions").filter(
        (F.col("created_at") >= start_time) & (F.col("created_at") < end_time)
    )
    return {
        "impressions": impressions_df,
        "clicks": clicks_df,
        "conversions": conversions_df,
        "dim_advertiser": spark.table("local.silver.dim_advertiser"),
        "dim_campaign": spark.table(
            "local.silver.dim_campaign"
        ),  # includes ad_groups array
    }


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    """
    Build wide OBT at impression grain by joining:
      - fct_ad_impressions (base, already has ad_group_id from Silver)
      - fct_ad_clicks (LEFT JOIN on impression_id)
      - fct_ad_conversions (LEFT JOIN on click_id)
      - dim_campaign exploded on ad_groups array to recover ad_group-level fields
      - dim_advertiser (via advertiser_id)

    Enrichment:
      - is_clicked: impression resulted in a click
      - is_converted: click resulted in a conversion
      - total_cost: impression_cost + click_cost
    """
    impressions = input_dfs["impressions"].select(
        F.col("impression_id"),
        F.col("creative_id"),
        F.col("ad_group_id"),
        F.col("session_id"),
        F.col("customer_id"),
        F.col("placement"),
        F.col("cost").alias("impression_cost"),
        F.col("cost_in_cents").alias("impression_cost_in_cents"),
        F.col("impressed_at"),
        F.col("created_at"),
    )
    clicks = input_dfs["clicks"].select(
        F.col("click_id"),
        F.col("impression_id"),
        F.col("cost").alias("click_cost"),
        F.col("cost_in_cents").alias("click_cost_in_cents"),
        F.col("clicked_at"),
    )
    conversions = input_dfs["conversions"].select(
        F.col("conversion_id"),
        F.col("click_id"),
        F.col("order_id"),
        F.col("revenue").alias("conversion_revenue"),
        F.col("revenue_in_cents").alias("conversion_revenue_in_cents"),
        F.col("attribution"),
        F.col("converted_at"),
    )

    # Explode ad_groups array to get one row per ad_group, then select fields from struct
    dim_campaign_exploded = (
        input_dfs["dim_campaign"]
        .select(
            F.col("campaign_id"),
            F.col("advertiser_id"),
            F.col("campaign_name"),
            F.col("objective"),
            F.col("budget_total"),
            F.col("budget_daily"),
            F.col("start_date"),
            F.col("end_date"),
            F.col("is_active").alias("campaign_is_active"),
            F.col("campaign_duration_days"),
            F.explode("ad_groups").alias("ag"),
        )
        .select(
            F.col("campaign_id"),
            F.col("advertiser_id"),
            F.col("campaign_name"),
            F.col("objective"),
            F.col("budget_total"),
            F.col("budget_daily"),
            F.col("start_date"),
            F.col("end_date"),
            F.col("campaign_is_active"),
            F.col("campaign_duration_days"),
            # unpack ad_group struct fields
            F.col("ag.ad_group_id").alias("ad_group_id"),
            F.col("ag.ad_group_name").alias("ad_group_name"),
            F.col("ag.targeting_type").alias("targeting_type"),
            F.col("ag.bid_strategy").alias("bid_strategy"),
            F.col("ag.max_cpc").alias("max_cpc"),
            F.col("ag.is_active").alias("ad_group_is_active"),
            F.col("ag.is_cpc_bidding").alias("is_cpc_bidding"),
        )
    )

    dim_advertiser = input_dfs["dim_advertiser"].select(
        F.col("advertiser_id"),
        F.col("advertiser_name"),
        F.col("is_active").alias("advertiser_is_active"),
    )

    return (
        impressions.join(clicks, on="impression_id", how="left")
        .join(conversions, on="click_id", how="left")
        .join(dim_campaign_exploded, on="ad_group_id", how="left")
        .join(dim_advertiser, on="advertiser_id", how="left")
        .select(
            # keys
            F.col("impression_id"),
            F.col("click_id"),
            F.col("conversion_id"),
            F.col("session_id"),
            F.col("customer_id"),
            F.col("creative_id"),
            F.col("ad_group_id"),
            F.col("campaign_id"),
            F.col("advertiser_id"),
            F.col("order_id"),
            # timestamps
            F.col("impressed_at"),
            F.col("clicked_at"),
            F.col("converted_at"),
            # impression metrics
            F.col("placement"),
            F.col("impression_cost"),
            F.col("impression_cost_in_cents"),
            # click metrics
            F.col("click_cost"),
            F.col("click_cost_in_cents"),
            # conversion metrics
            F.col("conversion_revenue"),
            F.col("conversion_revenue_in_cents"),
            F.col("attribution"),
            # ad group descriptors
            F.col("ad_group_name"),
            F.col("targeting_type"),
            F.col("bid_strategy"),
            F.col("max_cpc"),
            F.col("ad_group_is_active"),
            F.col("is_cpc_bidding"),
            # campaign descriptors
            F.col("campaign_name"),
            F.col("objective"),
            F.col("budget_total"),
            F.col("budget_daily"),
            F.col("start_date"),
            F.col("end_date"),
            F.col("campaign_is_active"),
            F.col("campaign_duration_days"),
            # advertiser descriptors
            F.col("advertiser_name"),
            F.col("advertiser_is_active"),
            # enrichment
            F.col("click_id").isNotNull().alias("is_clicked"),
            F.col("conversion_id").isNotNull().alias("is_converted"),
            (
                F.coalesce(F.col("impression_cost"), F.lit(0))
                + F.coalesce(F.col("click_cost"), F.lit(0))
            ).alias("total_cost"),
            # partition col
            F.col("created_at"),
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
