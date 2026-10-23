import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from soda_core.contracts import verify_contract_locally
from soda_sparkdf import SparkDataFrameDataSource

TABLE_NAME = "local.gold.ad_campaign_performance"
logger = logging.getLogger(__name__)


def extract(
    spark: SparkSession, start_time: str, end_time: str
) -> dict[str, DataFrame]:
    return {
        "obt_ad": spark.table("local.gold.obt_ad_impressions").filter(
            (F.col("created_at") >= start_time) & (F.col("created_at") < end_time)
        )
    }


def transform(input_dfs: dict[str, DataFrame]) -> DataFrame:
    """
    Aggregate obt_ad to campaign × day grain.
    Metrics:
      - total_impressions: COUNT(impression_id)
      - total_clicks: COUNT(click_id)
      - total_conversions: COUNT(conversion_id)
      - total_spend: SUM(impression_cost) + SUM(click_cost)
      - total_revenue: SUM(conversion_revenue)
      - roas: total_revenue / total_spend
    Rolling windows (l7d, l30d, l90d) on roas.
    """
    obt = input_dfs["obt_ad"]

    daily = (
        obt.groupBy(
            F.to_date(F.col("created_at")).alias("event_date"),
            F.col("campaign_id"),
            F.col("campaign_name"),
            F.col("advertiser_id"),
            F.col("advertiser_name"),
            F.col("objective"),
            F.col("budget_total"),
            F.col("budget_daily"),
            F.col("campaign_is_active"),
        )
        .agg(
            F.count("impression_id").alias("total_impressions"),
            F.count("click_id").alias("total_clicks"),
            F.count("conversion_id").alias("total_conversions"),
            (
                F.sum(F.coalesce(F.col("impression_cost"), F.lit(0)))
                + F.sum(F.coalesce(F.col("click_cost"), F.lit(0)))
            ).alias("total_spend"),
            F.sum(F.coalesce(F.col("conversion_revenue"), F.lit(0))).alias(
                "total_revenue"
            ),
        )
        .withColumn(
            "roas",
            F.when(
                F.col("total_spend") > 0, F.col("total_revenue") / F.col("total_spend")
            ).otherwise(F.lit(None)),
        )
    )

    window_7d = (
        Window.partitionBy("campaign_id")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-7 * 86400, 0)
    )
    window_30d = (
        Window.partitionBy("campaign_id")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-30 * 86400, 0)
    )
    window_90d = (
        Window.partitionBy("campaign_id")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-90 * 86400, 0)
    )

    return daily.select(
        F.col("event_date"),
        F.col("campaign_id"),
        F.col("campaign_name"),
        F.col("advertiser_id"),
        F.col("advertiser_name"),
        F.col("objective"),
        F.col("budget_total"),
        F.col("budget_daily"),
        F.col("campaign_is_active"),
        F.col("total_impressions"),
        F.col("total_clicks"),
        F.col("total_conversions"),
        F.col("total_spend"),
        F.col("total_revenue"),
        F.col("roas"),
        F.sum("total_revenue").over(window_7d).alias("l7d_revenue"),
        F.sum("total_spend").over(window_7d).alias("l7d_spend"),
        (
            F.sum("total_revenue").over(window_7d)
            / F.nullif(F.sum("total_spend").over(window_7d), F.lit(0))
        ).alias("l7d_roas"),
        (
            F.sum("total_revenue").over(window_30d)
            / F.nullif(F.sum("total_spend").over(window_30d), F.lit(0))
        ).alias("l30d_roas"),
        (
            F.sum("total_revenue").over(window_90d)
            / F.nullif(F.sum("total_spend").over(window_90d), F.lit(0))
        ).alias("l90d_roas"),
    )


def load(output_df: DataFrame) -> None:
    output_df.writeTo(TABLE_NAME).createOrReplace()


def validate(output_df: DataFrame, spark: SparkSession) -> bool:
    """
    WAP pattern — validate before load using Soda Core contracts.
    Registers output_df as a timestamped temp view for debuggability.
    Checks:
      - row_count > 0
      - no nulls on campaign_id, event_date
      - total_spend >= 0
      - roas >= 0
    Returns False and logs errors on failure.
    Caller (run()) is responsible for raising exception.
    """
    view_name = "tmp_ad_campaign_perf"
    output_df.createOrReplaceTempView(view_name)

    spark_data_source = SparkDataFrameDataSource.from_existing_session(
        session=spark, name="my_sparkdf"
    )

    file_name = TABLE_NAME.split(".")[-1]
    CONTRACT_PATH = Path(__file__).parent / "contracts" / f"{file_name}.yaml"

    result = verify_contract_locally(
        data_sources=[spark_data_source],
        contract_file_path=str(CONTRACT_PATH),
    )

    if result.is_ok:
        logger.info(f"✅ Validation passed for {view_name}")
        return True
    else:
        logger.error(f"❌ Validation failed for {view_name}")
        logger.error(result.get_errors_str())
        return False


def run(spark: SparkSession, start_time: str, end_time: str) -> None:
    output_df = transform(extract(spark, start_time, end_time))
    if not validate(output_df, spark):
        raise Exception(f"Validation failed for {TABLE_NAME}, aborting load")
    load(output_df)


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
