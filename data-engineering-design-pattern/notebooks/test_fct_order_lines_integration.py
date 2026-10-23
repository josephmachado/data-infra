import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from silver.fct_order_lines import load, transform
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


import silver.fct_order_lines as fct_order_lines

# ── fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test_fct_order_lines_load")
        .master("local[*]")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def jan1_df(spark):
    return (
        spark.createDataFrame(
            [
                (
                    "ol-1",
                    "ord-1",
                    "var-1",
                    2,
                    100.0,
                    10.0,
                    190.0,
                    "2025-01-01T00:00:00",
                    "2025-01-01T00:00:00",
                ),
                (
                    "ol-2",
                    "ord-2",
                    "var-2",
                    1,
                    200.0,
                    0.0,
                    200.0,
                    "2025-01-01T06:00:00",
                    "2025-01-01T06:00:00",
                ),
            ],
            [
                "order_line_id",
                "order_id",
                "variant_id",
                "quantity",
                "unit_price",
                "discount_amt",
                "line_total",
                "created_at",
                "updated_at",
            ],
        )
        .withColumn("created_at", F.col("created_at").cast("timestamp"))
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    )


@pytest.fixture
def jan3_df(spark):
    return (
        spark.createDataFrame(
            [
                (
                    "ol-3",
                    "ord-3",
                    "var-3",
                    3,
                    50.0,
                    5.0,
                    145.0,
                    "2025-01-02T00:00:00",
                    "2025-01-02T00:00:00",
                ),
                (
                    "ol-4",
                    "ord-4",
                    "var-4",
                    1,
                    200.0,
                    0.0,
                    200.0,
                    "2025-01-03T06:00:00",
                    "2025-01-03T06:00:00",
                ),
            ],
            [
                "order_line_id",
                "order_id",
                "variant_id",
                "quantity",
                "unit_price",
                "discount_amt",
                "line_total",
                "created_at",
                "updated_at",
            ],
        )
        .withColumn("created_at", F.col("created_at").cast("timestamp"))
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    )


TEST_TABLE = "local.silver.fct_order_lines_test"

@pytest.fixture
def setup_test_table(spark, monkeypatch):
    monkeypatch.setattr(fct_order_lines, "TABLE_NAME", TEST_TABLE)
    yield
    spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")

def test_overwrite_partitions_replaces_only_affected_partition(
    spark, jan1_df, jan3_df, setup_test_table
):

    # Run pipeline for Jan 1
    fct_order_lines.load(fct_order_lines.transform({"order_line": jan1_df}), spark)

    # Check 1 partition (only Jan 1)
    result = spark.sql(
        "SELECT DISTINCT DATE(created_at) as dt FROM local.silver.fct_order_lines_test"
    )
    assert result.count() == 1

    # Run pipeline for Jan 3 (jan3_df has Jan 2 + Jan 3 data)
    fct_order_lines.load(fct_order_lines.transform({"order_line": jan3_df}), spark)

    # Check 2 partitions (Jan 1 + Jan 2 + Jan 3)
    result = spark.sql(
        "SELECT DISTINCT DATE(created_at) as dt FROM local.silver.fct_order_lines_test"
    )
    assert result.count() == 3
