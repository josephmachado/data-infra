import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from silver.fct_order_lines import transform

# ── fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test_fct_order_lines")
        .master("local[*]")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def order_line_df(spark):
    return spark.createDataFrame(
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
            ),  # discounted
            (
                "ol-2",
                "ord-2",
                "var-2",
                1,
                200.0,
                0.0,
                200.0,
                "2025-01-02T00:00:00",
                "2025-01-02T00:00:00",
            ),  # not discounted
            (
                "ol-3",
                "ord-3",
                "var-3",
                3,
                50.0,
                50.0,
                100.0,
                "2025-01-03T00:00:00",
                "2025-01-03T00:00:00",
            ),  # fully discounted
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


# ── tests ─────────────────────────────────────────────────────────────────────


class TestIsDiscounted:
    def test_is_discounted_true_when_discount_gt_zero(self, order_line_df):
        transformed_df = transform({"order_line": order_line_df})
        row = transformed_df.filter(F.col("order_line_id") == "ol-1").first()
        assert row["is_discounted"] is True

    def test_is_discounted_false_when_discount_is_zero(self, order_line_df):
        transformed_df = transform({"order_line": order_line_df})
        row = transformed_df.filter(F.col("order_line_id") == "ol-2").first()
        assert row["is_discounted"] is False

    def test_is_discounted_true_when_fully_discounted(self, order_line_df):
        transformed_df = transform({"order_line": order_line_df})
        row = transformed_df.filter(F.col("order_line_id") == "ol-3").first()
        assert row["is_discounted"] is True


class TestEffectiveUnitPrice:
    def test_effective_unit_price_subtracts_discount(self, order_line_df):
        transformed_df = transform({"order_line": order_line_df})
        row = transformed_df.filter(F.col("order_line_id") == "ol-1").first()
        assert row["effective_unit_price"] == 90.0  # 100 - 10

    def test_effective_unit_price_equals_unit_price_when_no_discount(
        self, order_line_df
    ):
        transformed_df = transform({"order_line": order_line_df})
        row = transformed_df.filter(F.col("order_line_id") == "ol-2").first()
        assert row["effective_unit_price"] == 200.0  # 200 - 0

    def test_effective_unit_price_is_zero_when_fully_discounted(self, order_line_df):
        transformed_df = transform({"order_line": order_line_df})
        row = transformed_df.filter(F.col("order_line_id") == "ol-3").first()
        assert row["effective_unit_price"] == 0.0  # 50 - 50
