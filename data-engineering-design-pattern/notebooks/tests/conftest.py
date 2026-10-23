# conftest.py
import pytest
import logging
from pyspark.sql import SparkSession

from silver.fct_order_lines import transform as fct_order_lines_transform

from silver.dim_customer import transform as dim_customer_transform

logging.getLogger("DataFrameQueryContextLogger").setLevel(logging.CRITICAL)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test_dim_customer")
        .master("local[*]")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
# ─────────────────────────────────────────
# silver.dim_customer fixtures
# ─────────────────────────────────────────


@pytest.fixture(scope="session")
def customer_df(spark):
    return spark.createDataFrame(
        [
            (
                "cust-1",
                "alice@example.com",
                "Alice Smith",
                "555-0001",
                "active",
                "2025-01-01T00:00:00",
                "2025-01-01T00:00:00",
            ),
            (
                "cust-2",
                "bob@example.com",
                "Bob Jones",
                "555-0002",
                "active",
                "2025-01-02T00:00:00",
                "2025-01-02T00:00:00",
            ),
            (
                "cust-3",
                "eve@example.com",
                "Eve Davis",
                "555-0003",
                "inactive",
                "2025-01-03T00:00:00",
                "2025-01-03T00:00:00",
            ),
        ],
        [
            "customer_id",
            "email",
            "full_name",
            "phone",
            "status",
            "created_at",
            "updated_at",
        ],
    )


@pytest.fixture(scope="session")
def customer_address_df(spark):
    return spark.createDataFrame(
        [
            ("cust-1", True, "123 Main St", "Springfield", "IL", "US"),
            ("cust-1", False, "456 Oak Ave", "Shelbyville", "IL", "US"),
            ("cust-2", True, "789 Pine Rd", "Ogdenville", "NY", "US"),
        ],
        ["customer_id", "is_default", "line1", "city", "state", "country"],
    )


# ─────────────────────────────────────────
# silver.fct_order_lines fixtures
# ─────────────────────────────────────────


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