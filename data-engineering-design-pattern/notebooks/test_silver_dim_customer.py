import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from silver.dim_customer import transform

# ── fixtures ──────────────────────────────────────────────────────────────────


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


# ── tests ─────────────────────────────────────────────────────────────────────


def test_output_schema_is_correct(customer_df, customer_address_df, spark):
    transformed_df = transform(
        {"customer_df": customer_df, "customer_address_df": customer_address_df}, spark
    )
    expected_columns = {
        "customer_id",
        "email",
        "full_name",
        "phone",
        "status",
        "created_at",
        "updated_at",
        "addresses",
    }
    assert expected_columns == set(transformed_df.columns)


def test_one_row_per_customer(customer_df, customer_address_df, spark):
    transformed_df = transform(
        {"customer_df": customer_df, "customer_address_df": customer_address_df}, spark
    )
    expected_count = customer_df.select("customer_id").distinct().count()
    assert transformed_df.count() == expected_count
