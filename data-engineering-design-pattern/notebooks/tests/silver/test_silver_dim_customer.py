import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from silver.dim_customer import transform

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
