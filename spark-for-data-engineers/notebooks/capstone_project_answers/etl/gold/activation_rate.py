import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def extract(spark):
    logger.info("Getting Upstream Data")
    return {
        "dim_users": spark.read.table("stackoverflow.dim_users"),
        "fact_posts": spark.read.table("stackoverflow.fact_posts"),
    }


def transform(spark, upstream_data):
    logger.info("Transforming Upstream Data")
    return spark.sql(
        """
        WITH user_cohorts AS (
            SELECT 
                DATE_TRUNC('month', CreationDate) as cohort_month,
                Id as user_id,
                CreationDate as signup_date
            FROM stackoverflow.dim_users
        ),
        user_first_activity AS (
            -- First post
            SELECT DISTINCT
                OwnerUserId as user_id,
                MIN(CreationDate) as first_activity_date,
                'post' as activity_type
            FROM stackoverflow.fact_posts
            WHERE OwnerUserId IS NOT NULL
            GROUP BY OwnerUserId
            
            UNION ALL
            
            -- First comment
            SELECT
                UserId as user_id,
                MIN(CreationDate) as first_activity_date,
                'comment' as activity_type
            FROM stackoverflow.comments
            WHERE UserId IS NOT NULL
            GROUP BY UserId
            
            UNION ALL
            
            -- First vote
            SELECT 
                UserId as user_id,
                MIN(CreationDate) as first_activity_date,
                'vote' as activity_type
            FROM stackoverflow.votes
            WHERE UserId IS NOT NULL
            GROUP BY UserId
        ),
        earliest_activity AS (
            SELECT 
                user_id,
                MIN(first_activity_date) as first_activity_date
            FROM user_first_activity
            GROUP BY user_id
        ),
        activation_status AS (
            SELECT 
                uc.cohort_month,
                uc.user_id,
                uc.signup_date,
                ea.first_activity_date,
                CASE 
                    WHEN ea.first_activity_date IS NOT NULL 
                        AND DATEDIFF(ea.first_activity_date, uc.signup_date) <= 30
                    THEN 1 
                    ELSE 0 
                END as activated_30d
            FROM user_cohorts uc
            LEFT JOIN earliest_activity ea ON uc.user_id = ea.user_id
        )
        SELECT 
            cohort_month,
            COUNT(*) as total_users,
            SUM(activated_30d) as activated_users,
            ROUND(100.0 * SUM(activated_30d) / COUNT(*), 2) as activation_rate_pct
        FROM activation_status
        GROUP BY cohort_month
        ORDER BY cohort_month
    """,
        dim_users=upstream_data["dim_users"],
        fact_posts=upstream_data["fact_posts"],
    )


def data_quality_check(spark, transformed_data):
    logger.info("Running PK Check")
    pk_check_result = (
        spark.sql(
            """
            select case when count(distinct cohort_month) = count(*) then False else True end as is_error from {transformed_data}
            """,
            transformed_data=transformed_data,
        )
        .collect()[0]
        .asDict()["is_error"]
    )
    return pk_check_result


def load(spark, table_df, table_name):
    logger.info(f"Loading Data into {table_name}")
    spark.sql(f"drop table if exists stackoverflow.{table_name}")
    table_df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table_name}")


def run(spark, table_name="activation_rate"):
    upstream_data = extract(spark)
    transformed_data = transform(spark, upstream_data)
    if data_quality_check(spark, transformed_data):
        raise Exception("Data Quality Check Failed")
    load(spark, transformed_data, table_name)


if __name__ == "__main__":
    executor_memory = "8g"
    executor_cores = 4
    num_executors = 2
    table_name = "activation_rate"

    spark = (
        SparkSession.builder.appName(table_name)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", executor_cores)
        .config("spark.executor.instances", num_executors)
        .config("spark.cores.max", executor_cores * num_executors)
        .getOrCreate()
    )

    run(spark, table_name)
