import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def extract(spark):
    logger.info("Getting Upstream Data")
    return {"fact_posts": spark.read.table("stackoverflow.fact_posts")}


def transform(spark, upstream_data):
    logger.info("Transforming Upstream Data")
    return spark.sql(
        """
    WITH recent_questions AS (
        SELECT 
            Id as question_id,
            CreationDate,
            Title,
            Tags,
            ViewCount,
            AnswerCount,
            Score,
            (UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(CreationDate)) / 3600.0 as hours_since_posted
        FROM {fact_posts}
        WHERE PostTypeId = 1  -- Questions only
    ),
    velocity_calc AS (
        SELECT 
            question_id,
            Title,
            Tags,
            CreationDate,
            hours_since_posted,
            ViewCount,
            AnswerCount,
            Score,
            CASE 
                WHEN hours_since_posted > 0 
                THEN (COALESCE(ViewCount, 0) + COALESCE(AnswerCount, 0) * 10 + COALESCE(Score, 0) * 5) / hours_since_posted
                ELSE 0 
            END as engagement_velocity
        FROM recent_questions
    )
    SELECT 
        question_id,
        Title,
        Tags,
        CreationDate,
        ROUND(hours_since_posted, 2) as hours_old,
        ViewCount,
        AnswerCount,
        Score,
        ROUND(engagement_velocity, 2) as engagement_velocity_score
    FROM velocity_calc
    WHERE engagement_velocity > 0
    """,
        fact_posts=upstream_data["fact_posts"],
    )


def data_quality_check(spark, transformed_data):
    logger.info("Running Reconciliation Check")
    recon_check_result = (
        spark.sql(
            """
              with diff_calc as (
                select ROUND(((in_cnt - out_cnt)*100.0)/in_cnt, 2) as diff_perc 
                from
                (select count(*) as out_cnt from {transformed_data}) as output_data,
                (select count(*) as in_cnt from stackoverflow.posts where PostTypeId = 1) as input_data
                )
                select case when diff_perc > 0.01 then True else False end is_error
                from diff_calc
            """,
            transformed_data=transformed_data,
        )
        .collect()[0]
        .asDict()["is_error"]
    )
    logger.info("Running PK Check")
    pk_check_result = (
        spark.sql(
            """
                                select case when count(distinct question_id) = count(*) then False else True end as is_error from {transformed_data}
                                """,
            transformed_data=transformed_data,
        )
        .collect()[0]
        .asDict()["is_error"]
    )
    return pk_check_result and recon_check_result


def load(spark, table_df, table_name):
    logger.info(f"Loading Data into {table_name}")
    spark.sql(f"drop table if exists stackoverflow.{table_name}")
    table_df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table_name}")


def run(spark, table_name="engagement_velocity"):
    upstream_data = extract(spark)
    transformed_data = transform(spark, upstream_data)
    if data_quality_check(spark, transformed_data):
        raise Exception("Data Quality Check Failed")
    load(spark, transformed_data, table_name)


if __name__ == "__main__":
    executor_memory = "8g"
    executor_cores = 4
    num_executors = 2
    table_name = "engagement_velocity"

    spark = (
        SparkSession.builder.appName(table_name)
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", executor_cores)
        .config("spark.executor.instances", num_executors)
        .config("spark.cores.max", executor_cores * num_executors)
        .getOrCreate()
    )

    run(spark, table_name)
