import os
import subprocess

from pyspark.sql.types import BinaryType
from pyspark.sql import functions as F


def get_table_urls(url, year):
    if year == "*":
        return {
            "posts": f"{url}/posts/",
            "users": f"{url}/users.parquet",
            "comments": f"{url}/comments/",
            "votes": f"{url}/votes/",
            "badges": f"{url}/badges.parquet",
            "postlinks": f"{url}/postlinks.parquet",
        }
    else:
        return {
            "posts": f"{url}/posts/{year}.parquet",
            "users": f"{url}/users.parquet",
            "comments": f"{url}/comments/{year}.parquet",
            "votes": f"{url}/votes/{year}.parquet",
            "badges": f"{url}/badges.parquet",
            "postlinks": f"{url}/postlinks.parquet",
        }


def download_stackoverflow_data(
    url="s3://datasets-documentation/stackoverflow/parquet",
    year="2008",
    destination="./stackoverflow_data",
):
    # Check if path already exists and is a directory
    if os.path.exists(destination) and os.path.isdir(destination):
        print(f"Data exists already, delete the folder {destination} to re-download")
        return

    os.makedirs(destination, exist_ok=True)

    print("=" * 60)
    print("Step 1: Downloading data from S3")
    print("=" * 60)

    table_urls = get_table_urls(url, year)

    for table, s3_path in table_urls.items():
        table_dir = os.path.join(destination, table)
        os.makedirs(table_dir, exist_ok=True)

        print(f"\nDownloading {table}...")
        cmd = f"aws s3 cp {s3_path} {table_dir}/ --no-sign-request"

        try:
            subprocess.run(cmd, shell=True, check=True)
            print(f"✓ Downloaded: {table}")
        except subprocess.CalledProcessError as e:
            print(f"✗ Error downloading {table}: {e}")


def load_data_bronze_table(
    spark,
    data_path="./stackoverflow_data",
    tables=["posts", "users", "comments", "votes"],
):
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    spark.sql("create schema if not exists stackoverflow")
    spark.sql("use stackoverflow")
    create_enum_tables(spark)

    for table in tables:
        df = spark.sql(f"select * from parquet.`{data_path}/{table}/`")
        binary_cols = [
            field.name
            for field in df.schema.fields
            if type(field.dataType) == BinaryType
        ]
        for binary_col in binary_cols:
            df = df.withColumn(binary_col, df[binary_col].cast("string"))
        if table == "posthistory":
            df = df.withColumn(
                "CreationDate", F.from_unixtime(F.col("CreationDate") / 1000000000)
            )

        df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table}")
        print(f"✓ Loaded data into: stackoverflow.{table}")


def create_enum_tables(spark):
    post_type_data = [
        (1, "Question"),
        (2, "Answer"),
        (3, "Wiki"),
        (4, "TagWikiExcerpt"),
        (5, "TagWiki"),
        (6, "ModeratorNomination"),
        (7, "WikiPlaceholder"),
        (8, "PrivilegeWiki"),
    ]
    post_type_df = spark.createDataFrame(post_type_data, ["id", "postType"])
    post_type_df.write.mode("overwrite").saveAsTable("stackoverflow.post_type")
    print("✓ Loaded data into: stackoverflow.post_type")

    post_history_type_data = [
        (1, "Initial Title"),
        (2, "Initial Body"),
        (3, "Initial Tags"),
        (4, "Edit Title"),
        (5, "Edit Body"),
        (6, "Edit Tags"),
        (7, "Rollback Title"),
        (8, "Rollback Body"),
        (9, "Rollback Tags"),
        (10, "Post Closed"),
        (11, "Post Reopened"),
        (12, "Post Deleted"),
        (13, "Post Undeleted"),
        (14, "Post Locked"),
        (15, "Post Unlocked"),
        (16, "Community Owned"),
        (17, "Post Migrated"),
        (18, "Question Merged"),
        (19, "Question Protected"),
        (20, "Question Unprotected"),
        (21, "Post Disassociated"),
        (22, "Question Unmerged"),
        (23, "Unknown dev related event"),
        (24, "Suggested Edit Applied"),
        (25, "Post Tweeted"),
        (26, "Vote nullification by dev"),
        (27, "Post unmigrated/hidden moderator migration"),
        (28, "Unknown suggestion event"),
        (29, "Unknown moderator event"),
        (30, "Unknown event"),
        (31, "Comment discussion moved to chat"),
        (33, "Post notice added"),
        (34, "Post notice removed"),
        (35, "Post migrated away"),
        (36, "Post migrated here"),
        (37, "Post merge source"),
        (38, "Post merge destination"),
        (50, "Bumped by Community User"),
        (52, "Question became hot network question"),
        (53, "Question removed from hot network questions by moderator"),
        (66, "Created from Ask Wizard"),
    ]

    post_history_type_df = spark.createDataFrame(
        post_history_type_data, ["id", "history_type"]
    )
    post_history_type_df.write.mode("overwrite").saveAsTable(
        "stackoverflow.post_history_type"
    )
    print("✓ Loaded data into: stackoverflow.post_history_type")


def run(
    spark,
    url="s3://datasets-documentation/stackoverflow/parquet",
    year="2008",
    destination="./stackoverflow_data",
):
    download_stackoverflow_data(url, year, destination)
    load_data_bronze_table(spark, destination)


def run_dbx(
    spark,
    url="s3a://datasets-documentation/stackoverflow/parquet",
    year="2008",
):
    spark.sql("create schema if not exists stackoverflow")
    spark.sql("use stackoverflow")
    create_enum_tables(spark)

    table_urls = get_table_urls(url, year)
    for table, table_url in table_urls.items():
        print(f"Loading data from {table_url} and saving it to stackoverflow.{table}")
        df = spark.read.parquet(f"{table_url}")
        binary_cols = [
            field.name
            for field in df.schema.fields
            if type(field.dataType) == BinaryType
        ]
        for binary_col in binary_cols:
            df = df.withColumn(binary_col, df[binary_col].cast("string"))
        df.write.mode("overwrite").saveAsTable(f"stackoverflow.{table}")
