from pyspark.sql import SparkSession
from pyspark.sql import functions as F

JDBC_URL = "jdbc:postgresql://postgres:5432/ecommerce"
JDBC_PROPERTIES = {
    "user": "dataengineer",
    "password": "datapipeline",
    "driver": "org.postgresql.Driver",
}
TABLE_NAME = "local.bronze.advertiser"


def run(spark: SparkSession) -> None:
    spark.read.jdbc(
        url=JDBC_URL,
        table=f"""(
            SELECT * 
            FROM public.advertiser
        ) advertiser""",
        properties=JDBC_PROPERTIES,
    ).writeTo(TABLE_NAME).createOrReplace()


if __name__ == "__main__":
    spark = SparkSession.builder.appName(TABLE_NAME).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    run(spark)
