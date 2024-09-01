from src.main.utility.logging_config import *

from pyspark.sql import *
from pyspark.sql.functions import *

def spark_session(Access_key_ID, Secret_access_key):
    spark = SparkSession.builder \
        .appName("S3IntegrationExample") \
        .config("spark.hadoop.fs.s3a.access.key", Access_key_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", Secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com").getOrCreate()
    logger.info("spark session %s",spark)
    return spark