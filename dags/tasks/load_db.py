import sys
import datetime
import pytz
from pyspark.sql import SparkSession

args = sys.argv
aws_access_key_id = args[1]
aws_secret_access_key = args[2]
postgres_user = args[3]
postgres_password = args[4]
postgres_db = args[5]
goldenzone_bucker_name = args[6]

spark = SparkSession.builder.appName("AirflowSparkJob")\
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Spark Version :'+spark.version)

ho_chi_minh_tz = pytz.timezone('Asia/Ho_Chi_Minh')
today = datetime.datetime.now(ho_chi_minh_tz)

goldenzone_prefix = f"s3a://{goldenzone_bucker_name}/{today.year}/{today.month}/{today.day}"

db_url = f"jdbc:postgresql://postgres:5432/{postgres_db}"
db_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

customer_revenue_df = spark.read.parquet(f'{goldenzone_prefix}/customer-revenue/part-*.parquet', header=True).cache()
customer_revenue_df.write.jdbc(url=db_url, table="store.customer_revenue", mode="overwrite", properties=db_properties)

product_revenue_df = spark.read.parquet(f'{goldenzone_prefix}/product-revenue/part-*.parquet', header=True).cache()
product_revenue_df.write.jdbc(url=db_url, table="store.product_revenue", mode="overwrite", properties=db_properties)

spark.stop()