import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import datetime
import pytz
from pyspark.sql import SparkSession

args = sys.argv
aws_access_key_id = args[1]
aws_secret_access_key = args[2]
rawzone_bucket_name = args[3]
goldenzone_bucket_name = args[4]

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
yesterday = today - datetime.timedelta(days=1)

rawzone_prefix = f"s3a://{rawzone_bucket_name}/{today.year}/{today.month}/{today.day}"
goldenzone_prefix = f"s3a://{goldenzone_bucket_name}/{today.year}/{today.month}/{today.day}"
old_goldenzone_prefix = f"s3a://{goldenzone_bucket_name}/{yesterday.year}/{yesterday.month}/{yesterday.day}"

customers_df = spark.read.parquet(f'{rawzone_prefix}/customers.parquet', header=True).cache()
products_df = spark.read.parquet(f'{rawzone_prefix}/products.parquet', header=True).cache()
orders_df = spark.read.parquet(f'{rawzone_prefix}/orders.parquet', header=True).cache()

new_orders_products_df = orders_df.join(products_df, 'product_id', 'inner') \
    .join(customers_df, 'customer_id', 'inner') \
    .withColumn("revenue", col("quantity") * col("price"))

new_customer_revenue_df = new_orders_products_df.groupBy("customer_id", "first_name", "last_name", "age", "gender") \
    .agg(F.sum("revenue").alias("total_revenue"))\
    .select("customer_id", "first_name", "last_name", "age", "gender", "total_revenue")

new_product_revenue_df = new_orders_products_df.groupBy("product_id", "product_name", "feature", "target_audience") \
    .agg(F.sum("revenue").alias("total_revenue"))\
    .select("product_id", "product_name", "feature", "target_audience", "total_revenue")


customer_revenue_df = None
product_revenue_df = None


try:
    old_customer_revenue_df = spark.read.parquet(f'{old_goldenzone_prefix}/customer-revenue/part-*.parquet', header=True).cache()
    old_product_revenue_df = spark.read.parquet(f'{old_goldenzone_prefix}/product-revenue/part-*.parquet', header=True).cache()
    
    customer_revenue_df = old_customer_revenue_df.union(new_customer_revenue_df)

    customer_revenue_df = customer_revenue_df.groupBy('customer_id', 'first_name', 'last_name', 'age', 'gender')\
                    .agg(F.sum('total_revenue').alias('total_revenue'))\
                    .select("customer_id", "first_name", "last_name", "age", "gender", "total_revenue")
    
    product_revenue_df = old_product_revenue_df.union(new_product_revenue_df)

    product_revenue_df = product_revenue_df.groupBy("product_id", "product_name", "feature", "target_audience")\
                    .agg(F.sum('total_revenue').alias('total_revenue'))\
                    .select("product_id", "product_name", "feature", "target_audience", "total_revenue")
except:
    customer_revenue_df = new_customer_revenue_df
    product_revenue_df = new_product_revenue_df

customer_revenue_df.show()
product_revenue_df.show()

customer_revenue_df.coalesce(1).write.mode("overwrite").parquet(f"{goldenzone_prefix}/customer-revenue")
product_revenue_df.coalesce(1).write.mode("overwrite").parquet(f"{goldenzone_prefix}/product-revenue")

spark.stop()