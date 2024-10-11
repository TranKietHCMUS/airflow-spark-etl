import logging
import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pytz
import os

ho_chi_minh_tz = pytz.timezone('Asia/Ho_Chi_Minh')

current_time = datetime.datetime.now(ho_chi_minh_tz)
formatted_date = current_time.strftime("%Y-%m-%d")
formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

def load_file(bucket_name, filename):
    s3 = S3Hook(aws_conn_id='aws')

    key = f"{current_time.year}/{current_time.month}/{current_time.day}/" + filename + ".parquet"
    filename += ".parquet"

    s3.load_file(
        filename=filename,
        key=key,
        bucket_name=bucket_name,
        replace=True 
    )

    logging.info(f"Load {filename} to S3 successfully!")

    if os.path.isfile(filename):
        os.remove(filename)

def load_to_raw_zone(bucket_name):
    load_file(bucket_name, filename="customers")
    load_file(bucket_name, filename="products")
    load_file(bucket_name, filename="orders")