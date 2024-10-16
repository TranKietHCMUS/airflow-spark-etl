from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import pytz
from datetime import datetime, timedelta

ho_chi_minh_tz = pytz.timezone('Asia/Ho_Chi_Minh')

today = datetime.now(ho_chi_minh_tz)
yesterday = today - timedelta(days=1)

def extract_table(name, attributes):
    mysql_hook = MySqlHook(mysql_conn_id='mysql')
    
    sql_query = f"SELECT * FROM {name}"
    data = mysql_hook.get_records(sql_query)

    temp_df = pd.DataFrame(data, columns=attributes)
    temp_df['updated_at'] = pd.to_datetime(temp_df['updated_at'], format='%Y-%m-%d %H:%M:%S')
    df = temp_df[(temp_df['updated_at'] >= yesterday.strftime('%Y-%m-%d %H:%M:%S')) & (temp_df['updated_at'] < today.strftime('%Y-%m-%d %H:%M:%S'))]
    df.to_parquet(f"{name}.parquet", index=False)


def extract():
    customer_attributes = ['customer_id', 'first_name', 'last_name', 'age', 'gender', 'created_at', 'updated_at']
    extract_table("customers", customer_attributes)
    product_attributes = ['product_id', 'product_name', 'feature', 'target_audience', 'price', 'created_at', 'updated_at']
    extract_table("products", product_attributes)
    order_attributes = ['order_id', 'customer_id', 'product_id', 'quantity', 'created_at', 'updated_at']
    extract_table("orders", order_attributes)
