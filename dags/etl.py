from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import requests

from tasks.extract import extract
from tasks.load_aws import load_to_raw_zone
import os

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
rawzone_bucket_name = os.getenv("RAW_ZONE_BUCKET_NAME")
goldenzone_bucket_name = os.getenv("GOLDEN_ZONE_BUCKET_NAME")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

TELEGRAM_API_URL = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'

def send_message(task_id, status):
    message = f"Task {task_id} has {status}."
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }
    try:
        response = requests.post(TELEGRAM_API_URL, data=payload)
        if response.status_code == 200:
            print(f"Message sent successfully: {message}")
        else:
            print(f"Failed to send message: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error sending message: {e}")

def task_success_callback(context):
    task_id = context['task_instance'].task_id
    send_message(task_id, 'succeeded')

def task_failure_callback(context):
    task_id = context['task_instance'].task_id
    send_message(task_id, 'failed')

default_args = {
    'owner': 'trkiet',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'on_success_callback': task_success_callback,
    'on_failure_callback': task_failure_callback 
}

with DAG (
    dag_id = 'etl-dag',
    default_args=default_args,
    description = 'Build an ELT pipeline!',
    start_date = days_ago(0),
    schedule_interval = '@daily',
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
        provide_context=True
    )

    load_to_raw_zone_task = PythonOperator(
        task_id="load_to_raw_zone",
        python_callable=load_to_raw_zone,
        op_kwargs={
            'bucket_name': os.getenv("RAW_ZONE_BUCKET_NAME"),
        },
        provide_context=True
    )

    transform_spark_job_task = SparkSubmitOperator(
        application="dags/tasks/transform.py",
        task_id="transform",
        conn_id="spark",
        application_args=[aws_access_key_id, aws_secret_access_key, rawzone_bucket_name, goldenzone_bucket_name],
        total_executor_cores=2,
        executor_cores=2,
        executor_memory='1g',
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901"
    )

    load_spark_job_task = SparkSubmitOperator(
        application="dags/tasks/load_db.py",
        task_id="load_to_db",
        conn_id="spark",
        application_args=[aws_access_key_id, aws_secret_access_key, postgres_user, postgres_password, postgres_db, goldenzone_bucket_name],
        total_executor_cores=2,
        executor_cores=2,
        executor_memory='1g',
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901,org.postgresql:postgresql:42.7.4",
        conf={
            'spark.executor.extraClassPath': 'postgres/postgresql-42.7.4.jar',
            'spark.driver.extraClassPath': 'postgres/postgresql-42.7.4.jar',
        }
    )

    extract_task >> load_to_raw_zone_task >> transform_spark_job_task >> load_spark_job_task