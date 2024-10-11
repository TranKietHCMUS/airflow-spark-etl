from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
import requests

from tasks.extract import extract
from tasks.load_aws import load_to_raw_zone
import os

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

def send_telegram_message(context):
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    
    tasks = context['dag'].tasks
    message = f"DAG {dag_id} - Run {run_id} status report:\n"
    
    for task in tasks:
        task_instance = TaskInstance(task, dag_run.execution_date)
        task_instance.refresh_from_db()
        state = task_instance.state
        message += f"- Task {task.task_id}: {state}\n"
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }
    response = requests.post(url, data=payload)
    
    if response.status_code != 200:
        raise Exception(f"Failed to send message to Telegram: {response.text}")

def finish():
    print("Successfully!")

default_args = {
    'owner': 'trkiet',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'on_success_callback': send_telegram_message,
    'on_failure_callback': send_telegram_message 
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
        application_args=[aws_access_key_id, aws_secret_access_key, os.getenv("RAW_ZONE_BUCKET_NAME"), os.getenv("GOLDEN_ZONE_BUCKET_NAME")],
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
        application_args=[aws_access_key_id, aws_secret_access_key],
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

    finish_task = PythonOperator(
        task_id='finish',
        python_callable=finish
    )

    extract_task >> load_to_raw_zone_task >> transform_spark_job_task >> load_spark_job_task >> finish_task