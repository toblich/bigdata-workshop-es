"""SMS ETL dag."""
import json
from datetime import datetime, timedelta
from io import BytesIO

import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException

from minio_client import make_minio_client, create_bucket
from minio.error import S3Error
from minio.notificationconfig import NotificationConfig, PrefixFilterRule, QueueConfig

BUCKET="landing"


def create_bucket_step():
    client = make_minio_client()
    create_bucket(client, BUCKET)
    client.set_bucket_notification(BUCKET, NotificationConfig(
        queue_config_list=[QueueConfig(
            "arn:minio:sqs::LANDING:kafka",
            ["s3:ObjectCreated:Put"],
            config_id="1",
            prefix_filter_rule=PrefixFilterRule("sms-"),
        )]
    ))


def get_sms():
    r = requests.get("http://sms-api:3000")
    response = json.loads(r.content)
    offset = response['offset']
    data = json.dumps(response['data'])
    print(f'Fetched SMS page with offset {offset} and size {len(response["data"])}')
    return (offset, data)


def land_sms(**context):
    task_instance = context['ti']
    (offset, data) = task_instance.xcom_pull(task_ids='get_sms_page')

    if not data or (data == '[]'):
        raise AirflowSkipException

    object_name = f"sms-{offset:09d}.json"
    print(f'Writing object {object_name} into bucket {BUCKET}')

    client = make_minio_client()
    client.put_object(
        bucket_name=BUCKET,
        object_name=object_name,
        data=BytesIO(bytes(data, 'utf-8')),
        length=len(data),
        content_type="application/json",
    )


default_args = {'owner': 'tobi', 'retries': 0, 'start_date': datetime(2023, 2, 24)}
with DAG('sms_etl', default_args=default_args, schedule_interval=timedelta(seconds=5), is_paused_upon_creation=False) as dag:
    create_bucket_if_not_exists = PythonOperator(
        task_id='create_bucket_if_not_exists',
        python_callable=create_bucket_step
    )
    get_sms_page = PythonOperator(
        task_id='get_sms_page',
        python_callable=get_sms
    )
    land_sms_page = PythonOperator(
        task_id='land_sms_page',
        python_callable=land_sms,
        provide_context=True
    )

    create_bucket_if_not_exists >> get_sms_page >> land_sms_page
