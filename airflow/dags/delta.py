"""SMS delta transformation dag."""
import json
import random
import string

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from confluent_kafka import Consumer
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from minio_client import make_minio_client, create_bucket

import pandas as pd
from html import unescape
from io import BytesIO, StringIO

BUCKET = "delta"


class KafkaSensor(BaseSensorOperator):
    def __init__(self, brokers, topics, *args, **kwargs):
        super(KafkaSensor, self).__init__(*args, **kwargs)
        self.brokers = brokers
        self.topics = topics

    def poke(self, context):
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.brokers,
                "group.id": "airflow-delta-transform",
                "auto.offset.reset": "beginning",
                "debug": "generic,protocol,consumer",
                # 'api.version.request': True,
                # 'api.version.fallback.ms': 0,
                # 'broker.version.fallback': '0.10.1.0',
                "logger": self.log,
                # TODO should also auto-commit?
            }
        )
        self.consumer.subscribe(self.topics)

        self.log.info("KafkaSensor awaiting for message on topics: %s", self.topics)

        message = self.consumer.poll(timeout=self.timeout - 5.0)
        self.log.info("Message %s from topics %s", message, self.topics)

        if message is None:
            return False
        if message.error():
            print("Error: {}".format(message.error()))
            return False

        data = json.loads(message.value().decode("utf-8"))["Key"]

        context["ti"].xcom_push(key="key", value=data)
        self.consumer.commit(asynchronous=False)

        self.consumer.close()

        return True


with DAG(
    "sms_delta_transform",
    default_args={"owner": "tobi", "retries": 0, "start_date": datetime(2023, 2, 24)},
    schedule_interval="*/1 * * * *",
    is_paused_upon_creation=False,
) as dag:

    def transform(**context):
        task_instance = context["ti"]
        (bucket, object_name) = task_instance.xcom_pull(
            task_ids="listen_for_message", key="key"
        ).split("/")
        print(f"Processing file '{object_name}' from bucket '{bucket}'")

        client = make_minio_client()
        try:
            response = client.get_object(bucket, object_name)
            data = response.data.decode("utf-8")
        finally:
            response.close()
            response.release_conn()

        df = pd.read_json(StringIO(data), orient="records")

        df["raw"] = df["sms"]
        df["is_spam"] = df["label"].eq("spam")
        df["sms"] = df["raw"].apply(lambda s: unescape(s))

        print(df)

        parquet_bytes = df.to_parquet()

        create_bucket(client, BUCKET)

        client.put_object(
            bucket_name=BUCKET,
            object_name=object_name + ".parquet",
            data=BytesIO(parquet_bytes),
            length=len(parquet_bytes),
        )

    listen_for_message = KafkaSensor(
        brokers="kafka:9092",
        topics=["landing"],
        task_id="listen_for_message",
        soft_fail=True,
        poke_interval=60,
        timeout=15,
        mode="reschedule",
        max_active_runs=1,
    )

    transform_message = PythonOperator(
        task_id="transform", python_callable=transform, provide_context=True
    )

    listen_for_message >> transform_message
