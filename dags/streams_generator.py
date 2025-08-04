from airflow.operators.empty import EmptyOperator
from airflow import DAG

from datetime import datetime, timedelta
import pandas as pd
import random

from streams_kafka_operator import StreamProduceOperator

start_date = datetime(2025, 1, 1)
default_args = {
    'owner': 'GGCODE',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date,
}

with DAG('stream_generator',
         default_args=default_args,
         description='A DAG to generate stream data',
         schedule_interval=timedelta(days=1),
         start_date=start_date) as dag:
    start = EmptyOperator(task_id='start')

    generator_stream_data = StreamProduceOperator(
        task_id='generator_stream_data',
        kafka_broker='kafka-broker-3:19092',
        kafka_topic='stream_data',
        num_records = 100)

    end = EmptyOperator(task_id='end')

    start >> generator_stream_data >> end