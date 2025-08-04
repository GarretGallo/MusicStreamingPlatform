from airflow.operators.empty import EmptyOperator
from airflow import DAG

from datetime import datetime, timedelta
import pandas as pd
import random

from music_kafka_operator import MusicProduceOperator

start_date = datetime(2025, 1, 1)
default_args = {
    'owner': 'GGCODE',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date
}

with DAG('music_generator',
         default_args=default_args,
         description='A DAG to generate music data',
         schedule_interval=timedelta(days=1),
         start_date=start_date) as dag:
    start = EmptyOperator(task_id='start')

    generator_music_data = MusicProduceOperator(
        task_id='generator_music_data',
        kafka_broker='kafka-broker-1:19092',
        kafka_topic='music_data',
        num_records = 10)

    end = EmptyOperator(task_id='end')

    start >> generator_music_data >> end