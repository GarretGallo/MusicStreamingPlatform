from airflow.operators.empty import EmptyOperator
from airflow import DAG

from datetime import datetime, timedelta
import pandas as pd
import random

from artist_kafka_operator import ArtistProduceOperator

start_date = datetime(2025, 1, 1)
default_args = {
    'owner': 'GGCODE',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date,
}

with DAG('artist_generator',
         default_args=default_args,
         description='A DAG to generate artist data',
         schedule_interval=timedelta(days=1),
         start_date=start_date) as dag:

    start = EmptyOperator(task_id='start')

    generator_artist_data = ArtistProduceOperator(
        task_id='generator_artist_data',
        kafka_broker = 'kafka-broker-1:19092',
        kafka_topic = 'artist_data',
        num_records = 20)

    end = EmptyOperator(task_id='end')

    start >> generator_artist_data >> end