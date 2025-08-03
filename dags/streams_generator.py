from airflow.operators.empty import EmptyOperator
from airflow import DAG
from

from datetime import datetime, timedelta
import pandas as pd
import random

from kafka_operator import KafkaProducerOperator

start_date = datetime(2025, 1, 1)
default_args = {
    'owner': 'GGCODE',
    'depends_on_past': False,
    'backfill': False,
}

num_rows = 1000
output_file = './streams_large_data.csv'

def generate_random_data(row_num):
