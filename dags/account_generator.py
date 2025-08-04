from airflow.operators.empty import EmptyOperator
from airflow import DAG

from datetime import datetime, timedelta
import pandas as pd
import random

from account_kafka_operator import AccountProduceOperator

start_date = datetime(2025, 1, 1)
default_args = {
    'owner': 'GGCODE',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date,
}

with DAG('account_generator',
         default_args=default_args,
         description='A DAG to create account data',
         schedule_interval=timedelta(days=1),
         start_date=start_date) as dag:

    start = EmptyOperator(task_id='start')

    generator_account_data = AccountProduceOperator(
        task_id='generator_account_data',
        kafka_broker = 'kafka-broker-3:19092',
        kafka_topic = 'account_data',
        num_records = 50)

    end = EmptyOperator(task_id='end')

    start >> generator_account_data >> end
