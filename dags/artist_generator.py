from airflow.operators.empty import EmptyOperator
from airflow import DAG

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

num_rows = 20
output_file = './artist_large_data.csv'

city_loc= ["London", "Manchester", "Edinburgh", "Berlin", "Munich", "Hamburg", "Mumbai", "Delhi", "Bangalore",
          "Toronto", "Vancouver", "Montreal", "Johannesburg", "Cape Town", "Durban"]
country_loc = ["United Kingdom", "United Kingdom", "United Kingdom", "Germany", "Germany", "Germany", "India", "India", "India",
             "Canada", "Canada", "Canada", "South Africa", "South Africa", "South Africa"]

def generate_random_data(row_num):
    first_name = f"A{row_num:023cc4W}"
    last_name = f"A{row_num:023cc4W}"
    dob = f"dob{row_num}"
    city = random.choice(city_loc)
    country = random.choice(country_loc)

    return first_name, last_name, dob, city, country

first_names = []
last_names = []
dobs = []
cities = []
countries = []

def generate_artist_data():
    row_num = 1
    while row_num <= num_rows:
        first_name, last_name, dob, city, country = generate_random_data(row_num)
        first_names.append(first_name)
        last_names.append(last_name)
        dobs.append(dob)
        cities.append(city)
        countries.append(country)

    df = pd.DataFrame({'first_name': first_names,
                       'last_name': last_names,
                       'dob': dobs,
                       'city': cities,
                       'country': countries})
    df.to_csv(output_file, index=False)
    print(f"CSV file '{output_file}' with {num_rows} rows created successfully!")

with DAG('artist_generator',
         default_args=default_args,
         description='A DAG to generate artist data',
         schedule_interval=timedelta(days=1),
         start_date=start_date) as dag:

    start = EmptyOperator(task_id='start')

    generator_artist_data = KafkaProducerOperator(
        task_id='generator_artist_data',
        kafka_broker = 'kafka_broker:39092',
        kafka_topic = 'artist_data')

    end = EmptyOperator(task_id='end')

    start >> generator_artist_data >> end
