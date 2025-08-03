from airflow.operators.empty import EmptyOperator
from airflow import DAG

from datetime import datetime, timedelta
import pandas as pd
import random

from kafka_operator import KafkaProduceOperator

start_date = datetime(2025, 1, 1)
default_args = {
    'owner': 'GGCODE',
    'depends_on_past': False,
    'backfill': False,
}

num_rows = 100
output_file = './account_large_data.csv'

city_loc = ["New York", "Los Angeles", "Chicago", "Paris", "Marseille", "Lyon", "Tokyo", "Osaka", "Kyoto",
          "São Paulo", "Rio de Janeiro", "Brasília", "Sydney", "Melbourne", "Brisbane"]
country_loc = ["United States", "United States", "United States", "France", "France", "France", "Japan", "Japan", "Japan",
             "Brazil", "Brazil", "Brazil", "Australia", "Australia", "Australia"]

def generate_random_data(row_num):
    account_id = f"A{row_num:82d13Xr}"
    first_name = f"FirstName{row_num}"
    last_name = f"LastName{row_num}"
    email = f"user{row_num}@example.com"
    phone_number = f"1-123-{random.randint(1, 12)}"
    dob = f"dob{row_num}"
    city = random.choice(city_loc)
    country = random.choice(country_loc)

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 3650))
    reg_date = int(random_date.timestamp()*1000)

    return account_id, first_name, last_name, email, phone_number, dob, city, country, reg_date

account_ids = []
first_names = []
last_names = []
emails = []
phone_numbers = []
dobs = []
cities = []
countries = []
account_registration_dates = []

def generate_account_data():
    row_num = 1
    while row_num <= num_rows:
        account_id, first_name, last_name, email, phone_number, dob, city, country, reg_date = generate_random_data(row_num)
        account_ids.append(account_id)
        first_names.append(first_name)
        last_names.append(last_name)
        emails.append(email)
        phone_numbers.append(phone_number)
        dobs.append(dob)
        cities.append(city)
        countries.append(country)
        account_registration_dates.append(reg_date)
        row_num += 1

    df = pd.DataFrame({'account_id': account_ids,
                       'first_name': first_names,
                       'last_name': last_names,
                       'email': emails,
                       'phone_number': phone_numbers,
                       'dob': dobs,
                       'city': cities,
                       'country': countries,
                       'account_registration_date': account_registration_dates})
    df.to_csv(output_file, index=False)
    print(f"CSV file '{output_file}' with {num_rows} rows created successfully!")

with DAG('account_generator',
         default_args=default_args,
         description='A DAG to create account data',
         schedule_interval=timedelta(days=1),
         start_date=start_date) as dag:

    start = EmptyOperator(task_id='start')

    generator_account_data = KafkaProduceOperator(
        task_id='generator_account_data',
        kafka_broker = 'broker:29092',
        kafka_topic = 'account_data',)

    end = EmptyOperator(task_id='end')

    start >> generator_account_data >> end
