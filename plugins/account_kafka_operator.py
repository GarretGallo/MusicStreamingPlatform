from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer

from datetime import datetime, timedelta
import random
import json

city_loc = ["New York", "Los Angeles", "Chicago", "Paris", "Marseille", "Lyon", "Tokyo", "Osaka", "Kyoto",
          "São Paulo", "Rio de Janeiro", "Brasília", "Sydney", "Melbourne", "Brisbane"]
country_loc = ["United States", "United States", "United States", "France", "France", "France", "Japan", "Japan", "Japan",
             "Brazil", "Brazil", "Brazil", "Australia", "Australia", "Australia"]

class AccountProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=100, *args, **kwargs):
        super(AccountProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_account_data(self, row_num):
        account_id = f"A{row_num:08d}"
        first_name = f"FirstName{row_num}"
        last_name = f"LastName{row_num}"
        email = f"user{row_num}@example.com"
        phone_number = f"1-123-{random.randint(1, 12)}"
        dob = f"dob{row_num}"
        city = random.choice(city_loc)
        country = random.choice(country_loc)
        registration_date = int((datetime.now() - timedelta(days=random.randint(0,365))).timestamp() * 1000)

        account = {
            'account_id': account_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone_number': phone_number,
            'dob': dob,
            'city': city,
            'country': country,
            'registration_date': registration_date,
        }

        return account

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers = self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        for row_num in range(1, self.num_records+1):
            transaction = self.generate_account_data(row_num)
            producer.send(self.kafka_topic, value=transaction)
            self.log.info(f"Sent transaction {transaction}")

        producer.flush()
        self.log.info(f"{self.num_records} transactions sent has been sent to kafka {self.kafka_topic}!")