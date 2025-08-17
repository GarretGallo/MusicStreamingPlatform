from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer

import random
import json

from faker import Faker
fake = Faker()

LOCATIONS = {
    "United States": ["New York", "Los Angeles", "Chicago"],
    "France": ["Paris", "Marseille", "Lyon"],
    "Japan": ["Tokyo", "Osaka", "Kyoto"],
    "Brazil": ["São Paulo", "Rio de Janeiro", "Brasília"],
    "Australia": ["Sydney", "Melbourne", "Brisbane"]
}

class AccountProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=50, *args, **kwargs):
        super(AccountProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_account_data(self, row_num):
        account_id = f"A{row_num:08d}"
        name = fake.name()
        email = fake.email()
        phone_number = fake.phone_number()
        credit_card = fake.credit_card_full()
        dob = fake.date_of_birth(minimum_age=10, maximum_age=80)
        address = fake.address()
        country = random.choice(list(LOCATIONS.keys()))
        city = random.choice(LOCATIONS[country])
        registration_date = fake.date(pattern="%Y-%m-%d")

        account = {
            'account_id': account_id,
            'name': name,
            'email': email,
            'phone_number': phone_number,
            'credit_card': credit_card,
            'dob': dob,
            'address': address,
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
            producer.send(self.kafka_topic, value=transaction)
            self.log.info(f"Sent transaction {transaction}")

        producer.flush()
        self.log.info(f"{self.num_records} transactions sent has been sent to kafka {self.kafka_topic}!")
