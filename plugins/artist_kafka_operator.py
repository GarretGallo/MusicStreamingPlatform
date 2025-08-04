from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer

from datetime import datetime, timedelta
import random
import json

city_loc= ["London", "Manchester", "Edinburgh", "Berlin", "Munich", "Hamburg", "Mumbai", "Delhi", "Bangalore",
          "Toronto", "Vancouver", "Montreal", "Johannesburg", "Cape Town", "Durban"]
country_loc = ["United Kingdom", "United Kingdom", "United Kingdom", "Germany", "Germany", "Germany", "India", "India", "India",
             "Canada", "Canada", "Canada", "South Africa", "South Africa", "South Africa"]

class ArtistProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=20, *args, **kwargs):
        super(ArtistProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_artist_data(self, row_num):
        first_name = f"FirstName{row_num}"
        last_name = f"LastName{row_num}"
        dob = f"dob{row_num}"
        city = random.choice(city_loc)
        country = random.choice(country_loc)

        artist = {
            'first_name': first_name,
            'last_name': last_name,
            'dob': dob,
            'city': city,
            'country': country,
        }

        return artist

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers = self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        for row_num in range(1, self.num_records+1):
            transaction = self.generate_artist_data(row_num)
            producer.send(self.kafka_topic, value=transaction)
            self.log.info(f"Sent transaction {transaction}")

        producer.flush()
        self.log.info(f"{self.num_records} transactions sent has been sent to kafka {self.kafka_topic}!")