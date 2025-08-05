from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer

import random
import json

from faker import Faker
fake = Faker()

GENRES = ["Rock", "Pop", "Hip-Hop", "Jazz", "Classical",
          "Electronic", "R&B", "Country", "Reggae", "Metal"]

RECORD_LABELS = ["Sony Music", "Universal", "Warner", "Independent"]

class ArtistProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=20, *args, **kwargs):
        super(ArtistProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_artist_data(self, row_num):
        account_id = f"A{row_num:08d}"
        name = fake.name()
        biography = fake.sentences(nb=3)
        dob = fake.date_of_birth()
        city = fake.city()
        country = fake.country()
        genre = random.choice(GENRES)
        record_label = random.choice(RECORD_LABELS)
        number_albums = random.randint(1, 20)
        number_tracks = number_albums * random.randint(1, 20)

        artist = {
            'account_id': account_id,
            'name': name,
            'biography': biography,
            'dob': dob.isoformat(),
            'city': city,
            'country': country,
            'genre': genre,
            'record_label': record_label,
            'number_albums': number_albums,
            'number_tracks': number_tracks,
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