from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer
from faker import Faker

from datetime import datetime, timedelta
import random
import json

from artist_kafka_operator  import ArtistProduceOperator
from music_kafka_operator   import MusicProduceOperator

fake = Faker()

class StreamProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=20, *args, **kwargs):
        super(StreamProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_streams_data(self, row_num):
        listener_first_name = fake.first_name()
        listener_last_name = fake.last_name()
        song_name = fake.song_name()
        album_name = fake.album_name()
        artist_name = fake.artist_name()
        stream_date = fake.date()

        stream = {
            'listener_first_name': listener_first_name,
            'listener_last_name': listener_last_name,
            'song_name': song_name,
            'album_name': album_name,
            'artist_name': artist_name,
            'stream_date': stream_date,
        }
        return stream

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers = self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        for row_num in range(1, self.num_records+1):
            transaction = self.generate_streams_data(row_num)
            producer.send(self.kafka_topic, value=transaction)
            self.log.info(f"Sent transaction {transaction}")

        producer.flush()
        self.log.info(f"{self.num_records} transactions sent has been sent to kafka {self.kafka_topic}!")