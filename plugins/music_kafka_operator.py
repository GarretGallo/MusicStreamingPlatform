from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer

from datetime import datetime, timedelta
import random
import json


class MusicProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=10, *args, **kwargs):
        super(MusicProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_music_data(self, row_num):
        song = f"Song{row_num:08d}"
        album = f"Album{row_num:08d}"
        length = random.randint(1, 5)
        artist = f"Artist{row_num:08d}"
        genre = f"Genre{row_num:08d}"
        release_date = int((datetime.now() - timedelta(days=random.randint(0,365))).timestamp() * 1000)

        music = {
            'song': song,
            'album': album,
            'length': length,
            'artist': artist,
            'genre': genre,
            'release_date': release_date,
        }

        return music

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers = self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        for row_num in range(1, self.num_records+1):
            transaction = self.generate_music_data(row_num)
            producer.send(self.kafka_topic, value=transaction)
            self.log.info(f"Sent transaction {transaction}")

        producer.flush()
        self.log.info(f"{self.num_records} transactions sent has been sent to kafka {self.kafka_topic}!")