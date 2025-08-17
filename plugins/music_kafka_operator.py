from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer

import random
import json

from faker import Faker
fake = Faker()

GENRES = ["Rock", "Pop", "Hip-Hop", "Jazz", "Classical",
          "Electronic", "R&B", "Country", "Reggae", "Metal"]

SONGS = ["Aurora", "Eclipse", "Pulse!", "Mirage", "Solstice", "Drift", "Echo?", "Horizon", "Ember", "Gravity",
         "Cascade", "Velvet", "Spectrum", "Zenith", "Whispers", "Prism!", "Nomad", "Radiance", "Serenity", "Velocity",
         "Phoenix", "Odyssey", "Infinity", "Mosaic", "Tempest", "Reverie", "Quantum", "Horizon!", "Solace", "Luminous",
         "Fragment", "Silhouette", "Constellation", "Momentum", "Vapor", "Equinox", "Labyrinth", "Euphoria", "Vortex",
         "Crescendo", "Aurora?", "Ember!", "Paradox", "Celestial", "Anthem", "Mirage?", "Pulse", "Nocturne", "Radiant", "Spectrum?"]

ALBUMS = ["Rebellion", "Sanctuary", "Afterglow", "Nightfall", "Daybreak", "Static",
          "Utopia", "Islands", "Threshold", "Limelight", "Paragon", "Visions",
          "Elysium", "Exodus", "Ascension"]

class MusicProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=10, *args, **kwargs):
        super(MusicProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_music_data(self, row_num):
        song = random.choice(SONGS)
        artist = fake.name()
        album = fake.choice(ALBUMS)
        length = random.randint(1, 5)
        genre = random.choice(GENRES)
        release_date = fake.date(pattern="%Y-%m-%d")

        music = {
            'song': song,
            'artist': artist,
            'album': album,
            'length': length,
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
