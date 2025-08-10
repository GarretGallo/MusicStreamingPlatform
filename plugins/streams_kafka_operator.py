from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer
from faker import Faker

import random
import json
fake = Faker()

GENRES = ["Rock", "Pop", "Hip-Hop", "Jazz", "Classical",
          "Electronic", "R&B", "Country", "Reggae", "Metal"]

class StreamProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=100, *args, **kwargs):
        super(StreamProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_streams_data(self, row_num):
        listener = fake.name()
        song = fake.word(ext_word_list=["Aurora", "Eclipse", "Pulse!", "Mirage", "Solstice", "Drift", "Echo?", "Horizon", "Ember", "Gravity",
                                             "Cascade", "Velvet", "Spectrum", "Zenith", "Whispers", "Prism!", "Nomad", "Radiance", "Serenity", "Velocity",
                                             "Phoenix", "Odyssey", "Infinity", "Mosaic", "Tempest", "Reverie", "Quantum", "Horizon!", "Solace", "Luminous",
                                             "Fragment", "Silhouette", "Constellation", "Momentum", "Vapor", "Equinox", "Labyrinth", "Euphoria", "Vortex",
                                             "Crescendo", "Aurora?", "Ember!", "Paradox", "Celestial", "Anthem", "Mirage?", "Pulse", "Nocturne", "Radiant", "Spectrum?"])
        album = fake.word(ext_word_list=["Rebellion", "Sanctuary", "Afterglow", "Nightfall", "Daybreak", "Static",
                                         "Utopia", "Islands", "Threshold", "Limelight", "Paragon", "Visions",
                                         "Elysium", "Exodus", "Ascension"])
        genre = random.choice(GENRES)
        artist = fake.name()
        stream_date = fake.date_between(start_date="-1y", end_date="+30d")

        stream = {
            'listener': listener,
            'song': song,
            'album': album,
            'genre': genre,
            'artist': artist,
            'stream_date': stream_date.isoformat(),
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