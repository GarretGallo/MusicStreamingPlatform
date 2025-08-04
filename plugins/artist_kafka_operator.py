from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka import KafkaProducer
import json

class KafkaProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=100, *args, **kwargs):
        super(KafkaProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )


        for i in range(1, self.num_records + 1):
            payload = generate_random_data(i)
            producer.send(self.kafka_topic, value=payload)
            self.log.info(f"Sent #{i}: {payload!r}")
        producer.flush()
        return f"Produced {self.num_records} messages"