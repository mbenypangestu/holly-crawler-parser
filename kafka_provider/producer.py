import requests
import json

from bson import Binary, Code
from bson.json_util import dumps

from confluent_kafka import Producer
from confluent_kafka import KafkaException
from confluent_kafka import KafkaError


class CustomKafkaProducer:
    producer = Producer({'bootstrap.servers': 'localhost'})

    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.ms': 500,
            'batch.num.messages': 50,
            'default.topic.config': {'acks': 'all'}
        })

    def publish(self, topic, key, value):
        self.producer.produce(topic, dumps(value))

    def flush(self):
        self.producer.flush()


def handle_dr(err, msg):
    # Neither message payloads must not affect the error string.
    assert err is not None
    assert err.code() == KafkaError._MSG_TIMED_OUT
    assert "Message timed out" in err.str()
