from kafka import KafkaConsumer
import json

BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
LEGIT_TOPIC_NAME = 'legit'

consumer = KafkaConsumer(LEGIT_TOPIC_NAME, 
                        bootstrap_servers=BROKERS,
                        value_deserializer = lambda value : json.loads(value)
                        )

for message in consumer:
    msg = message.value
    print(f'Legit Data : {msg}')