import json
from kafka import KafkaProducer
import time
from create_data import generate_transaction_data

TOPIC_NAME = 'transaction'
BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']

producer = KafkaProducer(bootstrap_servers = BROKERS, 
                        value_serializer = lambda value : json.dumps(value).encode('utf-8')
                        )

while True:
    trax_data = generate_transaction_data()
    producer.send(TOPIC_NAME, trax_data)
    print(trax_data)

    time.sleep(2)
