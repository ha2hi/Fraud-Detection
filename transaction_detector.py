from kafka import KafkaConsumer, KafkaProducer
import json

BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
TRANSACTION_TOPIC_NAME = 'transaction'
LEGIT_TOPIC_NAME = 'legit'
FRAUD_TOPIC_NAME = 'fraud'

def is_suspicious(trax_msg) -> bool:
    return trax_msg['TRANSACTION_TYPE'] == 'BITCOIN' and trax_msg['AMOUNT'] >= 80

consumer = KafkaConsumer(TRANSACTION_TOPIC_NAME, 
                        bootstrap_servers=BROKERS,
                        value_deserializer = lambda value : json.loads(value)
                        )


producer = KafkaProducer(bootstrap_servers = BROKERS, 
                        value_serializer = lambda value : json.dumps(value).encode('utf-8')
                        )

for message in consumer:
    msg = message.value
    target_topic = FRAUD_TOPIC_NAME if is_suspicious(msg) else LEGIT_TOPIC_NAME

    producer.send(target_topic, msg)
    print(msg)
