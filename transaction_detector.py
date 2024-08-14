# transaction_detector.py
from kafka import KafkaConsumer, KafkaProducer
import io
import avro.schema
import avro.io
from avro.io import DatumWriter

BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
TRANSACTION_TOPIC_NAME = 'transaction'
LEGIT_TOPIC_NAME = 'legit'
FRAUD_TOPIC_NAME = 'fraud'
SCHEMA_PATH = "tranx.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

def is_suspicious(trax_msg: dict) -> bool:
    return trax_msg['TRANSACTION_TYPE'] == 'BITCOIN' and trax_msg['AMOUNT'] >= 80

def avro_serializer(value: dict, schema: avro.schema.Schema=SCHEMA) -> bytes:
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)

    writer.write(value, encoder)

    return bytes_writer.getvalue()

def avro_deserializer(value: bytes, schema: avro.schema.Schema=SCHEMA) -> dict:
    bytes_reader = io.BytesIO(value)
    decoder = avro.io.BinaryDecoder(bytes_reader)

    reader = avro.io.DatumReader(SCHEMA)
    message = reader.read(decoder)

    return message

consumer = KafkaConsumer(TRANSACTION_TOPIC_NAME, 
                        bootstrap_servers=BROKERS,
                        value_deserializer = lambda rows: avro_deserializer(rows)
                        )

producer = KafkaProducer(bootstrap_servers = BROKERS, 
                        value_serializer = lambda rows : avro_serializer(rows)
                        )
for msg in consumer:
    message = msg.value

    target_topic = FRAUD_TOPIC_NAME if is_suspicious(message) else LEGIT_TOPIC_NAME

    producer.send(target_topic, message)

    print(message)