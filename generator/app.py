from kafka import KafkaProducer
import time
import io
import avro.schema
from avro.io import DatumWriter
from create_data import generate_transaction_data

TOPIC_NAME = 'transaction'
BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
SCHEMA_PATH = "tranx.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

def avro_serializer(value: dict, schema: avro.schema.Schema=SCHEMA) -> bytes:
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)

    writer.write(value, encoder)

    return bytes_writer.getvalue()

producer = KafkaProducer(bootstrap_servers = BROKERS,
                        value_serializer = lambda rows : avro_serializer(rows)
                        )

while True:
    trax_data = generate_transaction_data()

    producer.send(TOPIC_NAME, trax_data)
    print(trax_data)

    time.sleep(3)