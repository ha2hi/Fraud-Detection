from kafka import KafkaConsumer
import io
import avro.schema
import avro.io

BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']
FRAUD_TOPIC_NAME = 'fraud'
SCHEMA_PATH = "tranx.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

def avro_deserializer(value: bytes, schema: avro.schema.Schema=SCHEMA) -> dict:
    bytes_reader = io.BytesIO(value)
    decoder = avro.io.BinaryDecoder(bytes_reader)

    reader = avro.io.DatumReader(SCHEMA)
    message = reader.read(decoder)

    return message
    
consumer = KafkaConsumer(FRAUD_TOPIC_NAME, 
                        bootstrap_servers=BROKERS,
                        value_deserializer = lambda rows: avro_deserializer(rows)
                        )

for msg in consumer:
    message = msg.value

    print(f'Fraud Data!! : {message}')