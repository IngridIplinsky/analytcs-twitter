from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
import csv
import json
from setting import *

schema_registry_client = SchemaRegistryClient(get_schema_config())

with open("schemas/teste.avsc") as f:
    value_schema = f.read()

avro_serializer = AvroSerializer(schema_registry_client, value_schema)

producer = Producer(get_producer_config())

with open('inputData/arq.csv') as file:
    reader = csv.DictReader(file, delimiter=",")
    for row in reader:
        json_data = json.dumps(row)
        producer.produce(topic='avroTwitter', value=json_data)

producer.flush()

# Função de entrega assíncrona do Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f'Erro ao enviar a mensagem para o Kafka: {err}')
    else:
        print(f'Mensagem enviada para o Kafka: {msg.value()}')