from confluent_kafka import Producer
import tweepy
import json
import pandas as pd
import csv

# Variáveis de configuração do Kafka
kafka_bootstrap_servers = 'localhost:9092'
topic = 'tweets_topic_1'

# Criação de um objeto API do Twitter
api = pd.read_csv('producer/twitter_dataset.csv')

# Função de entrega assíncrona do Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f'Erro ao enviar a mensagem para o Kafka: {err}')
    else:
        print(f'Mensagem enviada para o Kafka: {msg.value()}')

# Configurações do produtor Kafka
producer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'on_delivery': delivery_report,
    'acks': 'all',
    'enable.idempotence': 'true',
    'linger.ms': 100,
    'compression.type': 'gzip',
    'max.in.flight.requests.per.connection': 5
}

# Criação do produtor Kafka

producer = Producer(producer_conf)
with open('producer/arq.csv') as file:
    reader = csv.DictReader(file, delimiter=",")
    for line in reader:
        #dumps transforma dicionario em json
        json_data = json.dumps(line)
        producer.produce(topic, json_data.encode('utf-8'))
producer.flush()
            

