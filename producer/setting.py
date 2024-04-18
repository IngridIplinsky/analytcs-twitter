# Função de entrega assíncrona do Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f'Erro ao enviar a mensagem para o Kafka: {err}')
    else:
        print(f'Mensagem enviada para o Kafka: {msg.value()}')

kafka_bootstrap_servers = 'localhost:9092'
topic1 = 'topic_twitter'
topic2 = 'topic_tweets'
topic_spark = 'topic_spark'

starting_offsets = 'earliest'
checkpoint = 'checkpoint'

producer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'on_delivery': delivery_report,
    'acks': 'all',
    'enable.idempotence': True,
    'linger.ms': 100,
    'compression.type': 'gzip',
    'max.in.flight.requests.per.connection': 5,
    "request.required.acks": -1, 
    "retries": 5,
}
