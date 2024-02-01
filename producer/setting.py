def producerSetting():
    json = {
        'client.id': '',
        'bootstrap.server': broker,
        'acks': 'all',
        'enable.idempotence': ' true',
        'linger.ms': 100,
        'compression.type': 'gzip',
        'max.in.flight.requests.per.connection': 5
    }

def get_producer_config():
    producer_conf = {
        'bootstrap.servers': 'pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'SGMBTLJXGMJR46E2',
        'sasl.password': 'yEikfuODP3bgJORu4GZNQp2HfB54XQ4vAjApYHh2RHEM2LJNUrjjhlwhWkTFCEvq'
        }
    return producer_conf

def get_schema_config():
    schema_registry_conf = {'url': 'https://psrc-o268o.eu-central-1.aws.confluent.cloud',
                            'basic.auth.user.info': "LVG7OXSB3VKDDFZR:3anbUJAcGTOUL4c344N+XfgRRv4Q0EmQvQN2ebnc+LOQU8F+qFsDTfLtGmskzN/d"
    }
    return schema_registry_conf

input_topic = 'avroTwitter'
output_topic = 'sparkTwitter'
starting_offsets = 'latest'
checkpoint = 'checkpoint'