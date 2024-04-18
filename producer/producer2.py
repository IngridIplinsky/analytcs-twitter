from confluent_kafka import Producer
import csv
import io
import avro.schema
import avro.io
import setting as conf


schema = avro.schema.parse(open('schemas/esquema2.avsc', "rb").read())

# Criação do produtor Kafka
producer = Producer(conf.producer_conf)

# Nome do arquivo CSV de entrada
csv_filename = "inputData/tweets.csv"

# Abra o arquivo CSV de entrada
with open(csv_filename, 'r', newline='', encoding="utf-8") as csvfile: 

    # Leia o arquivo CSV e escreva no arquivo Avro
    csv_reader = csv.DictReader(csvfile)

    for row in csv_reader:
        # Certifique-se de que os dados estão no formato correto conforme o esquema Avro
        avro_record = {"author": row["author"], "content": row["content"], "country": row["country"], "date_time": row["date_time"], "id": row["id"], "language": row["language"], "latitude": row["latitude"],"longitude": row["longitude"],"number_of_likes": int(row["number_of_likes"]),"number_of_shares": int(row["number_of_shares"])}
        
        #transformação dos dados em avro
        #transformação dos dados em bytes para o Producer poder enviar
        bytes_writer = io.BytesIO()
        avro_writer = avro.io.DatumWriter(schema)
        encoder = avro.io.BinaryEncoder(bytes_writer)
        avro_writer.write(avro_record, encoder)
       
        raw_bytes = bytes_writer.getvalue()

        producer.produce(conf.topic2, value=raw_bytes)

        # Envia o registro para o tópico Kafka
        producer.flush()
        
