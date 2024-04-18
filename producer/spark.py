from os.path import abspath
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro
import avro.schema
import setting as conf

import os
 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

if __name__ == '__main__':
        warehouse = abspath('spark-warehouse')

        spark = SparkSession \
                .builder \
                .appName("Streaming from Kafka") \
                .config("spark.sql.streaming.checkpointLocation", 'checkpoint') \
                .config("spark.sql.shuffle.partitions", 4) \
                .config('spark.sql.warehouse.dir', warehouse) \
                .master("local[*]") \
                .getOrCreate()
                
        spark.sparkContext.setLogLevel('INFO')

        with open("schemas/esquema1.avsc") as f:
                schema1 = f.read()
        
        with open("schemas/esquema2.avsc") as f:
                schema2 = f.read()
        
        kafka_twitter_df = spark \
                        .read \
                        .format('kafka') \
                        .option('kafka.bootstrap.servers', conf.kafka_bootstrap_servers) \
                        .option('subscribe', conf.topic1) \
                        .option('startingOffsets', conf.starting_offsets) \
                        .load() 
        
        df_twitter_avro = kafka_twitter_df.select(from_avro('value', schema1).alias("Dados"))

        df_twitter_avro.printSchema()


        kafka_tweets_df = spark \
                        .read \
                        .format('kafka') \
                        .option('kafka.bootstrap.servers', conf.kafka_bootstrap_servers) \
                        .option('subscribe', conf.topic2) \
                        .option('startingOffsets', conf.starting_offsets) \
                        .load() 
        
        df_tweets_avro = kafka_tweets_df.select(from_avro('value', schema2).alias("Data"))

        df_tweets_avro.printSchema()
        
        df_twitter = df_twitter_avro.select(
                col('Dados.Tweet_ID').alias('id'),
                col('Dados.Username').alias('username'),
                col('Dados.Text').alias('text'),
                col('Dados.Retweets').alias('retweets'),
                col('Dados.Likes').alias('likes'),
                col('Dados.Timestamp').alias('timestamp'))


        df_tweets = df_tweets_avro.select(
                col('Data.id').alias('id'),
                col('Data.author').alias('username'),
                col('Data.content').alias('text'),
                col('Data.number_of_shares').alias('retweets'),
                col('Data.number_of_likes').alias('likes'),
                col('Data.date_time').alias('timestamp'))
        
        df_twitter = df_twitter.withColumn("date1", split(col("timestamp"), " ").getItem(0))\
                                .withColumn("hours1", split(col("timestamp"), " ").getItem(1))

        df_twitter = df_twitter.withColumn("hours", concat_ws(":", split(col("hours1"), ":").getItem(0), split(col("hours1"), ":").getItem(1)))
        
        df_tweets = df_tweets.withColumn("date1", split(col("timestamp"), " ").getItem(0))\
                                .withColumn("hours", split(col("timestamp"), " ").getItem(1))
        
        df_tweets = df_tweets.withColumn("dateDay", split(col("date1"), "/").getItem(0))\
                                .withColumn("dateMonth", split(col("date1"), "/").getItem(1))\
                                .withColumn("dateYear", split(col("date1"), "/").getItem(2))
        
        df_twitter = df_twitter.withColumn("dateDay", split(col("date1"), "-").getItem(2))\
                                .withColumn("dateMonth", split(col("date1"), "-").getItem(1))\
                                .withColumn("dateYear", split(col("date1"), "-").getItem(0))
        
        df_twitter = df_twitter.drop("timestamp","date1","hours1")
        df_tweets = df_tweets.drop("timestamp", "date1")


        df_tweets.select(to_json(struct(col('id'), col('username'), col('text'),col('retweets'),col('likes'), col('dateDay'), col('dateMonth'), col('dateYear'), col('hours'))).alias('value'))\
                .write \
                .format("kafka")\
                .option('kafka.bootstrap.servers', conf.kafka_bootstrap_servers) \
                .option('topic', conf.topic_spark) \
                .option('checkpointLocation', conf.checkpoint) \
                .save()
        
        df_twitter.select(to_json(struct(col('id'), col('username'), col('text'),col('retweets'),col('likes'), col('dateDay'), col('dateMonth'), col('dateYear'), col('hours'))).alias('value'))\
                .write \
                .format("kafka")\
                .option('kafka.bootstrap.servers', conf.kafka_bootstrap_servers) \
                .option('topic', conf.topic_spark) \
                .option('checkpointLocation', conf.checkpoint) \
                .save()

        
                        
        
