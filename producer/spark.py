from os.path import abspath
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from setting import *

warehouse = abspath('spark-warehouse')

if __name__ == '__main__':
    spark = SparkSession \
            .builder \
            .appName('twitter-stream') \
            .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
            .config('spark.sql.warehouse.dir', warehouse) \
            .enableHiveSupport() \
            .getOrCreate()
    
    spark.sparkContext.setLogLevel('INFO')

    with open("schemas/teste.avsc") as f:
        schema = f.read()

    jsonOptions = {'timestampFormat':'yyyy-MM-dd"T"HH:mm:ss.sss"Z"'}

    #print(bootstrap_servers)
    print(input_topic)
    print(output_topic)
    print(starting_offsets)

    stream_twitter = spark \
                .readStream \
                .format('kafka') \
                .option('kafka.bootstrap.server', get_producer_config()) \
                .option('subscribe', input_topic) \
                .option('startingOffsets', starting_offsets) \
                .option('checkpoint', checkpoint) \
                .load() \
                .select(from_json(col('value'.cast('string', schema, jsonOptions).alias('twitter'))))

    stream_twitter.printSchema()

    get_columns_for_stream_twitter = stream_twitter.select(
        col('twitter.Tweet_ID').alias('id'),
        col('twitter.Username').alias('name'),
        col('twitter.Text').alias('text'),
        col('twitter.Retweets').alias('retweets'),
        col('twitter.Likes').alias('likes'),
        col('twitter.Timestamp').alias('timestamp'),
    )

    write_console = get_columns_for_stream_twitter \
        .select('id', 'name', 'likes') \
        .select(to_json(struct(col('id'), col('name'), col('likes')))) \
        .writeStream \
        .format('console') \
        .start()
    print(write_console)

    write_topic = get_columns_for_stream_twitter \
        .select('id', 'name', 'likes') \
        .select(to_json(struct(col('id'), col('name'), col('likes')))) \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', bootstrap_servers) \
        .option('topic', output_topic) \
        .option('checkpointLocation', checkpoint) \
        .outputMode('append') \
        .start()
    print(write_console)
