from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import from_json, col

from config import configuration

KAFKA_BROKERS = 'kafka-broker-1:19092, kafka-broker-2:19092, kafka-broker-3:19092'

def main():
    spark = SparkSession.builder.appName("MusicStreamingProcessor") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    accoutSchema = StructType([
        StructField("account_id", StringType(), True),
        StructField('name', StringType(), True),
        StructField('email', StringType(), True),
        StructField('phone_number', StringType(), True),
        StructField('credit_card', StringType(), True),
        StructField('dob', StringType(), True),
        StructField('address', StringType(), True),
        StructField('city', StringType(), True),
        StructField('country', StringType(), True),
        StructField('registration_date', StringType(), True)])

    artistSchema=StructType([
        StructField("account_id", StringType(), True),
        StructField('name', StringType(), True),
        StructField('biography', StringType(), True),
        StructField('dob', StringType(), True),
        StructField('country', StringType(), True),
        StructField('city', StringType(), True),
        StructField('genre', StringType(), True),
        StructField('record_label', StringType(), True),
        StructField('number_albums', IntegerType(), True),
        StructField('number_tracks', IntegerType(), True)])

    musicSchema=StructType([
        StructField("song", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("album", StringType(), True),
        StructField("length", IntegerType(), True),
        StructField("genre", StringType(), True),
        StructField("release_date", StringType(), True)])

    streamSchema=StructType([
        StructField("listener", StringType(), True),
        StructField("song", StringType(), True),
        StructField("album", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("stream_date", StringType(), True),
        StructField("stream_country", StringType(), True),
        StructField("stream_city", StringType(), True)])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', KAFKA_BROKERS)
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                )

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    accountDF = read_kafka_topic('account_data', accoutSchema).alias('account')
    artistDF = read_kafka_topic('artist_data', artistSchema).alias('artist')
    musicDF = read_kafka_topic('music_data', musicSchema).alias('music')
    streamDF = read_kafka_topic('stream_data', streamSchema).alias('stream')

    query1 = streamWriter(accountDF, 's3a://music-streaming-data-gg/checkpoints/account_data',
                          's3a://music-streaming-data-gg/data/account_data')
    query2 = streamWriter(artistDF, 's3a://music-streaming-data-gg/checkpoints/artist_data',
                          's3a://music-streaming-data-gg/data/artist_data')
    query3 = streamWriter(musicDF, 's3a://music-streaming-data-gg/checkpoints/music_data',
                          's3a://music-streaming-data-gg/data/music_data')
    query4 = streamWriter(streamDF, 's3a://music-streaming-data-gg/checkpoints/stream_data',
                          's3a://music-streaming-data-gg/data/stream_data')

    query4.awaitTermination()

if __name__ == "__main__":
    main()
