from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema for the incoming Kafka messages
schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("listen_timestamp", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("song_title", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("duration_ms", StringType(), True),
    StructField("danceability", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("key", StringType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("mode", StringType(), True),
    StructField("speechiness", DoubleType(), True),
    StructField("acousticness", DoubleType(), True),
    StructField("instrumentalness", DoubleType(), True),
    StructField("liveness", DoubleType(), True),
    StructField("valence", DoubleType(), True),
    StructField("tempo", DoubleType(), True)
])

# Create a Spark session
spark = SparkSession.builder.appName("KafkaSparkStructuredStreaming").getOrCreate()

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "127.0.0.1:9092",
    "subscribe": "spotify",
    "group.id": "my_consumer_group"  
}

# Read streaming data from Kafka using the defined schema
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .option("group.id", kafka_params["group.id"]) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Filter data for a specific city (e.g., "Bhavnagar")
bhavnagar_stream = streaming_df.filter(streaming_df.city == "Bhavnagar")

# Group data by song ID and count occurrences
song_popularity_stream = bhavnagar_stream.groupBy("song_id").count()

# Find the most popular song
most_popular_song_stream = song_popularity_stream.orderBy(col("count").desc()).limit(1)

# Print the results
query = most_popular_song_stream \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()