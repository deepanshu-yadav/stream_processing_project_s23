# Reference: https://denisecase.github.io/starting-spark/
"""Script for processing kafka streams finding which song is most popular among indian males.
"""
import os
from typing import List, Optional
from dotenv import load_dotenv
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StructType


from schema import EVENTS_SCHEMA, PROCESSED_SCHEMA

# load env variables
load_dotenv()
# https://github.com/delta-io/delta/issues/593#issuecomment-816678840
os.environ["PYSPARK_PIN_THREAD"] = "true"

# ===================================================================================
#       LOAD ENVIRONMENT VARIABLES & SET CONFIGURATIONS
# ===================================================================================
KAFKA_TOPIC_NAME = os.environ.get("KAFKA_EVENTS_TOPIC")
KAFKA_BOOTSTRAP_SERVER = (
    os.environ.get("KAFKA_BROKER_ADDRESS") + ":" + os.environ.get("KAFKA_BROKER_PORT")
) # "localhost:9092"

print(KAFKA_BOOTSTRAP_SERVER)
print(KAFKA_TOPIC_NAME)

# Required Spark  packages
PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
]


def create_or_get_spark(
    app_name: str, packages: List[str], cluster_manager="local[*]"
) -> SparkSession:
    """_summary_

    Args:
        app_name (str): Name of the spark application
        jars (str): List of jars needs to be installed before running spark application
        cluster_manager (str, optional): cluster manager Defaults to "local[*]".

    Returns:
        SparkSession: returns spark session
    """
    jars = ",".join(packages)

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.jars.packages", jars)
        .master(cluster_manager)
        .getOrCreate()
    )

    return spark


def create_read_stream(
    spark: SparkSession, broker_address: str, topic: str, offset: str = "earliest"
) -> DataStreamReader:
    """_summary_

    Args:
        spark (SparkSession): spark session
        broker_address (str): kafka broker address Ex: localhost:9092
        topic (str): topic from which events needs to consumed
        offset (str, optional): _description_. Defaults to "earliest".

    Returns:
        DataStreamReader: Interface used to load a streaming DataFrame from external storage systems

    Reference:
        https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    """
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker_address)
        .option("subscribe", topic)
        .option("startingOffsets", offset)
        .option(
            "key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer",
        )
        .option(
            "value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer",
        )
        .option("failOnDataLoss", False)
        .load()
    )
    return stream


def process_stream(df: DataFrame, schema: StructType) -> DataFrame:
    """_summary_

    Args:
        df (DataFrame): _description_
        schema (StructType): _description_

    Returns:
        DataFrame: _description_
    """
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(F.from_json(col("value"), schema).alias("data")).select("data.*")
    # Add month, day, hour to split the data into separate directories
    df = (
        df.withColumn("ts", F.to_timestamp(col("listen_timestamp")))
        .withColumn("year", year(col("ts")))
        .withColumn("month", month(col("ts")))
        .withColumn("hour", hour(col("ts")))
        .withColumn("day", dayofmonth(col("ts")))
        .drop("ts")
    )

    # Add month, day, hour to split the data into separate directories
    df = (
        df.withColumn("duration_minutes", F.round(col("duration_ms") / 60000.0, 2))
        .withColumn("latitude", F.round(col("latitude"), 3))
        .withColumn("longitude", F.round(col("longitude"), 3))
        .withColumn(
            "full_name", F.concat(col("first_name"), F.lit(" "), col("last_name"))
        )
    )

    return df


def create_write_stream(
    df: DataFrame, checkpoint_path: str, output_path: str, trigger: str = "2 minutes"
):
    """_summary_

    Args:
        df (DataFrame): _description_
        checkpoint_path (str): _description_
        output_path (str): _description_
        trigger (str, optional): _description_. Defaults to "2 minutes".

    Returns:
        _type_: _description_
    """
    stream = (
        df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("month", "day", "hour")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger)
    )
    return stream


# ===================================================================================
#                           MAIN ENTRYPOINT
# ===================================================================================

# Create a SparkSession
spark = create_or_get_spark(
    app_name="spotify_streaming", packages=PACKAGES, cluster_manager="local[*]"
)

print("Spark Session Created")

# read stream events
stream_df = create_read_stream(spark, KAFKA_BOOTSTRAP_SERVER, KAFKA_TOPIC_NAME)
print("Read Stream Created")

# process stream events
stream_df = process_stream(stream_df, EVENTS_SCHEMA)
print("Stream is Processed")

# Filter data for Indian Males 
male_stream = stream_df.filter(stream_df.gender == "male") # & stream_df.country == "India")

indian_male_stream = male_stream.filter(male_stream.country == "India")

# Group data by song ID and count occurrences
song_popularity_stream = indian_male_stream.groupBy("song_title").count()

# Find the most popular song
most_popular_song_stream = song_popularity_stream.orderBy(col("count").desc()).limit(1)

# # Print the results
query = most_popular_song_stream \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
