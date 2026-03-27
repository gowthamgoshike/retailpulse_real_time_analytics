from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from config.settings import KAFKA_BROKER, USER_ACTIVITY_TOPIC
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("Day8_UserActivity") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Define schema
# -----------------------------
user_activity_schema = StructType([
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("activity_type", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("page_url", StringType()),
    StructField("device_type", StringType()),
    StructField("traffic_source", StringType()),
    StructField("activity_timestamp", StringType())
])

# -----------------------------
# Read Kafka Stream
# -----------------------------
user_activity_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", USER_ACTIVITY_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------
# Parse JSON and convert timestamp
# -----------------------------
user_activity_df = user_activity_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), user_activity_schema).alias("data")) \
    .select("data.*") \
    .withColumn("activity_timestamp", to_timestamp("activity_timestamp"))

# -----------------------------
# Basic Transformations
# -----------------------------
# Example: filter meaningful activity (search, product_view, checkout)
user_activity_clean = user_activity_df.filter(
    col("activity_type").isin("search", "product_view", "checkout", "homepage_visit")
)

# -----------------------------
# Streaming query to console (for testing)
# -----------------------------
activity_query = user_activity_clean.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()