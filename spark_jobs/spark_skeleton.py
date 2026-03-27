from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from config.settings import KAFKA_BROKER, ORDERS_TOPIC, USER_ACTIVITY_TOPIC, TRANSACTIONS_TOPIC
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("RetailPulseStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Define schemas
# -----------------------------
order_schema = StructType([
    StructField("event_type", StringType()),
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", FloatType()),
    StructField("total_amount", FloatType()),
    StructField("order_status", StringType()),
    StructField("order_timestamp", StringType())
])

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

transaction_schema = StructType([
    StructField("event_type", StringType()),
    StructField("transaction_id", StringType()),
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("payment_method", StringType()),
    StructField("transaction_status", StringType()),
    StructField("amount", FloatType()),
    StructField("currency", StringType()),
    StructField("transaction_timestamp", StringType())
])

# -----------------------------
# Skeleton for reading Kafka streams
# -----------------------------
# Orders
orders_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ORDERS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# User Activity
user_activity_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", USER_ACTIVITY_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Transactions
transactions_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TRANSACTIONS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------
# Convert Kafka value from bytes to JSON
# -----------------------------
from pyspark.sql.functions import from_json, col

orders_df = orders_df.selectExpr("CAST(value AS STRING) as json") \
                     .select(from_json(col("json"), order_schema).alias("data")).select("data.*")

user_activity_df = user_activity_df.selectExpr("CAST(value AS STRING) as json") \
                     .select(from_json(col("json"), user_activity_schema).alias("data")).select("data.*")

transactions_df = transactions_df.selectExpr("CAST(value AS STRING) as json") \
                     .select(from_json(col("json"), transaction_schema).alias("data")).select("data.*")

# -----------------------------
# Placeholder for transformations
# -----------------------------
# Example: just print schema for now
orders_df.printSchema()
user_activity_df.printSchema()
transactions_df.printSchema()

# -----------------------------
# Placeholder for streaming query
# -----------------------------
# For skeleton, we just start a console output
orders_query = orders_df.writeStream.outputMode("append").format("console").start()
user_activity_query = user_activity_df.writeStream.outputMode("append").format("console").start()
transactions_query = transactions_df.writeStream.outputMode("append").format("console").start()

spark.streams.awaitAnyTermination()