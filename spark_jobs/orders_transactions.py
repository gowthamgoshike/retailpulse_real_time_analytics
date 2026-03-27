from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from config.settings import KAFKA_BROKER, ORDERS_TOPIC, TRANSACTIONS_TOPIC
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("Day7_OrdersTransactions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Define Schemas
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
# Read Kafka Streams
# -----------------------------
orders_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ORDERS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

transactions_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TRANSACTIONS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------
# Parse JSON and convert timestamps
# -----------------------------
orders_df = orders_df.selectExpr("CAST(value AS STRING) as json") \
                     .select(from_json(col("json"), order_schema).alias("data")) \
                     .select("data.*") \
                     .withColumn("order_timestamp", to_timestamp("order_timestamp"))

transactions_df = transactions_df.selectExpr("CAST(value AS STRING) as json") \
                     .select(from_json(col("json"), transaction_schema).alias("data")) \
                     .select("data.*") \
                     .withColumn("transaction_timestamp", to_timestamp("transaction_timestamp"))

# -----------------------------
# Basic Transformations
# -----------------------------
# Example: filter confirmed orders and successful transactions
orders_clean = orders_df.filter(col("order_status") == "confirmed")
transactions_clean = transactions_df.filter(col("transaction_status") == "success")

# Compute revenue per order as extra column (if not already correct)
orders_clean = orders_clean.withColumn("computed_total_amount", col("quantity") * col("price"))

# -----------------------------
# Streaming queries to console (for testing)
# -----------------------------
orders_query = orders_clean.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

transactions_query = transactions_clean.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()