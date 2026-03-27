from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, sum as spark_sum, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from config.settings import KAFKA_BROKER, ORDERS_TOPIC, TRANSACTIONS_TOPIC, USER_ACTIVITY_TOPIC
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("Day9_AggregateMetrics") \
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
# Read Kafka Streams
# -----------------------------
orders_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ORDERS_TOPIC) \
    .option("startingOffsets", "latest").load()

transactions_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TRANSACTIONS_TOPIC) \
    .option("startingOffsets", "latest").load()

user_activity_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", USER_ACTIVITY_TOPIC) \
    .option("startingOffsets", "latest").load()

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

user_activity_df = user_activity_df.selectExpr("CAST(value AS STRING) as json") \
                     .select(from_json(col("json"), user_activity_schema).alias("data")) \
                     .select("data.*") \
                     .withColumn("activity_timestamp", to_timestamp("activity_timestamp"))

# -----------------------------
# Filter clean streams
# -----------------------------
orders_clean = orders_df.filter(col("order_status") == "confirmed")
transactions_clean = transactions_df.filter(col("transaction_status") == "success")
user_activity_clean = user_activity_df.filter(
    col("activity_type").isin("search", "product_view", "checkout", "homepage_visit")
)

# -----------------------------
# Aggregate Metrics
# -----------------------------
# 1️⃣ Total Revenue per Product
revenue_df = orders_clean.groupBy("product_id", "product_name") \
    .agg(spark_sum("total_amount").alias("total_revenue"))

# 2️⃣ Clicks per Product
clicks_df = user_activity_clean.filter(col("activity_type") != "checkout") \
    .groupBy("product_id", "product_name") \
    .count().alias("click_count")

# 3️⃣ Approx Sessions per User
from pyspark.sql.functions import approx_count_distinct

sessions_df = user_activity_clean.groupBy("user_id") \
    .agg(approx_count_distinct("session_id").alias("session_count"))
# -----------------------------
# Output to console for testing
# -----------------------------
revenue_query = revenue_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

clicks_query = clicks_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

sessions_query = sessions_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()