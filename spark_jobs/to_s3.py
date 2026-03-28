# to_s3.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, sum as spark_sum, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from dotenv import load_dotenv
import os

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC")
TRANSACTIONS_TOPIC = os.getenv("TRANSACTIONS_TOPIC")
USER_ACTIVITY_TOPIC = os.getenv("USER_ACTIVITY_TOPIC")

if not all([AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET, AWS_REGION, KAFKA_BROKER]):
    raise ValueError("Some AWS or Kafka credentials are missing in environment variables")

# -----------------------------
# Initialize Spark
# -----------------------------
spark = SparkSession.builder \
    .appName("Day10_SaveToS3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Configure S3 access
# -----------------------------
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
hadoop_conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.path.style.access", "true")

# -----------------------------
# Schemas
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
# Read Kafka streams
# -----------------------------
orders_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ORDERS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("order_timestamp", to_timestamp("order_timestamp"))

transactions_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TRANSACTIONS_TOPIC) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), transaction_schema).alias("data")) \
    .select("data.*") \
    .withColumn("transaction_timestamp", to_timestamp("transaction_timestamp"))

user_activity_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", USER_ACTIVITY_TOPIC) \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), user_activity_schema).alias("data")) \
    .select("data.*") \
    .withColumn("activity_timestamp", to_timestamp("activity_timestamp"))

# -----------------------------
# Raw streams → S3 (append mode)
# -----------------------------
orders_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/raw/orders") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/orders") \
    .outputMode("append") \
    .start()

transactions_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/raw/transactions") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/transactions") \
    .outputMode("append") \
    .start()

user_activity_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/raw/user_activity") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/user_activity") \
    .outputMode("append") \
    .start()

# -----------------------------
# Aggregations → append mode with watermark
# -----------------------------
# Revenue per product
revenue_df = orders_df \
    .withWatermark("order_timestamp", "10 minutes") \
    .groupBy(
        col("product_id"),
        col("product_name"),
        col("order_timestamp")
    ) \
    .agg(spark_sum("total_amount").alias("total_revenue"))

revenue_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/processed/revenue") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/revenue") \
    .outputMode("append") \
    .start()

# Clicks per product
clicks_df = user_activity_df \
    .withWatermark("activity_timestamp", "10 minutes") \
    .groupBy(
        col("product_id"),
        col("product_name"),
        col("activity_timestamp")
    ) \
    .agg(approx_count_distinct("session_id").alias("unique_clicks"))

clicks_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/processed/clicks") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/clicks") \
    .outputMode("append") \
    .start()

# Active sessions per user
sessions_df = user_activity_df \
    .withWatermark("activity_timestamp", "10 minutes") \
    .groupBy(
        col("user_id"),
        col("activity_timestamp")
    ) \
    .agg(approx_count_distinct("session_id").alias("active_sessions"))

sessions_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/processed/sessions") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/sessions") \
    .outputMode("append") \
    .start()

# -----------------------------
# Await termination
# -----------------------------
spark.streams.awaitAnyTermination()