from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, from_utc_timestamp 
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
import os
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "openweather_topic")
OUTPUT_PATH = os.getenv("OUTPUT_PATH")
APP_NAME = os.getenv("APP_NAME", "OpenWeatherConsumerRAW")
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "86400"))

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"[Spark Session] --> OK. AppName: {spark.sparkContext.appName}")

schema = StructType([
    StructField("source", StringType()),
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ts", LongType()),
    StructField("datetime_utc", StringType()), 
    StructField("temp_c", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("wind_speed", DoubleType())
])

df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") 

df_partitioned = df_parsed \
    .withColumn("ts_utc", from_utc_timestamp(col("datetime_utc"), "UTC")) \
    .withColumn("year", date_format(col("ts_utc"), "yyyy")) \
    .withColumn("month", date_format(col("ts_utc"), "MM")) \
    .withColumn("day", date_format(col("ts_utc"), "dd")) \
    .drop("ts_utc")

df_coalesced = df_partitioned.coalesce(1)

writer = df_coalesced.writeStream \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", OUTPUT_PATH + "_checkpoints/") \
    .partitionBy("year", "month", "day") \
    .outputMode("append")

query = writer.trigger(processingTime=f"{TRIGGER_SECONDS} seconds").start()
print(f"[Spark Structured Streaming] --> OK. S3: {OUTPUT_PATH}")
query.awaitTermination()
