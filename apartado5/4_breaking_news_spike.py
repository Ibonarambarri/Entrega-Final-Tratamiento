from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "172.31.84.18:9094"    # CAMBIA por tu IP o localhost
KAFKA_TOPIC = "news_events"

event_schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

spark = SparkSession.builder.appName("BreakingNewsSpike").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "news_events").load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")).select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

result = df.withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "10 minutes"), "article_id") \
    .count().filter(col("count") > 10)

query = result.writeStream.outputMode("update").format("console").start()
query.awaitTermination()