from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "172.31.84.18:9094"
KAFKA_TOPIC = "news_events"

event_schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

spark = SparkSession.builder.appName("ActiveSessions").getOrCreate()

# Disable correctness check for multi-level stateful aggregation
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC).load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")).select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

# First aggregation
sessions = df.withWatermark("event_time", "5 minutes") \
    .groupBy(window(col("event_time"), "5 minutes"), "location", "session_id") \
    .count()

# Second aggregation
result = sessions.groupBy("location").count()

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "/tmp/checkpoint_active_sessions") \
    .start()

query.awaitTermination()
