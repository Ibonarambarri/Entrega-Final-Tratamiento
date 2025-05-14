from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType

# 👉 Aquí va tu bloque
KAFKA_BOOTSTRAP_SERVERS = "event-generator-private-ip:9094"   # CAMBIA por tu IP o localhost
KAFKA_TOPIC = "news_events"

event_schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

spark = SparkSession.builder.appName("DeviceUsage").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "event-generator-private-ip:9094") \
    .option("subscribe", "news_events").load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")).select("data.*")

result = df.groupBy("device_type").count()

query = result.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()