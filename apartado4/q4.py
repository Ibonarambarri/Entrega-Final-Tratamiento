from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, count

# Import load_data and add_quadrants from load_data.py
from load_data import load_data, add_quadrants

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query_4_Time_Slots_Pickups") \
        .getOrCreate()

    # Input file path (adjust path as needed)
    file_path = "/home/osboxes/Downloads/project/yellow_tripdata_2015-01.csv"

    # Load data using load_data (already includes add_quadrants and timestamp parsing)
    df = load_data(file_path, spark)

    # Extract hour from pickup datetime
    df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

    # Aggregate number of pickups by hour
    result = df.groupBy("pickup_hour").agg(
        count("VendorID").alias("pickup_count")
    ).orderBy(col("pickup_count").desc())

    # Show results
    result.show()

    # Save to HDFS (optional)
    # result.write.mode("overwrite").csv("hdfs://<your-hdfs-path>/query4_result")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
