from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, count, percentile_approx, lit, stddev

# Import load_data and add_quadrants from load_data.py
from load_data import load_data, add_quadrants

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query_4_Enhanced_Time_Slots_Pickups") \
        .getOrCreate()

    # Input file path (adjust path as needed)
    file_path = "/home/ec2-user/yellow_tripdata_2015-01.csv"

    # Load data using load_data (already includes add_quadrants and timestamp parsing)
    df = load_data(file_path, spark)

    # Extract hour from pickup datetime
    df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

    # Aggregate number of pickups by hour with distance percentiles
    result = df.groupBy("pickup_hour").agg(
        count("VendorID").alias("pickup_count"),
        percentile_approx("trip_distance", lit(0.9)).alias("distance_p90"),
        stddev("total_amount").alias("price_stddev")
    ).orderBy(col("pickup_count").desc())

    # Show results
    print("\nAnálisis de pickups por hora con percentil 90 de distancias:")
    result.show()

    # Save to HDFS
    result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://hadoop-master:9000/home/ec2-user/results/q4")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()