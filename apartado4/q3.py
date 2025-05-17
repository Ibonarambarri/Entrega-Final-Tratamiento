from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Import load_data and add_quadrants from load_data.py
from load_data import load_data, add_quadrants

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query_3_Total_Income_by_Destination") \
        .getOrCreate()

    # Input file path (adjust as needed)
    file_path = "/home/ec2-user/yellow_tripdata_2015-01.csv"

    # Load data using load_data
    df = load_data(file_path, spark)

    # Aggregate total income by dropoff quadrant
    result = df.groupBy("dropoff_quadrant").agg(
        sum("total_amount").alias("total_income")
    ).orderBy(col("total_income").desc())

    # Show results
    result.show()

    # Save to HDFS (uncomment and adjust path as needed)
    result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://hadoop-master:9000/home/ec2-user/results/q3")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()