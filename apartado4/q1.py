from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Import load_data and add_quadrants from load_data.py
from load_data import load_data, add_quadrants

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query_1_Common_Routes") \
        .getOrCreate()

    # Input file path (adjust as needed)
    file_path = "hdfs://hadoop-master:9000/home/ec2-user/yellow_tripdata_2015-01.csv"

    # Load data using load_data
    df = load_data(file_path, spark)
    
    # Las rutas más comunes (combinación de origen-destino)
    result = df.groupBy("pickup_quadrant", "dropoff_quadrant") \
        .count() \
        .orderBy(col("count").desc())
    
    print("\nRutas más comunes (origen-destino):")
    result.show(10)
    
    # Guardar resultados
    result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://hadoop-master:9000/home/ec2-user/results/q1")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()