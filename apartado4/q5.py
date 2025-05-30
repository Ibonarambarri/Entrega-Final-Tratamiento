from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, skewness, variance

# Import load_data and add_quadrants from load_data.py
from load_data import load_data, add_quadrants

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query_5_Enhanced_Tip_Distribution") \
        .getOrCreate()

    # Input file path (adjust as needed)
    file_path = "/home/ec2-user/yellow_tripdata_2015-01.csv"

    # Load data using load_data
    df = load_data(file_path, spark)
    
    # Calcular métricas de propinas por cuadrante de destino con asimetría
    result = df.groupBy("dropoff_quadrant") \
        .agg(
            avg("tip_amount").alias("avg_tip"),
            avg(col("tip_amount") / col("fare_amount")).alias("tip_percentage"),
            sum("tip_amount").alias("total_tips"),
            count("*").alias("trip_count"),
            skewness("tip_amount").alias("tip_skewness"),
            variance("tip_amount").alias("tip_variance")
        ) \
        .orderBy(col("avg_tip").desc())
    
    print("\nDistribución de propinas por área con análisis de asimetría:")
    result.show()
    
    # Guardar los resultados como CSV para análisis posterior
    result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://hadoop-master:9000/home/ec2-user/results/q5")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()