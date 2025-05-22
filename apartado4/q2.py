from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, percentile_approx, lit

# Import load_data and add_quadrants from load_data.py
from load_data import load_data, add_quadrants

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query_2_Enhanced_Trips_By_Day") \
        .getOrCreate()

    # Input file path (adjust as needed)
    file_path = "/home/ec2-user/yellow_tripdata_2015-01.csv"

    # Load data using load_data
    df = load_data(file_path, spark)
    
    # Número de viajes, ingreso promedio y percentiles por día de la semana
    result = df.groupBy("day_of_week") \
        .agg(
            count("*").alias("trip_count"),
            avg("total_amount").alias("avg_income"),
            percentile_approx("total_amount", lit(0.25)).alias("income_q1"),
            percentile_approx("total_amount", lit(0.5)).alias("income_median"),
            percentile_approx("total_amount", lit(0.75)).alias("income_q3")
        ) \
        .orderBy(
            # Ordenar por día de la semana (lunes a domingo)
            when(col("day_of_week") == "Monday", 1)
            .when(col("day_of_week") == "Tuesday", 2)
            .when(col("day_of_week") == "Wednesday", 3)
            .when(col("day_of_week") == "Thursday", 4)
            .when(col("day_of_week") == "Friday", 5)
            .when(col("day_of_week") == "Saturday", 6)
            .when(col("day_of_week") == "Sunday", 7)
        )
    
    print("\nAnálisis de viajes por día con percentiles de ingresos:")
    result.show()
    
    # Guardar resultados
    result.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://hadoop-master:9000/home/ec2-user/results/q2_enhanced")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()