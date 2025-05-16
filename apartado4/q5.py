from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count

# Import load_data and add_quadrants from load_data.py
from load_data import load_data, add_quadrants

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Query_5_Tip_Distribution_By_Destination") \
        .getOrCreate()

    # Input file path (adjust as needed)
    file_path = "/home/osboxes/Downloads/project/yellow_tripdata_2015-01.csv"

    # Load data using load_data
    df = load_data(file_path, spark)
    
    # Calcular métricas de propinas por cuadrante de destino
    tip_metrics_by_destination = df.groupBy("dropoff_quadrant") \
        .agg(
            avg("tip_amount").alias("avg_tip"),
            avg(col("tip_amount") / col("fare_amount")).alias("tip_percentage"),
            sum("tip_amount").alias("total_tips"),
            count("*").alias("trip_count")
        ) \
        .orderBy(col("avg_tip").desc())
    
    print("\nDistribución de propinas por área de destino:")
    tip_metrics_by_destination.show()
    
    # Guardar los resultados como CSV para análisis posterior
    tip_metrics_by_destination.coalesce(1).write.mode("overwrite").option("header", "true").csv("resultados/propinas_por_destino")
    print("Resultados guardados en el directorio 'resultados/propinas_por_destino'")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()