from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, date_format, count, avg, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# Función auxiliar para determinar el cuadrante basado en coordenadas
def add_quadrants(df):
    # Para Nueva York, aproximadamente:
    # Centro de Manhattan está alrededor de -73.98, 40.75
    # Dividimos en cuadrantes basados en estas coordenadas centrales
    return df.withColumn(
        "pickup_quadrant", 
        when((col("pickup_longitude") == 0) | (col("pickup_latitude") == 0), "Invalid")
        .when((col("pickup_longitude") > -73.98) & (col("pickup_latitude") > 40.75), "NE")
        .when((col("pickup_longitude") > -73.98) & (col("pickup_latitude") <= 40.75), "SE")
        .when((col("pickup_longitude") <= -73.98) & (col("pickup_latitude") > 40.75), "NW")
        .when((col("pickup_longitude") <= -73.98) & (col("pickup_latitude") <= 40.75), "SW")
        .otherwise("Unknown")
    ).withColumn(
        "dropoff_quadrant", 
        when((col("dropoff_longitude") == 0) | (col("dropoff_latitude") == 0), "Invalid")
        .when((col("dropoff_longitude") > -73.98) & (col("dropoff_latitude") > 40.75), "NE")
        .when((col("dropoff_longitude") > -73.98) & (col("dropoff_latitude") <= 40.75), "SE")
        .when((col("dropoff_longitude") <= -73.98) & (col("dropoff_latitude") > 40.75), "NW")
        .when((col("dropoff_longitude") <= -73.98) & (col("dropoff_latitude") <= 40.75), "SW")
        .otherwise("Unknown")
    )

def load_data(path,spark):
    # Definir el esquema para los datos
    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    # Cargar los datos CSV
    # Usamos la ruta relativa según la estructura del proyecto descrita
    df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load("data/yellow_tripdata_2016-03.csv")
    
    # Mostrar las primeras filas para verificar que los datos se cargaron correctamente
    print("Muestra de los datos originales:")
    df.show(5)

    # Añadir columnas de cuadrantes
    df_with_quadrants = add_quadrants(df)

    # Añadir columna del día de la semana
    df_with_days = df_with_quadrants.withColumn(
        "day_of_week", 
        date_format(col("tpep_pickup_datetime"), "EEEE")
    )

    return df_with_days






