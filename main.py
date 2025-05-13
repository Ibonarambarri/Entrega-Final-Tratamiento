import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import kagglehub
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Crear directorio resultados si no existe
if not os.path.exists('resultados'):
    os.makedirs('resultados')
    print("Directorio 'resultados' creado exitosamente")
else:
    print("El directorio 'resultados' ya existe")

sc = SparkContext.getOrCreate()
sc
spark = SparkSession.builder.getOrCreate()
print(spark)

df = data(path,spark)
