from kagglehub import dataset_download
from dotenv import set_key, load_dotenv
import os

# Cargar variables existentes
dotenv_path = ".env"
load_dotenv(dotenv_path)

# Descargar dataset
path = dataset_download("elemento/nyc-yellow-taxi-trip-data")

# Guardar la ruta en .env usando python-dotenv
set_key(dotenv_path, "DATA_PATH", path)

print(f"Ruta guardada en .env: {path}")
