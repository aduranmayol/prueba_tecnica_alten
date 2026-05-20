import requests
import json
from google.cloud import bigquery
import pandas as pd

url = "https://apis.codante.io/olympic-games/countries"
table_id = "alten-496917.SANDBOX_ARNAU.olympic_games_countries"

# LLAMADA A LA API PARA OBTENER DATA SOBRE LOS JUEGOS OLÍMPICOS 2024
response = requests.get(url, params={"page": 1})

response_json = response.json()
events_list = response_json.get("data", [])

# Creamos dataframe con pds
df = pd.DataFrame(events_list)
print(df)
# SUBIDA A BQ
client = bigquery.Client()
job = client.load_table_from_dataframe(df, table_id)

job.result()
print(f"Data cargada a BigQuery en la tabla {table_id}")
