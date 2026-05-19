#pip install requests
#pip install google-cloud-bigquery pandas pyarrow

import requests
import json
from google.cloud import bigquery
import pandas as pd

url = "https://apis.codante.io/olympic-games/events"
table_id = "ALTEN.SANDBOX.olympic_games_events"

# LLAMADA A LA API PARA OBTENER DATA SOBRE LOS JUEGOS OLÍMPICOS 2024
response = requests.get(url, params={"page": 1})

data = response.json()


# Creamos dataframe con pds
df = pd.DataFrame(data)

# SUBIDA A BQ
client = bigquery.Client()
job = client.load_table_from_dataframe(df, table_id)

job.result()

print(f"Data cargada a BigQuery en la tabla {table_id}")