from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# 1) argumentos
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1900, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    dag_id='test',
    default_args=default_args,
    description='DAG del proyecto de la pipeline de datos de ALTEN',
    schedule_interval='0 3 * * *',  # 3:00 UTC (Formato Cron)
    catchup=False
) as dag:

    # 2) Tareas de Inicio y Fin (Empty/Dummy)
    start = EmptyOperator(task_id='start') # EmptyOperator para las versiones más modernas de Airflow
    end = EmptyOperator(task_id='end')

    # 3) Lista de tareas

    N = 6 
    dummy_tasks = []

    for i in range(1, N + 1):
        task = EmptyOperator(task_id=f'task_{i}')
        dummy_tasks.append(task)
        
        start >> task

    # Separamos las tareas creadas por par o impar
    odd_tasks = [t for i, t in enumerate(dummy_tasks, start=1) if i % 2 != 0]
    even_tasks = [t for i, t in enumerate(dummy_tasks, start=1) if i % 2 == 0]

    # Hacemos que cada tarea par dependa de todas las tareas impares
    for even in even_tasks:
        odd_tasks >> even
        even >> end
        
# 5) ¿Qué es un Hook? ¿En qué se diferencia de una conexión? Puedes responder en un comentario dentro del código. 
#
# Una conexión almacena datos y credenciales esenciales para acceder a un servicio externo, API, etc. 
# Por ejemplo el JSON de la Service Account de Big Query.
# Un hook es el cliente de ejecución que se encarga de hacer la llamada API, la consulta SQL, etc.
