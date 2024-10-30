from datetime import datetime

#Importing Appache Airflow

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'air_flow_user',
    'start_date': datetime(2024, 10, 27, 11, 00),
}

def gettin_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}

    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name']=res['name']['last']
    data['gender'] = res['gender']
    data['address']= f"{str(location['street']['number'])}"

    return data

def streaming_data():
    import json
#importing kafka
    from kafka import KafkaProducer

    data = gettin_data()
    formated_data = format_data(data)

    print(json.dumps(formated_data, indent= 3))
    # kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    producer.send('user_created', json.dumps(data).encode('utf-8'))

#Creating a DAG

# with DAG('user_automation',
#         default_args = default_args,
#         schedule_interval = '@daily',
#         catchup = False) as dag:
#
#     streaming_task = PythonOperator(
#         task_id = 'streaming_data_from_API',
#         python_callable = streaming_data
#     )

streaming_data()