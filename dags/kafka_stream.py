import time
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
#transform data
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
    import time
    import logging
    #importing kafka
    from kafka import KafkaProducer

    logging.info("Starting streaming_data function")

    # kafka producer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) #kafka consumer broker

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #This is a minute
            break #breaking the loop after 1 min
        try:
            data = gettin_data()
            formated_data = format_data(data)
            producer.send('user_created', json.dumps(data).encode('utf-8'))
        except Exception as error:
            logging.error(f"An error occured in the load data: {error}")
            continue

#Creating a DAG

with DAG('user_automation',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False) as dag:

    streaming_task = PythonOperator(
        task_id = 'streaming_data_from_API',
        python_callable=streaming_data
    )