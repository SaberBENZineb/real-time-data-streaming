from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 9, 15, 10, 00)
}

def get_data():
    import requests
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    return res['results'][0]

def format_data(res):
    import uuid
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from confluent_kafka import Producer
    import logging
    import time
    
    res = get_data()
    res = format_data(res)
    
    conf = {
        'bootstrap.servers': 'broker:29092',
        'security.protocol': 'PLAINTEXT',
        'acks': 'all'
    }

    producer = Producer(conf)

    curr_time=time.time()

    while True:
        if time.time() > curr_time + 30: #after 1 minute
            logging.error('Time out')
            break
        try:
            res=get_data()
            res=format_data(res)
            def acked(err, msg):
                if err is not None:
                    print(f"Failed to deliver message: {err.str()}")
                else:
                    print(f"Message produced: {msg.value().decode('utf-8')}")

            producer.produce('users_created', json.dumps(res).encode('utf-8'), callback=acked)
            producer.flush()
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

# Définition du DAG avec 'start_date'
with DAG(
        'userAutomation',
        default_args=default_args,
        schedule="@daily",
        catchup=False
) as dag:

    # Opérateur Python pour exécuter la fonction 'stream_data'
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
