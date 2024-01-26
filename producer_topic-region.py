from kafka import KafkaProducer
import time
from json import dumps
import requests
import json
import random
from datetime import datetime, timedelta


def fetch_data(date):
    url = f"http://127.0.0.1:5000/get_data?date={date}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return response

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: dumps(v).encode('utf-8'))

start_date = '2017-12-26-09:30'
formatted_date = datetime.strptime(start_date, '%Y-%m-%d-%H:%M')
date = formatted_date.strftime('%Y-%m-%d-%H:%M')

while True:
    data = fetch_data(date)
    aggregated_data = {}

    message = {
        "date": date,
        "data": data,
        "aggregated_data": aggregated_data
    }
    print(message, data['Région'])

    producer.send('Region', key=data['Région'], value=message)

    time.sleep(1)
    new_date = datetime.strptime(date, '%Y-%m-%d-%H:%M')
    next_date = new_date + timedelta(minutes=30)
    date = next_date.strftime('%Y-%m-%d-%H:%M')

