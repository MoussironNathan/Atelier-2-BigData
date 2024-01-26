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

def filter_energy_data(data, energy_type):
    columns_for_energy = {
        'Thermique': ['Consommation (MW)', 'Thermique (MW)','TCO Thermique (%)','TCH Thermique (%)'],
        'Nucléaire': ['Consommation (MW)', 'Nucléaire (MW)','TCO Nucléaire (%)','TCH Nucléaire (%)'],
        'Eolien': ['Consommation (MW)', 'Eolien (MW)','TCO Eolien (%)','TCH Eolien (%)'],
        'Solaire': ['Consommation (MW)', 'Solaire (MW)','TCO Solaire (%)','TCH Solaire (%)'],
        'Hydraulique': ['Consommation (MW)', 'Hydraulique (MW)','TCO Hydraulique (%)','TCH Hydraulique (%)'],
        'Pompage': ['Consommation (MW)', 'Pompage (MW)'],
        'Bioénergies': ['Consommation (MW)', 'Bioénergies (MW)','TCO Bioénergies (%)','TCH Bioénergies (%)'],
    }

    filtered_data = {key: data[key] for key in columns_for_energy[energy_type]}

    return filtered_data

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         key_serializer=str.encode,
                         value_serializer=lambda v: dumps(v).encode('utf-8'))

start_date = '2017-12-26-09:30'
formatted_date = datetime.strptime(start_date, '%Y-%m-%d-%H:%M')
date = formatted_date.strftime('%Y-%m-%d-%H:%M')

while True:
    data = fetch_data(date)
    aggregated_data = {}

    energylist = [
        'Thermique',
        'Nucléaire',
        'Eolien',
        'Solaire',
        'Hydraulique',
        'Pompage',
        'Bioénergies'
    ]
    for energy_type in energylist:

        filtered_data = filter_energy_data(data, energy_type)

        message = {
            "date": date,
            "data": filtered_data,
            "aggregated_data": aggregated_data
        }
        print(message)
        producer.send('TypeEnergie', key=energy_type, value=message)

    time.sleep(1)
    new_date = datetime.strptime(date, '%Y-%m-%d-%H:%M')
    next_date = new_date + timedelta(minutes=30)
    date = next_date.strftime('%Y-%m-%d-%H:%M')



