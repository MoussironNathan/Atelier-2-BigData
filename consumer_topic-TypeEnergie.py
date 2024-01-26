import json
from kafka import KafkaConsumer

consommateur = KafkaConsumer('TypeEnergie', group_id='TypeEnergie-vue', bootstrap_servers='localhost:9092', enable_auto_commit=False)
for msg in consommateur:
    mem = {}
    donnees = json.loads(msg.value)
    mem[msg.key.decode()] = donnees
    print(mem)
    consommateur.commit()