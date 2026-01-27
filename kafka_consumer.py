import json
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import matplotlib.pyplot as plt
from config import batch, BATCH_SIZE, collection
from analyzer import analyse_fn

#Kafka-Consumer deklarieren
consumer = KafkaConsumer('sensor_data',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset = 'earliest',
                         group_id='tareks_sensor_consumer',
                         enable_auto_commit=True,
                         consumer_timeout_ms=5000)

#Funktionen
def mongo_fill():
    total_inserted = 0
    total_duplicates = 0

    for message in consumer:
        m = message.value

        if isinstance(m, (bytes, bytearray)):
            data = json.loads(m.decode("utf-8"))
        elif isinstance(m, dict):
            data = m
        else:
            raise TypeError(f"Unexpected message.value type: {type(m)}")

        batch.append(data)

        if len(batch) >= BATCH_SIZE:
            try:
                result = collection.insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)
            except BulkWriteError as e:
                total_inserted += e.details['nInserted']
                total_duplicates += len(e.details['writeErrors'])
            batch.clear()

    if batch:
        try:
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
        except BulkWriteError as e:
            total_inserted += e.details['nInserted']
            total_duplicates += len(e.details['writeErrors'])
        batch.clear()

    print(f"\nFertig: {total_inserted} eingefügt, {total_duplicates} Duplikate übersprungen")

mongoDB = pd.DataFrame()
while analyse == True:

    choice = input("\n1 für Datenbankabfrage/ 2 für Analyse/ 3. für Ende: ")
    if choice == "1":
        mongoDB = mongo_fill()
    elif choice == "2":
        analyse_fn()
    elif choice == "3":
        print("Programm wird beendet")
        analyse=False














