import json
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient
import numpy as np
import time

#Variablen/Konstanten
batch = []
BATCH_SIZE = 8
CONNNECTION = "mongodb://data_engineering:data@localhost:27017/?authSource=admin"
client = MongoClient(CONNNECTION) #mit Mongo-DB-Server verbinden
db = client["sensor-streaming"]
collection = db["sensor-data"]
analyse = True

#Kafka-Consumer deklarieren
consumer = KafkaConsumer('sensor_data',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset = 'earliest',
                         group_id='my_consumer_group',
                         enable_auto_commit=True,)

#Funktionen
def mongo_fill():

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
            df = pd.DataFrame(batch)
            print("\n--- Neuer Batch empfangen ---")
            print(df)

            result = collection.insert_many(batch)
            print(f"Eingefügt: {len(result.inserted_ids)} docs")
            batch.clear()
            print("Übergabe des Kafka-Stream an Mongo fertig.")
            return df

def analyse_fn(df):
    mongoDB = df
    print("ich analysiere das Dataframe")
    print(mongoDB)
    print(mongoDB.describe())

#thresholdcheck und spikedetection gemessen an den Unterschieden der Vorwerte

mongoDB = pd.DataFrame()
while analyse == True:

    choice = input("\n1 für Datenbankabfrage/ 2 für Analyse/ 3. für Ende: ")

    if choice == "1":
        mongoDB = mongo_fill()
        print(mongoDB)
    elif choice == "2":

        analyse_fn(mongoDB)

    else:
        print("Programm wird beendet")
        analyse=False
















