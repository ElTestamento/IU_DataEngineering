import json
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import seaborn as sns
import matplotlib.pyplot as plt

import streamlit as st

#Variablen/Konstanten
batch = []
BATCH_SIZE = 8
CONNNECTION = "mongodb://data_engineering:data@localhost:27017/?authSource=admin"
client = MongoClient(CONNNECTION) #mit Mongo-DB-Server verbinden
db = client["sensor-streaming"]
collection = db["sensor-data"]
collection.create_index([('datetime',1), ('sensorsId', 1)], unique=True)
analyse = True

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

def analyse_fn():

    analyse_mongo = list(collection.find(({})))
    df = pd.DataFrame(analyse_mongo)
    print("ich analysiere das Dataframe")
    analyse_df= df.drop(['_id','coordinates', 'sensorsId', 'locationsId'], axis='columns')
    print(analyse_df.columns)
    print('Convert to datetime')
    analyse_df['datetime'] = analyse_df['datetime'].apply(lambda x: x['utc'])
    analyse_df['datetime'] = pd.to_datetime(analyse_df['datetime'])
    print("Nun der Pivot der Tabellen ,damit Zeitreihenanalyse möglich wird.")
    df_clean_pivot = analyse_df.pivot_table(index='datetime', columns='target', values='value', aggfunc = 'first')
    print(df_clean_pivot.columns)
    print(df_clean_pivot)

    col_lst = ['co', 'no', 'no2', 'nox', 'o3', 'pm10', 'pm25', 'so2']
    ausreisser_liste = []
    threshold_liste = []

    for col in col_lst:
        mean_col = df_clean_pivot[col].mean()
        threshold = mean_col + (2 * mean_col / 10)  # +20%
        print(f"{col}: Mittelwert={round(mean_col, 4)}, Ausreißer-Grenze={round(threshold, 4)}")

        for datum, wert in df_clean_pivot[col].items():
            if pd.isna(wert):
                continue
            if wert > threshold:
                ausreisser_liste.append({'datum': datum, 'sensor': col, 'wert': round(wert, 4), 'typ': 'Ausreißer'})
            elif wert > mean_col:
                threshold_liste.append({'datum': datum, 'sensor': col, 'wert': round(wert, 4), 'typ': 'Überschreitung'})

    print("\n--- Ausreißer (>20% über Mittelwert) ---")
    for a in ausreisser_liste:
        print(f"{a['datum']} | {a['sensor']}: {a['wert']}")

    print("\n--- Threshold-Überschreitungen (über Mittelwert) ---")
    for t in threshold_liste:
        print(f"{t['datum']} | {t['sensor']}: {t['wert']}")

    fig, axes = plt.subplots(2, 4, figsize=(16, 8))
    axes = axes.flatten()
    for i, col in enumerate(col_lst):
        axes[i].plot(df_clean_pivot.index, df_clean_pivot[col])
        axes[i].set_title(col)

    plt.tight_layout()
    plt.show()

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














