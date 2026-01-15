import json
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

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
            print(df.columns)
            try:
                result = collection.insert_many(batch, ordered=False)
                print(f"Eingefügt: {len(result.inserted_ids)} docs")
            except BulkWriteError as e:
                print(f"Eingefügt: {e.details['nInserted']}, Duplikate übersprungen: {len(e.details['writeErrors'])}")
            batch.clear()
            print("Übergabe des Kafka-Stream an Mongo fertig.")
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)  # Passt die Breite an das Terminal an
            pd.set_option('display.max_colwidth', None)  # Zeigt den vollständigen Inhalt von Zellen an
            df_clean=df.drop(['coordinates', 'sensorsId', 'locationsId'], axis='columns')
            print(df_clean.columns)
            print('Convert to datetime')
            df_clean['datetime'] = df_clean['datetime'].apply(lambda x: x['utc'])
            df_clean['datetime'] = pd. to_datetime(df_clean['datetime'])
            print("Nun der Pivot der Tabellen ,damit Zeitreihenanalyse möglich wird.")
            df_clean_pivot = df_clean.pivot(index='datetime', columns='target', values='value')
            print(df_clean_pivot.columns)
            return df_clean_pivot


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
    for i in col_lst:
        mean_col = df_clean_pivot[i].mean()
        for j in df_clean_pivot[i]:
            if j > mean_col:
                print(f"Threshold(Mean) übershcritten:{j}")
                if j >= mean_col+(2*mean_col/10):
                    print(f"Der Wert überragt 20% des Mittelwerts({mean_col}) und ist damit ein Ausreißer: {j}/{mean_col+(2*mean_col/10)}")
            elif j<= mean_col:
                print(f"Threshold(Mean) im Normbereich:{j}")


#thresholdcheck und spikedetection gemessen an den Unterschieden der Vorwerte
#Ausgabe mit Streamlit/Flet

mongoDB = pd.DataFrame()
while analyse == True:

    choice = input("\n1 für Datenbankabfrage/ 2 für Analyse/ 3. für Ende: ")
    if choice == "1":
        mongoDB = mongo_fill()
        print(mongoDB)
    elif choice == "2":
        analyse_fn()
    else:
        print("Programm wird beendet")
        analyse=False
















