import json
import pandas as pd
from kafka import KafkaConsumer
from pymongo import MongoClient


consumer = KafkaConsumer('sensor_data',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset = 'earliest',
                         group_id='my_consumer_group',
                         enable_auto_commit=True,)


batch = []
BATCH_SIZE = 8
CONNNECTION = "mongodb://data_engineering:data@localhost:27017/?authSource=admin"
client = MongoClient(CONNNECTION) #mit Mongo-DB-Server verbinden
db = client["sensor-streaming"]
collection = db["sensor-data"]

for message in consumer:
    m = message.value

    if isinstance(m, (bytes, bytearray)):
        data = json.loads(m.decode("utf-8"))
    elif isinstance(m, dict):
        data = m
    else:
        raise TypeError(f"Unexpected message.value type: {type(m)}")

    batch.append(data)
    #raw = message.value.decode('utf-8')
    #data = json.loads(raw)

    if len(batch) >= BATCH_SIZE:
        df = pd.DataFrame(batch)
        print("\n--- Neuer Batch empfangen ---")
        print(df)

        result = collection.insert_many(batch)
        print(f"Eingefügt: {len(result.inserted_ids)} docs")
        batch.clear()








