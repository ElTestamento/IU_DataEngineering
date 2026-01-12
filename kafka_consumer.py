import json
import pandas as pd
from kafka import KafkaConsumer

consumer = KafkaConsumer('sensor_data',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset = 'earliest',
                         group_id='my_consumer_group')
rows = []
consumer_df = pd.DataFrame()
batch = []
BATCH_SIZE = 8

for message in consumer:
    raw = message.value.decode('utf-8')
    data = json.loads(raw)
    batch.append(data)
    if len(batch) == BATCH_SIZE:
        df = pd.DataFrame(batch)
        print("\n--- Neuer Batch empfangen ---")
        print(df)
        batch = []

