#Projekt Architekturpunkte:######################
#Datenquelle: OpenAQ (https://docs.openaq.org/using-the-api/api-key)
#https://explore.openaq.org/?location=4794#12/52.36292/9.70612
#Data:Timestamps, Messungen in Hannover:
# NO mass µg/m³, NO₂ mass µg/m³, O₃ mass µg/m³, PM10 µg/m³, PM2.5 µg/m³
#Geplante Architektur:

"""[Python Producer] → [Kafka Topic] → [Python Consumer] → [MongoDB]
       ↑                                    ↓
   (API Abfrage)                    (Optional: Visualisierung)

Was jede Komponente tut:
KomponenteAufgabeProducerHolt Daten von OpenAQ, schickt sie als Message an KafkaKafkaNimmt Messages entgegen,
speichert sie in einem Topic, stellt sie für Consumer bereitConsumerLiest Messages aus Kafka,
schreibt sie in MongoDB
"""
#Die daten werden dann in MongoDB (non-SQL) hinterlegt.
#Organisation über Docke Compose
#Storage auf GitHub

#Code:##########################################
import pandas as pd
import requests
import os
from pathlib import Path
from dotenv import load_dotenv
import time
from kafka import KafkaProducer
import json



#PFADE und Variablen----------------------------------------
load_dotenv(Path(r"C:\GitHub\IU_Data_Engineering\.venv\key.env"))
print("Script um die Daten via API-Key von OpenAQ zu laden")
print("Request wird bei 100% ausgeführt und über Kafka an den Consumer gesendet.")
url = "https://api.openaq.org/v3/locations/4794" #Metadaten Location
URL = "https://api.openaq.org/v3/locations/2178/latest"#Sensordaten
API_KEY = os.getenv("OPENAQ_API_KEY")

topic_name= "sensor_data"
#Funktionen####################################
#Einmalgige Senosor_ID abfrage:

def sensor_request(api_key,sensor_lst):
    print(f"\nSensor-Request wird eingeholt. Folgende Sensoren bietet dieser Request------------------------")
    API_KEY = api_key
    sensor_lst = sensor_lst
    sensor_infos = []
    df = pd.DataFrame()
    for i in sensor_lst:
        URL = f"https://api.openaq.org/v3/sensors/{i}"
        response = requests.get(URL, headers={'X-API-Key': API_KEY})
        raw_js_data = response.json()
        # Extraktion auf "name" reduzieren:
        for sensors in raw_js_data['results']:
            sensor_infos.append({
                                 'sensor_id':sensors['id'],
                                 'sensor_target': sensors['parameter']['name'],
                                 'units': sensors['parameter']['units']
                                 })
        # json-data zu csv konvertieren:
        df = pd.DataFrame(sensor_infos)
        df.to_excel('sensor_data.xlsx', index=False)

    print(df)
    return df
    #print("Es folgt ein Mapping der SensorID auf die Funktion(Molekül:Target) und die entsprechende Einheit(Units)")


#Repeptive Datenabfrage:
def data_request(request_count, url, api_key):
    request_count = request_count
    URL = url
    API_KEY = api_key

    print(f"Request Nr {request_count} wird eingeholt")
    response = requests.get(URL, headers={'X-API-Key': API_KEY})
    raw_js_data = response.json()
    print(f"Die Keys der response sind {raw_js_data.keys()}")

    # json-data zu csv konvertieren:
    df = pd.DataFrame(raw_js_data['results'])
    df.to_excel('latest_response_data.xlsx', index=False)
    print("\nDie Sensoren im vorliegenden Request als Liste der IDs-------------------")
    sensor_id_lst = df['sensorsId'].tolist()
    return sensor_id_lst, df

#Setzt den Timer:
def request_timer ():
    sec = 0
    timer_value = 1800 #Testweise 10 Sekunden
    load_sign = '='
    percent = 0
    while timer_value > 0:
        if sec%18==0:
            percent=percent+1
            load_sign = load_sign+f'\rFortschritt:{percent}%'
            os.system('cls')
            print(load_sign, end="", flush=True)
        time.sleep(1)
        sec+= 1
        timer_value -= 1
    print(f"Timer {request_count} abgelaufen")

#Main-Fn:
if __name__ == '__main__':
    #Abfrage regelmäßig iterjieren
    request_count = 1
    while True:
        request_timer()
        sensor_lst, df_data = data_request(request_count,URL, API_KEY)
        print(sensor_lst)
        sensor_df = sensor_request(API_KEY, sensor_lst)
        df_data[['target', 'units']] = sensor_df[['sensor_target', 'units' ]].values
        request_count +=1
        print(df_data)
        producer = KafkaProducer(
            bootstrap_servers = 'localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for idx, row in df_data.iterrows():
            rec = row.to_dict()
            producer.send(topic_name, value=rec)
            print(f"Gesendet: {rec}")

        producer.flush()
        print("Alle Daten gesendet.")













