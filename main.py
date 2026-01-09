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
import openpyxl
from httpx import request
from kafka.protocol import API_KEYS

#PFADE----------------------------------------
load_dotenv(Path(r"C:\GitHub\IU_Data_Engineering\.venv\key.env"))
print("Script um Die Daten via API-Key von OpenAQ zu laden")
url = "https://api.openaq.org/v3/locations/4794" #Metadaten Location
URL = "https://api.openaq.org/v3/locations/2178/latest"#Sensordaten
API_KEY = os.getenv("OPENAQ_API_KEY")

#Funktionen####################################
def data_request(request_count, url, api_key):
    request_count = request_count
    URL = url
    API_KEY = api_key

    print(f"Request Nr {request_count} wird eingeholt")
    response = requests.get(URL, headers={'X-API-Key': API_KEY})
    raw_js_data = response.json()
    print(raw_js_data["results"][0])
    print(f"Die Keys der response sind {raw_js_data.keys()}")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_columns', None)

    # json-data zu csv konvertieren:
    df = pd.DataFrame(raw_js_data['results'])
    df.to_excel('latest_response_data.xlsx', index=False)
    print(df)

def request_timer ():
    sec = 1
    timer_value = 10 #Testweise 10 Sekunden
    while timer_value > 0:
        print(f"{sec}")
        time.sleep(1)
        sec += 1
        timer_value -= 1
    print(f"Timer abgelaufen: {request_count}")

if __name__ == '__main__':
    #Abfrage regelmäßig iterjieren
    request_count = 1

    while True:
        request_timer()
        data_request(request_count,URL, API_KEY)
        request_count +=1











