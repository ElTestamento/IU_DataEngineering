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
#API-Key: cda1f91fb9fcd1bffd9bb4a4b049988da7b987ca0a3932a561f75eeb750124a2
#Code:##########################################
from httpx import request
from kafka.protocol import API_KEYS
#Einsatz des API-Key:

import pandas as pd
import requests
import openpyxl

print("Script um Die Daten via API-Key von OpenAQ zu laden")
url = "https://api.openaq.org/v3/locations/4794" #Metadaten Location
URL = "https://api.openaq.org/v3/locations/2178/latest"#Sensordaten
API_KEY = "cda1f91fb9fcd1bffd9bb4a4b049988da7b987ca0a3932a561f75eeb750124a2"

#Abfrage regelmäßig itereieren:
response = requests.get(URL, headers={'X-API-KEy' : API_KEY})
raw_data = response.json()
print(raw_data.keys())
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)

#json zu csv:
df = pd.DataFrame(raw_data['results'])
df.to_excel('response_data.xlsx', index = False)

print(df)








