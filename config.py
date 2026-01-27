from dotenv import load_dotenv
load_dotenv()
import os
from pymongo import MongoClient

#Variablen/Konstanten
batch = []
BATCH_SIZE = 8
CONNNECTION = "mongodb://data_engineering:data@localhost:27017/?authSource=admin"
client = MongoClient(CONNNECTION) #mit Mongo-DB-Server verbinden
db = client["sensor-streaming"]
collection = db["sensor-data"]
collection.create_index([('datetime',1), ('sensorsId', 1)], unique=True)
url = "https://api.openaq.org/v3/locations/4794" #Metadaten Location
URL = "https://api.openaq.org/v3/locations/2178/latest"#Sensordaten
API_KEY = os.getenv("OPENAQ_API_KEY")
topic_name= "sensor_data"

