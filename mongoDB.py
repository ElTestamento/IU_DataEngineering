from pymongo import MongoClient

CONNNECTION = "mongodb://localhost:27017/"

client = MongoClient(CONNNECTION) #mit Server verbinden


sd = client["sensor-daten"]

