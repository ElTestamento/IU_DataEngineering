#Projekt Architekturpunkte:######################
#Datenquelle: OpenAQ (https://docs.openaq.org/using-the-api/api-key)
#https://explore.openaq.org/?location=4794#12/52.36292/9.70612
#Data:Timestamps, Messungen in Hannover:
# NO mass µg/m³, NO₂ mass µg/m³, O₃ mass µg/m³, PM10 µg/m³, PM2.5 µg/m³
#Geplante Architektur: Apache Kafka -> Phyton-Script für threshold-check mit Warnung und "spike-detect" anhand der kontinuierlichen Messungen.
#Die daten werden dann in MongoDB (non-SQL) hinterlegt.
#Organisation über Docke Compose
#Storage auf GitHub
#API-Key: cda1f91fb9fcd1bffd9bb4a4b049988da7b987ca0a3932a561f75eeb750124a2
#Code:##########################################

#Einsatz des API-Key:





