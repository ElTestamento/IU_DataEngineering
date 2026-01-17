OpenAQ Sensor Data Streaming Pipeline

Make sure:
•	Docker Desktop installed and running
Deps:
•	Python 3.10+ with dependencies: kafka-python, pymongo, pandas, matplotlib, seaborn
•	OpenAQ API Key (stored in .env file). Use your own unique ID please 
Quick Start
1. Start Docker Services
Ensure Docker Desktop is running, then start all containers:
docker-compose up -d
This launches Zookeeper, Kafka, and MongoDB.
2. Start the Producer
python kafka_producer.py
The producer queries the OpenAQ API every 30 minutes and sends sensor data to Kafka. Note: The sensors typically update hourly, so consecutive requests within the same hour may return identical data.
3. Start the Consumer
python kafka_consumer.py
Menu Options:
•	1 – Fetch data from Kafka and store in MongoDB (duplicates are automatically filtered)
•	2 – Run rudimentary analysis: displays threshold checks, outlier detection, and sensor trend plots
•	3 – Exit program
