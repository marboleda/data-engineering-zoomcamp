from kafka import KafkaConsumer
from models import GreenTrip, trip_deserializer
import psycopg2

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='trips-console',
    value_deserializer=trip_deserializer
)

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

rides_greater_than_5 = 0;

for record in consumer:
    
    if (record.value.trip_distance > 5.0):
        rides_greater_than_5 += 1
        print(f"Tally of trips with trip_distance > 5km: {rides_greater_than_5}")

consumer.close()
cur.close()
conn.close()