from models import GreenTrip, green_trip_from_row, trip_serializer
from time import time
from kafka import KafkaProducer
import pandas as pd


# send all rows ...
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']

df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet', columns=columns)
print(df.head())

# Cover bad data cases
df['passenger_count'] = df['passenger_count'].fillna(0).astype(int)

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=trip_serializer
)

topic_name = 'green-trips'

t0 = time()

for _, row in df.iterrows():
    trip = green_trip_from_row(row)
    producer.send(topic_name, value=trip)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')