
import dataclasses
import json

from dataclasses import dataclass

@dataclass
class GreenTrip:
    lpep_pickup_datetime: int # epoch milliseconds
    lpep_dropoff_datetime: int # epoch milliseconds
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

def green_trip_from_row(row):
    return GreenTrip(
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),    
    )


def trip_serializer(trip):
    green_trip_dict = dataclasses.asdict(trip)
    green_trip_json = json.dumps(green_trip_dict).encode('utf-8')
    return green_trip_json

def trip_deserializer(data):
    json_str = data.decode('utf-8')
    green_trip_dict = json.loads(json_str)
    return GreenTrip(**green_trip_dict)