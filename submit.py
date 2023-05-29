import sys

sample = [{
    "ride_id": "bcf590c3-74ab-4fc2-88e2-b42575e5b9c1",
    "point_idx": 509,
    "latitude": 40.720870000000005,
    "longitude": -73.98159000000001,
    "timestamp": "2023-05-22T02:52:00.78627-04:00",
    "meter_reading": 15.530334,
    "meter_increment": 0.030511463,
    "ride_status": "enroute",
    "passenger_count": 5
} for _ in range(10000)]

memory = 1 * 1024 * 1024 * 1024

print(sys.getsizeof(sample))
print(memory / sys.getsizeof(sample))