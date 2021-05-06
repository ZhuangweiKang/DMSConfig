import os
import argparse
import time
from kafka import KafkaConsumer
from math import sin, cos, sqrt, atan2, radians, ceil


def distance_between_cors(cor1, cor2):
    """
    Calculate distance between two coordinates in meters
    :param cor1: (lat, lon)
    :param cor2: (lat, lon)
    :return:
    """
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(float(cor1[0]))
    lon1 = radians(float(cor1[1]))
    lat2 = radians(float(cor2[0]))
    lon2 = radians(float(cor2[1]))

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return abs(1000 * distance)


# convert (lon, lat) to cell relative to the START_COR
def get_route(record):
    record = record.decode().split(',')
    latency = time.time() - float(record[-1])
    print(latency)

    # record latency
    with open('latency.log', 'a+') as f:
        latency = time.time() - float(record[-1])
        print(latency)
        f.write(str(latency) + '\n')

    # pickup_longitude, pickup_latitude
    pick_lon = ceil(distance_between_cors(START_COR, (record[10], START_COR[1])) / CELL_SIZE)
    pick_lat = ceil(distance_between_cors(START_COR, (START_COR[0], record[11])) / CELL_SIZE)

    # dropoff_longitude, dropoff_latitude
    drop_lon = ceil(distance_between_cors(START_COR, (record[12], START_COR[1])) / CELL_SIZE)
    drop_lat = ceil(distance_between_cors(START_COR, (START_COR[0], record[13])) / CELL_SIZE)

    # drop illegal cell
    if pick_lat > CELL_SIZE or pick_lon > CELL_SIZE or drop_lat > CELL_SIZE or drop_lon > CELL_SIZE:
        return None, None
    else:
        return ('%d.%d' % (int(pick_lon), int(pick_lat)), '%d.%d' % (int(drop_lon), int(drop_lat))), 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', type=str, required=True)
    parser.add_argument('--topic', type=str, default='nyc_taxi')
    parser.add_argument('--execution_time', type=int, default=120)

    parser.add_argument('--fetch_wait_max_ms', type=int, default=500)
    parser.add_argument('--fetch_min_bytes', type=int, default=1)
    args = parser.parse_args()

    consumer = KafkaConsumer(args.topic, auto_offset_reset='latest', group_id='group-1',
                             consumer_timeout_ms=1000 * args.execution_time,
                             bootstrap_servers=[args.bootstrap_servers],
                             fetch_min_bytes=args.fetch_min_bytes,
                             fetch_max_wait_ms=args.fetch_wait_max_ms)

    if os.path.exists('latency.log'):
        os.system('rm latency.log')

    START_COR = (-74.913585, 41.474937)
    CELL_SIZE = 500
    MAX_CELLS = 300

    records = {}
    start = time.time()
    while time.time() - start < 120:
        message_batch = consumer.poll()
        for partition_batch in message_batch.values():
            for message in partition_batch:
                record = get_route(message.value)
                cell = str(record[0])
                if record[0]:
                    if cell not in records:
                        records[cell] = 1
                    else:
                        records[cell] += 1