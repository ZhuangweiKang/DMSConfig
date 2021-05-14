import argparse
import time
from kafka import KafkaConsumer
from math import sin, cos, sqrt, atan2, radians, ceil
import utils
import os
import numpy as np
from threading import Thread

INTERVAL = 1
START_COR = (-74.913585, 41.474937)
CELL_SIZE = 500
MAX_CELLS = 300


# convert (lon, lat) to cell relative to the START_COR
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


def get_route(record):
    record = record.decode().split(',')
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


class MyConsumer(object):
    def __init__(self, args, sub_id):
        self.args = args
        self.sub_id = sub_id
        self.latency_samples = []
        self.e2e_latency_samples = []
        self.throughput_samples = []
        self.last_timestamp = None

    def capture_metrics(self, consumer_metrics, timestamp):
        now = time.time()
        try:
            metrics = consumer_metrics['consumer-fetch-manager-metrics']
            _latency = metrics['fetch-latency-avg']
            _throughput = metrics['bytes-consumed-rate']
            _e2e_latency = 1000 * now - timestamp
            self.latency_samples.append(_latency)
            self.throughput_samples.append(_throughput)
            self.e2e_latency_samples.append(_e2e_latency)
        except Exception:
            pass
        finally:
            self.last_timestamp = now

    def consume_msg(self):
        consumer = KafkaConsumer(args.topic, auto_offset_reset='latest', group_id='group-1',
                                 consumer_timeout_ms=1000 * args.execution_time,
                                 bootstrap_servers=[args.bootstrap_servers],
                                 fetch_min_bytes=args.fetch_min_bytes,
                                 fetch_max_wait_ms=args.fetch_wait_max_ms)

        os.system('rm *.log')

        records = {}
        start = time.time()
        while time.time() - start < args.execution_time:
            message_batch = consumer.poll()
            for partition_batch in message_batch.values():
                for message in partition_batch:
                    if not self.last_timestamp or (time.time() - self.last_timestamp > INTERVAL):
                        self.capture_metrics(consumer.metrics(), message.timestamp)

                    record = get_route(message.value)
                    cell = str(record[0])
                    if record[0]:
                        if cell not in records:
                            records[cell] = 1
                        else:
                            records[cell] += 1
        consumer.close()

    def get_latency(self):
        return utils.process_metrics(self.latency_samples)

    def get_throughput(self):
        return utils.process_metrics(self.throughput_samples)

    def get_e2e_latency(self):
        return utils.process_metrics(self.e2e_latency_samples)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', type=str, default='localhost:9092')
    parser.add_argument('--topic', type=str, default='nyc_taxi')
    parser.add_argument('--execution_time', type=int, default=120)
    parser.add_argument('--num_subs', type=int, default=1, help='the amount of subscriber threads')

    parser.add_argument('--fetch_wait_max_ms', type=int, default=500)
    parser.add_argument('--fetch_min_bytes', type=int, default=1)
    args = parser.parse_args()

    threads = []
    subs = []
    for i in range(args.num_subs):
        sub = MyConsumer(args, i)
        subs.append(sub)
        thr = Thread(target=sub.consume_msg, args=())
        threads.append(thr)
        thr.start()

    for thr in threads:
        thr.join()

    latency = []
    throughput = []
    e2e_latency = []

    for i in range(args.num_subs):
        latency.append(subs[i].get_latency())
        throughput.append(subs[i].get_throughput())
        e2e_latency.append(subs[i].get_e2e_latency())

    latency = np.array(latency).mean(axis=0).reshape(1, -1)
    throughput = np.array(throughput).mean(axis=0).reshape(1, -1)
    e2e_latency = np.array(e2e_latency).mean(axis=0).reshape(1, -1)

    np.savetxt('latency.log', latency, fmt='%.3f', delimiter=',')
    np.savetxt('throughput.log', throughput, fmt='%.3f', delimiter=',')
    np.savetxt('e2e_latency.log', e2e_latency, fmt='%.3f', delimiter=',')