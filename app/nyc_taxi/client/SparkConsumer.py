import os
import argparse
import time
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
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
    record = record[1].decode().split(',')

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


# define the update function
def updateState(new, old):
    if old is None:
        old = 0
    return sum(new, old)


# save the top 10 routes into a CSV file
def findTop10(rdd):
    top_10_routes = []
    if rdd.count() > 0:
        out = rdd.repartition(1).take(10)
        for route in out:
            top_10_routes.append([route[0][0], route[0][1], route[1]])
        top_10_routes = pd.DataFrame(top_10_routes, columns=['start_cell', 'end_cell', 'count'])
        top_10_routes.to_csv('top10.csv', index=None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', type=str, required=True)
    parser.add_argument('--topic', type=str, default='nyc_taxi')
    parser.add_argument('--execution_time', type=int, default=120)

    parser.add_argument('--fetch_wait_max_ms', type=int, default=500)
    parser.add_argument('--fetch_min_bytes', type=int, default=1)
    parser.add_argument('--batch_interval', type=int, default=1)
    args = parser.parse_args()

    conf = SparkConf().setAppName('nyc_taxi').set('spark.hadoop.validateOutputSpecs', False)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    if os.path.exists('latency.log'):
        os.system('rm latency.log')

    START_COR = (-74.913585, 41.474937)
    CELL_SIZE = 500
    MAX_CELLS = 300

    # Create Spark Streaming context
    ssc = StreamingContext(sparkContext=sc, batchDuration=args.batch_interval)

    # Defining the checkpoint directory
    ssc.checkpoint("/root/tmp")

    # Connect to Kafka
    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc, kafkaParams={
        "metadata.broker.list": args.bootstrap_servers,
        "fetch.wait.max.ms": str(args.fetch_wait_max_ms),
        "fetch.min.bytes": str(args.fetch_min_bytes)
    }, topics=[args.topic])

    # main process
    kafkaStream.map(lambda record: get_route(record)) \
        .filter(lambda x: x[0]) \
        .reduceByKey(lambda a, b: a + b) \
        .updateStateByKey(updateState) \
        .transform(lambda rdd: rdd.sortBy(lambda x: -x[1])) \
        .foreachRDD(findTop10)

    ssc.start()
    ssc.awaitTerminationOrTimeout(args.execution_time)
    ssc.stop()
