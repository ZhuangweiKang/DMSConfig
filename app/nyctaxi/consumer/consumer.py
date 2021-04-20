import os
import argparse
import time
import pandas as pd
import numpy as np
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

def process_data():
    data = pd.read_csv('latency.log').to_numpy().squeeze()
    min_val = np.min(data)
    max_val = np.max(data)
    median_val = np.median(data)
    mean_val = np.mean(data)
    std_val = np.std(data)
    p_vals = np.percentile(data, [25, 50, 75, 90, 95, 99])

    vals = [min_val, max_val, median_val, mean_val, std_val]
    vals.extend(p_vals)
    vals = pd.DataFrame(vals).round(2).T
    vals.columns = ['min','max','median','mean','std','25th','50th','75th','90th','95th','99th']
    vals.to_csv('latency.csv', index=None)
    os.system('rm latency.log')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', type=str, required=True)
    parser.add_argument('--topic', type=str, default='nyc_taxi')
    parser.add_argument('--execution_time', type=int, default=120)

    parser.add_argument('--fetch_max_wait_ms', type=int, default=500)
    parser.add_argument('--fetch_min_bytes', type=int, default=16384)
    args = parser.parse_args()

    conf = SparkConf().setAppName('nyc_taxi').set('spark.hadoop.validateOutputSpecs', False)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    START_COR = (-74.913585, 41.474937)
    CELL_SIZE = 500
    MAX_CELLS = 300

    # METRICS
    throughput = 0
    delay = None

    # Create Spark Streaming context
    ssc = StreamingContext(sparkContext=sc, batchDuration=ceil(args.fetch_max_wait_ms/1000.0))  # convert ms --> s

    # Defining the checkpoint directory
    ssc.checkpoint("/root/tmp")

    # Connect to Kafka
    kafkaStream = KafkaUtils.createDirectStream(ssc=ssc, kafkaParams={"metadata.broker.list": args.bootstrap_servers, "fetch.min.bytes": str(args.fetch_min_bytes)}, topics=[args.topic])

    # convert (lon, lat) to cell relative to the START_COR
    def get_route(record):
        record = record[1].split(',')

        # record latency
        with open('latency.log', 'a+') as f:
          latency = time.time() - float(record[-1])
          f.write(str(latency)+'\n')

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
    def saveTop10(rdd):
        top_10_routes = []
        if rdd.count() > 0:
            out = rdd.repartition(1).take(10)
            for route in out:
                top_10_routes.append([route[0][0], route[0][1], route[1]])
            top_10_routes = pd.DataFrame(top_10_routes, columns=['start_cell', 'end_cell', 'count'])
            top_10_routes.to_csv('top10.csv', index=None)

    # main process
    kafkaStream.map(lambda record: get_route(record)) \
        .filter(lambda x: x[0]) \
        .reduceByKey(lambda a, b: a + b) \
        .updateStateByKey(updateState) \
        .transform(lambda rdd: rdd.sortBy(lambda x: -x[1])) \
        .foreachRDD(saveTop10)

    ssc.start()
    ssc.awaitTerminationOrTimeout(args.execution_time)
    ssc.stop()
    process_data()
    