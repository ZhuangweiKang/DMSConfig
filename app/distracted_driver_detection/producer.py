import os
import random
import argparse
import pickle
import time
from kafka import KafkaProducer
from threading import Thread
from utils import *

INTERVAL = 1  # the interval of recording producer metrics


class MyProducer(object):
    def __init__(self, args, pub_id):
        self.args = args
        self.pub_id = pub_id
        self.last_timestamp = None
        self.latency_records = []
        self.throughput_records = []

    # callback for computing latency
    def capture_metrics(self, pro_metrics):
        metrics = pro_metrics['producer-metrics']
        latency = metrics['request-latency-avg']
        throughput = metrics['outgoing-byte-rate']
        self.latency_records.append(latency)
        self.throughput_records.append(throughput)
        self.last_timestamp = time.time()

    def produce_msg(self):
        compression_type = None
        if self.args.compression_type != 'none':
            compression_type = self.args.compression_type
        producer = KafkaProducer(bootstrap_servers=self.args.bootstrap_servers,
                                 batch_size=self.args.batch_size,
                                 linger_ms=self.args.linger_ms,
                                 compression_type=compression_type,
                                 buffer_memory=self.args.buffer_memory)

        start = time.time()
        images = os.listdir(self.args.data_path)
        images = [x for x in images if 'img' in x]
        while time.time() - start < self.args.execution_time:
            rand_img = random.sample(images, 1)  # select an image from the testing dataset randomly
            img_path = self.args.data_path + "/" + rand_img
            send_data = normal(img_path)
            producer.send(topic=self.args.topic, value=pickle.dumps(send_data))
            if self.args.sync:
                producer.flush()
            if not self.last_timestamp or (time.time() - self.last_timestamp > INTERVAL):
                self.capture_metrics(producer.metrics())
            if self.args.sleep > 0:
                time.sleep(self.args.sleep)
        producer.close()

    def get_latency(self):
        return process_metrics(self.latency_records)

    def get_throughput(self):
        return process_metrics(self.throughput_records)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', type=str, default='localhost:9092')
    parser.add_argument('--data_path', type=str, default='data/imgs/test')
    parser.add_argument('--topic', type=str, default='distracted_driver_detection')
    parser.add_argument('--sleep', type=int, default=0, help='sleep in seconds between sends')
    parser.add_argument('--execution_time', type=int, default=120)
    parser.add_argument('--sync', action='store_true', default=False, help='run a synchronous producer')
    parser.add_argument('--num_pubs', default=1, type=int, help='the amount of producer threads')

    parser.add_argument('--batch_size', type=int, default=16384)
    parser.add_argument('--linger_ms', type=int, default=0)
    parser.add_argument('--compression_type', type=str, default='none', choices=['none', 'gzip', 'snappy', 'lz4'])
    parser.add_argument('--buffer_memory', type=int, default=33554432)
    args = parser.parse_args()

    threads = []
    pubs = []
    for i in range(args.num_pubs):  # run multiple producers as threads
        pub = MyProducer(args, i)
        pubs.append(pub)
        thr = Thread(target=pub.produce_msg, args=())
        threads.append(thr)
        thr.start()

    for thr in threads:
        thr.join()

    latency = []
    throughput = []
    e2e_latency = []

    for i in range(args.num_pubs):
        latency.append(pubs[i].get_latency())
        throughput.append(pubs[i].get_throughput())

    latency = np.array(latency).mean(axis=0).reshape(1, -1)
    throughput = np.array(throughput).mean(axis=0).reshape(1, -1)
    np.savetxt('latency.log', latency, fmt='%.3f', delimiter=',')
    np.savetxt('throughput.log', throughput, fmt='%.3f', delimiter=',')

