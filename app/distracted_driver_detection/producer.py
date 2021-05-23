import os
import random
import argparse
import pickle
import time
from kafka import KafkaProducer
from utils import *

INTERVAL = 1  # the interval of recording producer metrics


class MyProducer(object):
    def __init__(self, args):
        self.args = args
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
        print(latency, throughput)

    def produce_msg(self):
        compression_type = None
        if self.args.compression_type != 'none':
            compression_type = self.args.compression_type
        producer = KafkaProducer(bootstrap_servers=self.args.bootstrap_servers,
                                 batch_size=self.args.batch_size,
                                 linger_ms=self.args.linger_ms,
                                 compression_type=compression_type,
                                 buffer_memory=self.args.buffer_memory,
                                 acks=self.args.acks)

        start = time.time()
        images = os.listdir(self.args.data_path)
        images = [x for x in images if 'img' in x]
        while time.time() - start < self.args.execution_time:
            rand_img = random.sample(images, 1)[0]  # select an image from the testing dataset randomly
            img_path = self.args.data_path + "/" + rand_img
            send_data = normal(img_path)
            encoded_data = pickle.dumps(send_data)
            producer.send(topic=self.args.topic, value=encoded_data)
            if self.args.sync:
                producer.flush()

            if not self.last_timestamp or (time.time() - self.last_timestamp > INTERVAL):
                self.capture_metrics(producer.metrics())
            if self.args.sleep > 0:
                time.sleep(self.args.sleep/1000)
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
    parser.add_argument('--sleep', type=float, default=0, help='sleep in milliseconds between sends')
    parser.add_argument('--execution_time', type=int, default=120)
    parser.add_argument('--sync', action='store_true', default=False, help='run a synchronous producer')
    parser.add_argument('--acks', type=int, default=1, choices=[0, 1], help='acknowledgement from broker')

    parser.add_argument('--batch_size', type=int, default=16384)
    parser.add_argument('--linger_ms', type=int, default=0)
    parser.add_argument('--compression_type', type=str, default='none', choices=['none', 'gzip', 'snappy', 'lz4'])
    parser.add_argument('--buffer_memory', type=int, default=33554432)
    args = parser.parse_args()

    pub = MyProducer(args)
    pub.produce_msg()

    latency = []
    throughput = []
    e2e_latency = []

    latency.append(pub.get_latency())
    throughput.append(pub.get_throughput())
    latency = np.array(latency).mean(axis=0).reshape(1, -1)
    throughput = np.array(throughput).mean(axis=0).reshape(1, -1)
    np.savetxt('latency.log', latency, fmt='%.3f', delimiter=',')
    np.savetxt('throughput.log', throughput, fmt='%.3f', delimiter=',')

