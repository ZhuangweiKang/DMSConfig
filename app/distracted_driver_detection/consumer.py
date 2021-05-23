import sys
import argparse
import time
from kafka import KafkaConsumer
import utils
import pickle
from predict import *

INTERVAL = 1


class MyConsumer(object):
    def __init__(self, args):
        self.args = args
        self.latency_samples = []
        self.e2e_latency_samples = []
        self.throughput_samples = []
        self.last_timestamp = None

        self.model = load_model()

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
        consumer = KafkaConsumer(self.args.topic, auto_offset_reset='latest', group_id='group-1',
                                 consumer_timeout_ms=1000 * self.args.execution_time,
                                 bootstrap_servers=[self.args.bootstrap_servers],
                                 fetch_max_wait_ms=self.args.fetch_wait_max_ms,
                                 max_partition_fetch_bytes=self.args.max_partition_fetch_bytes)

        os.system('rm *.log')

        start = time.time()
        while time.time() - start < self.args.execution_time:
            message_batch = consumer.poll()
            for partition_batch in message_batch.values():
                for message in partition_batch:
                    if not self.last_timestamp or (time.time() - self.last_timestamp > INTERVAL):
                        self.capture_metrics(consumer.metrics(), message.timestamp)
                    img = pickle.loads(message.value)
                    predict(self.model, img_matrix=img)
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
    parser.add_argument('--topic', type=str, default='distracted_driver_detection')
    parser.add_argument('--execution_time', type=int, default=120)

    parser.add_argument('--fetch_wait_max_ms', type=int, default=500)
    parser.add_argument('--max_partition_fetch_bytes', type=int, default=1048576)
    args = parser.parse_args()

    sub = MyConsumer(args)
    sub.consume_msg()

    latency = []
    throughput = []
    e2e_latency = []

    latency.append(sub.get_latency())
    throughput.append(sub.get_throughput())
    e2e_latency.append(sub.get_e2e_latency())

    latency = np.array(latency).mean(axis=0).reshape(1, -1)
    throughput = np.array(throughput).mean(axis=0).reshape(1, -1)
    e2e_latency = np.array(e2e_latency).mean(axis=0).reshape(1, -1)

    np.savetxt('latency.log', latency, fmt='%.3f', delimiter=',')
    np.savetxt('throughput.log', throughput, fmt='%.3f', delimiter=',')
    np.savetxt('e2e_latency.log', e2e_latency, fmt='%.3f', delimiter=',')