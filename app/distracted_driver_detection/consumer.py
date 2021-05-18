import argparse
import time
from kafka import KafkaConsumer
import utils
from threading import Thread
import pickle
from predict import *

INTERVAL = 1

activity_map = {'c0': 'Safe driving',
                'c1': 'Texting - right',
                'c2': 'Talking on the phone - right',
                'c3': 'Texting - left',
                'c4': 'Talking on the phone - left',
                'c5': 'Operating the radio',
                'c6': 'Drinking',
                'c7': 'Reaching behind',
                'c8': 'Hair and makeup',
                'c9': 'Talking to passenger'}


class MyConsumer(object):
    def __init__(self, args, sub_id):
        self.args = args
        self.sub_id = sub_id
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
                                 fetch_min_bytes=self.args.fetch_min_bytes,
                                 fetch_max_wait_ms=self.args.fetch_wait_max_ms)

        os.system('rm *.log')

        start = time.time()
        while time.time() - start < self.args.execution_time:
            message_batch = consumer.poll()
            for partition_batch in message_batch.values():
                for message in partition_batch:
                    if not self.last_timestamp or (time.time() - self.last_timestamp > INTERVAL):
                        self.capture_metrics(consumer.metrics(), message.timestamp)
                    img = pickle.loads(message.value)

                    # predict function returns perception results
                    y_prediction = predict(self.model, img_matrix=img)
                    print('Predicted: {}'.format('c{}'.format(np.argmax(y_prediction)) + ' - ' + activity_map.get('c{}'.format(np.argmax(y_prediction)))))
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
    parser.add_argument('--num_subs', type=int, default=1, help='the amount of subscriber threads')
    parser.add_argument('--print', action='store_true', default=False, help='print distracted driver detection results')

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