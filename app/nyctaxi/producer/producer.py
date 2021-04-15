import argparse
import time
from kafka import KafkaProducer

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', type=str, required=True)
    parser.add_argument('--payload_file', type=str, required=True, default='data/data.csv')
    parser.add_argument('--topic', type=str, default='nyc_taxi')
    parser.add_argument('--sleep', type=int, default=0, help='sleep in seconds between sends')

    parser.add_argument('--batch_size', type=int, default=16384)
    parser.add_argument('--linger_ms', type=int, default=0)
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers,
                             batch_size=args.batch_size,
                             linger_ms=args.linger_ms)
    with open(args.payload_file) as f:
        while True:
            line = f.readline()
            if not line:
                break
            producer.send(topic=args.topic, value=line.encode())
            time.sleep(args.sleep)

    producer.close()
