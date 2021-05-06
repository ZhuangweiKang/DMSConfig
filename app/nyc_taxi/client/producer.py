import argparse
import time
from kafka import KafkaProducer

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap_servers', type=str, required=True)
    parser.add_argument('--payload_file', type=str, required=True)
    parser.add_argument('--topic', type=str, default='nyc_taxi')
    parser.add_argument('--sleep', type=int, default=0, help='sleep in seconds between sends')
    parser.add_argument('--execution_time', type=int, default=120)

    parser.add_argument('--batch_size', type=int, default=16384)
    parser.add_argument('--linger_ms', type=int, default=0)
    parser.add_argument('--compression_type', type=str, default='none', choices=['none', 'gzip', 'snappy', 'lz4'])
    parser.add_argument('--buffer_memory', type=int, default=33554432)
    args = parser.parse_args()

    compression_type = None
    if args.compression_type != 'none':
        compression_type = args.compression_type

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers,
                             batch_size=args.batch_size,
                             linger_ms=args.linger_ms,
                             compression_type=compression_type,
                             buffer_memory=args.buffer_memory)
    
    start = time.time()
    with open(args.payload_file) as f:
        f.readline()  # skip the header
        while time.time() - start < args.execution_time:
            line = f.readline().strip()
            line += ',%s' % str(time.time())
            if not line:
                break
            producer.send(topic=args.topic, value=line.encode())
            if args.sleep > 0:
                time.sleep(args.sleep)

    producer.close()
