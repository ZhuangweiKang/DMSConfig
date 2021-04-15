#!venv/bin/python3

import pandas as pd
import json
import zmq
import subprocess
from influxdb import InfluxDBClient
import dataformatter as dftr

BROKER_CORES = 4


def find_influxdb(influxdb):
    cnr_pid = subprocess.check_output("sudo docker inspect --format '{{ .State.Pid }}' %s" % influxdb,
                                      shell=True).decode().strip()
    cnr_ip = \
    subprocess.check_output("sudo nsenter -t %s -n ip addr | grep inet | awk '{print $2}' | tail -n 1" % cnr_pid,
                            shell=True).decode().strip().split('/')[0]
    return cnr_ip


def run(gid, exp_id):
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.bind('tcp://*:5555')

    with open('fecbench.json') as f:
        influxdb_settings = json.load(f)['influxdb']
    client = InfluxDBClient(host=find_influxdb(influxdb_settings['host']), port=8086, username=influxdb_settings['user'],
                            password=influxdb_settings['pwd'], database=influxdb_settings['database'])
    test_plan = pd.read_csv('schedule.csv')

    while True:
        notify = socket.recv_string()
        notify = json.loads(notify)

        kafka_metrics_query_str = 'SELECT * FROM "collectd_db"."autogen"."docker_kafka_metrics" WHERE time > now()-%ds AND time < now() GROUP BY "instance"' % (notify['duration'])
        kafka_metrics = client.query(query=kafka_metrics_query_str, database=influxdb_settings['database'])
        kafka_raw_metrics = kafka_metrics.raw['series']

        docker_metrics_query_str = 'SELECT * FROM "collectd_db"."autogen"."container_metrics" WHERE time > now()-%ds AND time < now() GROUP BY "instance" ' % (notify['duration'])
        docker_metrics = client.query(query=docker_metrics_query_str, database=influxdb_settings['database'])
        docker_raw_metrics = docker_metrics.raw['series']

        for i in range(len(kafka_raw_metrics)):
            instance = kafka_raw_metrics[i]['tags']['instance']

            try:
                kafka_metrics = list(kafka_metrics.get_points(tags={"instance": instance}))
                kafka_metrics = pd.DataFrame(kafka_metrics)
                kafka_metrics = dftr.format_kafka_metrics(kafka_metrics)

                docker_metrics = list(docker_metrics.get_points(tags={"instance": instance}))
                docker_metrics = pd.DataFrame(docker_metrics)
                docker_metrics = dftr.format_container_metrics(docker_metrics, BROKER_CORES)

                kafka_metrics.to_csv('data/group_%d_exp_%d_kafka.csv' % (gid, exp_id))
                docker_metrics.to_csv('data/group_%d_exp_%d_container.csv' % (gid, exp_id))

            except Exception as ex:
                print(ex)
