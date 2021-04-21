import argparse
import os
import pandas as pd
import numpy as np
import subprocess
import threading
import time
import json
import traceback
from influxdb import InfluxDBClient
import dataformatter as dftr
import K8sAPI
from pyDOE import lhs
import utils

DMSCONFIG_PRO_IMAGE = "zhuangweikang/dmsconfig_producer:latest"
DMSCONFIG_CON_IMAGE = "zhuangweikang/dmsconfig_consumer:latest"
DMSCONFIG_KAFKA_IMAGE = "zhuangweikang/dmsconfig_kafka:latest"

BROKER_REQ_RES = {'cpu': '4', 'memory': '8Gi'}
CLIENT_REQ_RES = {'cpu': '2', 'memory': '2Gi'}
ZOOKEEPER_REQ_RES = {'cpu': '2', 'memory': '4Gi'}

NUM_PUB = 2
NUM_BROKER = 3
NUM_SUB = 1


def find_influxdb(influxdb):
    cnr_pid = subprocess.check_output("sudo docker inspect --format '{{ .State.Pid }}' %s" % influxdb,
                                      shell=True).decode().strip()
    cnr_ip = subprocess.check_output(
            "sudo nsenter -t %s -n ip addr | grep inet | awk '{print $2}' | tail -n 1" % cnr_pid,
            shell=True).decode().strip().split('/')[0]
    return cnr_ip


def clean():
    os.system('kubectl delete pods --all')
    os.system('kubectl delete services --all')


class GroupManager:
    def __init__(self, gid, client_node, server_node, num_pubs=1, num_brokers=1, num_subs=1):
        self.gid = gid
        self.k8s_api = K8sAPI.K8sCluster()
        self.logger = utils.get_logger('Group%d' % gid)

        # the number of entities in each group
        self.num_pubs = num_pubs
        self.num_brokers = num_brokers
        self.num_subs = num_subs

        # entity knobs
        self.broker_knobs = {}
        self.pub_knobs = {}
        self.sub_knobs = {}
        self.topic_knobs = {}

        self.this_group = {
            'nodes': {'client': {'host': client_node, 'label': None}, 'server': {'host': server_node, 'label': None}},
            'pods': {'pub': [], 'sub': [], 'broker': [], 'zookeeper': []},
            'services': {'kafka': [], 'zookeeper': []}
        }

    def config_cluster(self):
        client_label = 'G%d-Client' % self.gid  # node for kafka client
        self.k8s_api.label_node(node=self.this_group['nodes']['client']['host'], label_val=client_label)
        self.this_group['nodes']['client']['label'] = client_label

        server_label = 'G%d-Server' % self.gid  # node for kafka server
        self.k8s_api.label_node(node=self.this_group['nodes']['server']['host'], label_val=server_label)
        self.this_group['nodes']['server']['label'] = server_label

    def deploy_kafka(self):
        # create zookeeper pod
        pod_name = 'g-%d-zk' % self.gid
        cluster_ip = self.k8s_api.create_svc(svc_name=pod_name, svc_ports=[2181])
        self.k8s_api.create_pod(name=pod_name, image=DMSCONFIG_KAFKA_IMAGE, resource_limit=ZOOKEEPER_REQ_RES,
                                command=None, node_label=self.this_group['nodes']['server']['label'])
        self.this_group['pods']['zookeeper'].append(pod_name)
        self.this_group['services']['zookeeper'].append(cluster_ip)

        # create a K8s Service for each Kafka broker
        for j in range(self.num_brokers):
            pod_name = 'g-%d-b-%d' % (self.gid, j)
            cluster_ip = self.k8s_api.create_svc(svc_name=pod_name, svc_ports=[9092])
            self.k8s_api.create_pod(pod_name, DMSCONFIG_KAFKA_IMAGE, BROKER_REQ_RES, None,
                                    node_label=self.this_group['nodes']['server']['label'])
            self.this_group['pods']['broker'].append(pod_name)
            self.this_group['services']['kafka'].append(cluster_ip)

        # create pub pods
        for j in range(self.num_pubs):
            pod_name = 'g-%d-p-%d' % (self.gid, j)
            # pub pods mount volume: /home/ubuntu/DMSConfig/benchmark/data -> /app/data
            self.k8s_api.create_pod(pod_name, DMSCONFIG_PRO_IMAGE, CLIENT_REQ_RES, None,
                                    node_label=self.this_group['nodes']['client']['label'],
                                    volume={'host_path': '/home/ubuntu/DMSConfig/benchmark/data',
                                            'container_path': '/app/data'})
            self.this_group['pods']['pub'].append(pod_name)

        # create sub pods
        for j in range(self.num_subs):
            pod_name = 'g-%s-s-%d' % (self.gid, j)
            self.k8s_api.create_pod(pod_name, DMSCONFIG_CON_IMAGE, CLIENT_REQ_RES, None,
                                    node_label=self.this_group['nodes']['client']['label'])
            self.this_group['pods']['sub'].append(pod_name)

    def config_kafka(self):
        for j, pod in enumerate(self.this_group['pods']['broker']):
            # add static configs
            self.broker_knobs.update({
                "broker.id": j,
                "offsets.topic.replication.factor": len(self.this_group['pods']['broker']),
                "listeners": "PLAINTEXT://0.0.0.0:9092",
                "advertised.listeners": "PLAINTEXT://%s:9092" % self.this_group['services']['kafka'][j],
                "zookeeper.connect": "%s:2181" % self.this_group['services']['zookeeper'][0],
                "log.dirs": "/kafka/logs"
            })

            # populate Broker config file
            with open('runtime/%s-server.properties' % pod, 'w') as f:
                for key in self.broker_knobs:
                    f.write('%s = %s\n' % (key, self.broker_knobs[key]))

            # copy broker config file into pod
            subprocess.check_output(
                "kubectl cp runtime/%s-server.properties %s:/kafka/config/server.properties" % (pod, pod), shell=True)

    def start_kafka(self):
        # start zookeeper
        cmd = 'bin/zookeeper-server-start.sh -daemon config/zookeeper.properties'
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd)
        while True:
            try:
                self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], 'pgrep java', detach=False)
                break
            except:
                pass
        for broker in self.this_group['pods']['broker']:
            cmd = 'bin/kafka-server-start.sh -daemon config/server.properties'
            self.k8s_api.exec_pod(broker, cmd)
            while True:
                try:
                    self.k8s_api.exec_pod(broker, 'pgrep java', detach=False)
                    break
                except:
                    pass

    def start_exp(self, topic, execution_time, payload):
        brokers_str = ','.join(['%s:9092' % svc for svc in self.this_group['services']['kafka']])
        for pod in self.this_group['pods']['pub']:
            cmd = ['python3', 'producer.py',
                   '--topic', topic,
                   '--payload_file', payload,
                   '--bootstrap_servers', brokers_str,
                   '--execution_time', str(execution_time)]
            for knob in self.pub_knobs:
                cmd.append('--%s' % knob)
                cmd.append(self.pub_knobs[knob])

            self.logger.info(msg='Producer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd)

        for pod in self.this_group['pods']['sub']:
            cmd = ['spark-submit',
                   '--packages', 'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7',
                   'consumer.py',
                   '--bootstrap_servers', brokers_str,
                   '--topic', topic,
                   '--execution_time', str(execution_time)]
            for knob in self.sub_knobs:
                cmd.append('--%s' % knob)
                cmd.append(self.sub_knobs[knob])

            self.logger.info(msg='Consumer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd)
        
        time.sleep(execution_time)
        for pod in self.this_group['pods']['sub']:
            while True:
                try:
                    self.k8s_api.exec_pod(pod, 'pgrep java', detach=False)
                except Exception:  # return code is non zero if the no java process is running
                    # copy latency results to master
                    os.system('kubectl cp %s:latency.log %s-latency.log' % (pod, pod))
                    break

    def stop_exp(self):
        stop_zk = 'bin/zookeeper-server-stop.sh'
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], stop_zk)
        for broker in self.this_group['pods']['broker']:
            stop_kafka = 'bin/kafka-server-stop.sh'
            self.k8s_api.exec_pod(broker, stop_kafka)

    def process_state_metrics(self, pod, execution_time, config_index):
        with open('fecbench.json') as f:
            influxdb_settings = json.load(f)['influxdb']
        
        client = InfluxDBClient(host=find_influxdb(influxdb_settings['host']), port=8086,
                                username=influxdb_settings['user'],
                                password=influxdb_settings['pwd'], database=influxdb_settings['database'])
        kafka_metrics_query_str = 'SELECT * FROM "docker_kafka_metrics" WHERE time > now()-%ds AND time < now() GROUP BY "instance"' % execution_time
        kafka_metrics = client.query(query=kafka_metrics_query_str, database=influxdb_settings['database'])
        kafka_raw_metrics = kafka_metrics.raw['series']
        for i in range(len(kafka_raw_metrics)):
            instance = kafka_raw_metrics[i]['tags']['instance']
            try:
                if pod in instance:
                    kafka_metrics = list(kafka_metrics.get_points(tags={"instance": instance}))
                    kafka_metrics = pd.DataFrame(kafka_metrics)
                    kafka_metrics = dftr.format_kafka_metrics(kafka_metrics)
                    state_meta = pd.read_csv('meta/state_meta.csv')['name'].to_list()
                    kafka_metrics = kafka_metrics[state_meta]
                    with open('metrics/states.csv', 'a') as f:
                        f.write('%d,%s\n' % (config_index, ','.join([str(x) for x in kafka_metrics.iloc[0].to_list()])))
                    break
            except Exception:
                traceback.print_exc()
    
    def process_latency_metrics(self, pod, config_index):
        data = pd.read_csv('%s-latency.log' % pod).to_numpy().squeeze()
        min_val = np.min(data)
        max_val = np.max(data)
        median_val = np.median(data)
        mean_val = np.mean(data)
        std_val = np.std(data)
        p_vals = np.percentile(data, [25, 50, 75, 90, 95, 99])

        vals = [min_val, max_val, median_val, mean_val, std_val]
        vals.extend(p_vals)
        vals = pd.DataFrame(vals).round(2).T
        
        with open('metrics/latency.csv', 'a') as f:
            f.write('%d,%s\n' % (config_index, ','.join([str(x) for x in vals.iloc[0].to_list()])))

    def run(self, schedule, topic, execution_time, payload):
        try:
            self.config_cluster()
            self.deploy_kafka()
            all_pods = []
            for e in self.this_group['pods']:
                all_pods.extend(self.this_group['pods'][e])
            self.k8s_api.wait_pods_ready(all_pods)

            columns = list(schedule.columns[2:])
            for _, exp in schedule.iterrows():
                for col in columns:
                    knob_entity = col.split('->')[0]
                    knob_name = col.split('->')[1]
                    if knob_entity == 'pub':
                        self.pub_knobs.update({knob_name: exp[col]})
                    elif knob_entity == 'broker':
                        self.broker_knobs.update({knob_name: exp[col]})
                    elif knob_entity == 'sub':
                        self.sub_knobs.update({knob_name: exp[col]})
                    elif knob_entity == 'topic':
                        self.topic_knobs.update({knob_name: exp[col]})

                self.logger.info(msg='Config index: %d' % exp['index'])
                self.config_kafka()
                self.start_kafka()
                self.start_exp(topic, execution_time, payload)
                self.stop_exp()
                self.logger.info(msg='Done')
                # TODO: do we need metrics of followers?
                self.process_state_metrics(self.this_group['pods']['broker'][0], execution_time, exp['index'])
                self.process_latency_metrics(self.this_group['pods']['sub'][0], exp['index'])
                self.logger.info(msg='-------------------------')
        except Exception:
            traceback.print_exc()
        finally:
            clean()


def sample_configs(budget):
    """
    Generate samples using Latin Hypercube Sampling
    :param budget: the number of samples
    :return:
    """
    meta_info = pd.read_csv('meta/knob_meta.csv')
    knobs = pd.DataFrame(lhs(meta_info.shape[0], samples=budget, criterion="maximin"))
    knobs = knobs.apply(
        lambda raw_knobs: ((raw_knobs*(meta_info['max']-meta_info['min'])+meta_info['min']) * meta_info['unit']).round(2).astype(
            int), axis=1)
    knobs.columns = meta_info['knob']
    knobs.reset_index(inplace=True)
    knobs.to_csv(path_or_buf="schedule.csv", index=None)
    return knobs


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Benchmark")
    parser.add_argument('--groups', type=int, default=3, help='The number of groups')
    parser.add_argument("--new", action="store_true", default=False, help="build model with new test plan")
    parser.add_argument("--from_index", type=int, default=0, help="start running test from the given index")
    parser.add_argument('--to_index', type=int, default=-1, help='stop running test at the given index')
    parser.add_argument("--budget", type=int, default=1000, help='LHS budget')
    parser.add_argument('--num_pubs', type=int, default=1)
    parser.add_argument('--num_brokers', type=int, default=3)
    parser.add_argument('--num_subs', type=int, default=1)
    parser.add_argument('--topic', type=str, default='nyc_taxi')
    parser.add_argument('--execution_time', type=int, default=120)
    parser.add_argument('--payload', type=str, default='data/trip_data_1.csv', help='name of the payload file')
    args = parser.parse_args()

    if args.new or not os.path.exists("schedule.csv"):
        exp_sch = sample_configs(args.budget)
    else:
        exp_sch = pd.read_csv('schedule.csv')
    exp_sch = exp_sch.iloc[args.from_index: args.to_index, :]

    with open('metrics/states.csv', 'w') as f:
        knobs = pd.read_csv('meta/state_meta.csv')['name'].to_list()
        f.write('index,' + ','.join(knobs) + '\n')
    with open('metrics/latency.csv', 'w') as f:
        latency_metrics = ['index', 'min','max','median','mean','std','25th','50th','75th','90th','95th','99th']
        f.write(','.join(latency_metrics) + '\n')

    '''
    k = 0
    cluster = K8sAPI.K8sCluster()
    worker_nodes = cluster.list_pods_name()[1:]
    group_size = int(exp_sch.shape[0] / args.parallel)
    '''
    with open('node_dist.json', 'r') as f:
        worker_nodes = json.load(f)
    group_size = int(exp_sch.shape[0] / 3)
    # distribute experiments to multiple groups
    for gid in range(args.groups):
        subset = exp_sch.iloc[range(gid * group_size, min((gid + 1) * group_size, exp_sch.shape[0]))].reset_index()
        gman = GroupManager(gid, worker_nodes[gid]['client'], worker_nodes[gid]['kafka'], args.num_pubs,
                            args.num_brokers, args.num_subs)
        exp_thr = threading.Thread(target=gman.run, args=(subset, args.topic, args.execution_time, args.payload,))
        exp_thr.start()
