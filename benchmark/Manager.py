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
from sklearn.utils import shuffle
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
    os.system('kubectl delete pods --force --all')
    os.system('kubectl delete services --force --all')


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
        self.k8s_api.label_node(node=self.this_group['nodes']['client']['host'], label={'dmsconfig_client': client_label})
        self.this_group['nodes']['client']['label'] = client_label

        server_label = 'G%d-Server' % self.gid  # node for kafka server
        self.k8s_api.label_node(node=self.this_group['nodes']['server']['host'], label={'dmsconfig_server': server_label})
        self.this_group['nodes']['server']['label'] = server_label

    def deploy_services(self):
        pod_name = 'g-%d-zk' % self.gid
        cluster_ip = self.k8s_api.create_svc(svc_name=pod_name, svc_ports=[2181])
        self.this_group['services']['zookeeper'].append(cluster_ip)
        for j in range(self.num_brokers):
            pod_name = 'g-%d-b-%d' % (self.gid, j)
            cluster_ip = self.k8s_api.create_svc(svc_name=pod_name, svc_ports=[9092])
            self.this_group['services']['kafka'].append(cluster_ip)

    def deploy_kafka(self):
        pod_name = 'g-%d-zk' % self.gid
        self.k8s_api.create_pod(name=pod_name, image=DMSCONFIG_KAFKA_IMAGE, resource_limit=ZOOKEEPER_REQ_RES,
                                command=None, node_label=dict(dmsconfig_server=self.this_group['nodes']['server']['label']))
        self.this_group['pods']['zookeeper'].append(pod_name)
        
        for j in range(self.num_brokers):
            pod_name = 'g-%d-b-%d' % (self.gid, j)
            envs = [{'name': 'JMX_PORT', 'value': '9999'}]
            self.k8s_api.create_pod(pod_name, DMSCONFIG_KAFKA_IMAGE, BROKER_REQ_RES, None,
                                    node_label=dict(dmsconfig_server=self.this_group['nodes']['server']['label']), envs=envs)
            self.this_group['pods']['broker'].append(pod_name)
    
    def delete_kafka(self):
        self.k8s_api.delete_pod(self.this_group['pods']['zookeeper'][0])
        for pod in self.this_group['pods']['broker']:
            self.k8s_api.delete_pod(pod)
        self.this_group['pods']['broker'] = []
        self.this_group['pods']['zookeeper'] = []

    def deploy_clients(self):
         # create pub pods
        for j in range(self.num_pubs):
            pod_name = 'g-%d-p-%d' % (self.gid, j)
            # pub pods mount volume: /home/ubuntu/DMSConfig/benchmark/data -> /app/data
            self.k8s_api.create_pod(pod_name, DMSCONFIG_PRO_IMAGE, CLIENT_REQ_RES, None,
                                    node_label=dict(dmsconfig_client=self.this_group['nodes']['client']['label']),
                                    volume={'host_path': '/home/ubuntu/DMSConfig/benchmark/data',
                                            'container_path': '/app/data'})
            self.this_group['pods']['pub'].append(pod_name)

        # create sub pods
        for j in range(self.num_subs):
            pod_name = 'g-%s-s-%d' % (self.gid, j)
            self.k8s_api.create_pod(pod_name, DMSCONFIG_CON_IMAGE, CLIENT_REQ_RES, None,
                                    node_label=dict(dmsconfig_client=self.this_group['nodes']['client']['label']))
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
                "delete.topic.enable": 'true',
                "log.dirs": "/kafka/logs"
            })

            with open('runtime/%s-server.properties' % pod, 'w') as f:
                for key in self.broker_knobs:
                    f.write('%s = %s\n' % (key, self.broker_knobs[key]))

            subprocess.check_output(
                "kubectl cp runtime/%s-server.properties %s:/kafka/config/server.properties" % (pod, pod), shell=True)

    def start_zk(self):
        cmd = './bin/zookeeper-server-start.sh -daemon config/zookeeper.properties'
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd)
        while True:
            if len(self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], 'pgrep java')) > 0:
                break

    def start_kafka(self):
        for broker in self.this_group['pods']['broker']:
            cmd = './bin/kafka-server-start.sh config/server.properties'
            self.k8s_api.exec_pod(broker, cmd, detach=True)
        # wait until all brokers are avaliable in zookeeper
        start = time.time()
        while time.time() - start < 60:
            cmd = './bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids | tail -n 1'
            try:
                avail_brokers = self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd).replace('[', '').replace(']', '').split(',')
                avail_brokers = [int(x) for x in avail_brokers]
                if len(self.this_group['pods']['broker']) == len(avail_brokers):
                    break
            except Exception as ex:
                pass
                # self.logger.warning(str(ex))

    def create_topic(self, topic):
        cmd = ['./bin/kafka-topics.sh --zookeeper localhost:2181',
                '--create', '--topic', topic,
                '--replication-factor %d' % (len(self.this_group['pods']['broker']))]
        for key in self.topic_knobs:
            cmd.append('--%s %s' % (key, str(self.topic_knobs[key])))
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], ' '.join(cmd))

    def start_exp(self, topic, execution_time, payload):
        brokers_str = ','.join(['%s:9092' % svc for svc in self.this_group['services']['kafka']])
        for pod in self.this_group['pods']['pub']:
            cmd = ['python3', 'producer.py',
                   '--topic', topic,
                   '--payload_file', payload,
                   '--bootstrap_servers', brokers_str,
                   '--execution_time', str(execution_time)]
            for knob in self.pub_knobs:
                cmd.append('--%s %s' % (knob, str(self.pub_knobs[knob])))

            # self.logger.info(msg='Producer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd, detach=True)

        for pod in self.this_group['pods']['sub']:
            cmd = ['spark-submit',
                   '--packages', 'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7',
                   'consumer.py',
                   '--bootstrap_servers', brokers_str,
                   '--topic', topic,
                   '--execution_time', str(execution_time)]
            for knob in self.sub_knobs:
                cmd.append('--%s %s' % (knob, str(self.sub_knobs[knob])))

            # self.logger.info(msg='Consumer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd, detach=True)
        
        time.sleep(execution_time)
        for pod in self.this_group['pods']['sub']:
            tmp = self.k8s_api.exec_pod(pod, 'tail -n 1 latency.log')
            i = 0
            while i < 5:
                last_line = self.k8s_api.exec_pod(pod, 'tail -n 1 latency.log')
                if tmp == last_line:
                    i += 1
                else:
                    i -= 1
                tmp = last_line
            os.system('kubectl cp %s:latency.log %s-latency.log' % (pod, pod))

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
                    outputs = ['min','max','median','mean','std','25th','50th','75th','90th','95th','99th']
                    for j, metric in enumerate(outputs):
                        selectd_metrics = kafka_metrics[j]
                        selectd_metrics = selectd_metrics[state_meta]
                        with open('metrics/states_%s.csv' % metric, 'a') as f:
                            f.write('%d,%s\n' % (config_index, ','.join([str(x) for x in selectd_metrics.iloc[0].to_list()])))
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
        vals = [config_index, min_val, max_val, median_val, mean_val, std_val]
        vals.extend(p_vals)
        vals = ','.join([str(x) for x in vals])
        with open('metrics/latency.csv', 'a') as f:
            f.write(vals+'\n')

    def run(self, schedule, topic, execution_time, payload):
        self.config_cluster()
        self.deploy_services()
        self.deploy_clients()
        self.k8s_api.wait_pods_ready(self.this_group['pods']['pub'] + self.this_group['pods']['sub'])
        self.k8s_api.limit_bw(self.this_group['pods']['pub'] + self.this_group['pods']['sub'], 1000)
        columns = list(schedule.columns[2:])

        for _, exp in schedule.iterrows():
            try:
                for col in columns:
                    knob_entity = col.split('->')[0]
                    knob_name = col.split('->')[1]
                    if knob_entity == 'producer':
                        self.pub_knobs.update({knob_name: exp[col]})
                    elif knob_entity == 'broker':
                        self.broker_knobs.update({knob_name: exp[col]})
                    elif knob_entity == 'consumer':
                        self.sub_knobs.update({knob_name: exp[col]})
                    elif knob_entity == 'topic':
                        self.topic_knobs.update({knob_name: exp[col]})

                self.logger.info(msg='Config index: %d' % exp['index'])
                self.deploy_kafka()
                self.k8s_api.wait_pods_ready(self.this_group['pods']['broker'] + self.this_group['pods']['zookeeper'])
                self.k8s_api.limit_bw(self.this_group['pods']['broker'] + self.this_group['pods']['zookeeper'], 1000)
                self.config_kafka()
                self.start_zk()
                self.start_kafka()
                self.create_topic(topic)
                self.start_exp(topic, execution_time, payload)
                # Note: we have only one broker in this project
                self.process_state_metrics(self.this_group['pods']['broker'][0], execution_time, exp['index'])
                self.process_latency_metrics(self.this_group['pods']['sub'][0], exp['index'])
            except Exception:
                self.logger.error('Config %d failed' % exp['index'])
                for pod in self.this_group['pods']['pub']:
                    self.k8s_api.exec_pod(pod, 'pgrep python3 | xargs kill', detach=True)
                for pod in self.this_group['pods']['sub']:
                    self.k8s_api.exec_pod(pod, 'pgrep java | xargs kill', detach=True)
            finally:
                self.delete_kafka()

def sample_configs(budget):
    """
    Generate samples using Latin Hypercube Sampling
    :param budget: the number of samples
    :return:
    """
    meta_info = pd.read_csv('meta/knob_meta.csv')
    knobs = pd.DataFrame(lhs(meta_info.shape[0], samples=budget, criterion="maximin"))
    knobs = knobs.apply(
        lambda raw_knobs: (np.floor(raw_knobs*(meta_info['max']-meta_info['min'])+meta_info['min']) * meta_info['unit']).astype(int), axis=1)
    knobs.columns = meta_info['knob']
    knobs.reset_index(inplace=True)
    msg_cps = ['none', 'gzip', 'snappy']
    knobs['producer->compression_type'] = knobs['producer->compression_type'].apply(lambda x: msg_cps[x])
    knobs.to_csv(path_or_buf="schedule.csv", index=None)
    return knobs


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Benchmark")
    parser.add_argument("--new", action="store_true", default=False, help="build model with new test plan")
    parser.add_argument("--from_index", type=int, default=0, help="start running test from the given index")
    parser.add_argument('--to_index', type=int, default=-1, help='stop running test at the given index')
    parser.add_argument("--budget", type=int, default=1000, help='LHS budget')
    parser.add_argument('--num_pubs', type=int, default=1)
    parser.add_argument('--num_brokers', type=int, default=1)
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
    exp_sch = shuffle(exp_sch)
    metrics = ['index', 'min','max','median','mean','std','25th','50th','75th','90th','95th','99th']
    for m in metrics:
        with open('metrics/states_%s.csv' % m, 'w') as f:
            knobs = pd.read_csv('meta/state_meta.csv')['name'].to_list()
            f.write('index,' + ','.join(knobs) + '\n')
    
    with open('metrics/latency.csv', 'w') as f:
        f.write(','.join(metrics) + '\n')

    '''
    k = 0
    cluster = K8sAPI.K8sCluster()
    worker_nodes = cluster.list_pods_name()[1:]
    group_size = int(exp_sch.shape[0] / args.parallel)
    '''
    with open('groups.json', 'r') as f:
        worker_nodes = json.load(f)
    group_size = int(exp_sch.shape[0] / len(worker_nodes))
    # distribute experiments to multiple groups
    for gid in range(len(worker_nodes)):
        subset = exp_sch.iloc[range(gid * group_size, min((gid + 1) * group_size, exp_sch.shape[0]))].reset_index()
        gman = GroupManager(gid, worker_nodes[gid]['client'], worker_nodes[gid]['kafka'], args.num_pubs,
                            args.num_brokers, args.num_subs)
        exp_thr = threading.Thread(target=gman.run, args=(subset, args.topic, args.execution_time, args.payload,))
        exp_thr.start()
