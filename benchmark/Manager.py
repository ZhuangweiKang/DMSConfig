import argparse
import os
import pandas as pd
import numpy as np
import subprocess
import K8sAPI
from pyDOE import lhs
import utils
import json
import threading
import traceback
import time
from jmxquery import JMXConnection, JMXQuery

MBEANS_FILE = 'docker_kafka.json'
INTERVAL = 2

DMSCONFIG_CLIENT_IMAGE = "zhuangweikang/dmsconfig_3d:latest"
DMSCONFIG_KAFKA_IMAGE = "zhuangweikang/dmsconfig_kafka:latest"

BROKER_REQ_RES = {'cpu': '2000m', 'memory': '4Gi'}
PUB_REQ_RES = {'cpu': '500m', 'memory': '1Gi'}
SUB_REQ_RES = {'cpu': '1000m', 'memory': '2Gi'}
ZOOKEEPER_REQ_RES = {'cpu': '1000m', 'memory': '1Gi'}


def clean():
    os.system('kubectl delete pods --force --all')
    os.system('kubectl delete services --force --all')


class JMXMonitor(threading.Thread):
    def __init__(self, address, label):
        super(JMXMonitor, self).__init__()
        self.stop = False
        svc_url = 'service:jmx:rmi:///jndi/rmi://%s/jmxrmi' % address
        self._conn = JMXConnection(svc_url)
        self.label = label
        with open(MBEANS_FILE) as f:
            mbeans = json.load(f)
        self._query_obj = []
        for mbean in mbeans:
            for val in mbean['Values']:
                self._query_obj.append(JMXQuery(mBeanName=mbean['ObjectName'],
                                                attribute=val['Attribute'],
                                                value_type=val['Type'],
                                                metric_name=val['InstancePrefix'],
                                                metric_labels={'type': val['Type']}))
        # Automatically start stats reading thread
        self.start()

    def run(self):
        failures = 0
        observations = {}
        state_cols = pd.read_csv('./meta/kafka_state_meta.csv')['name'].tolist()
        if not os.path.exists('metrics/states.csv'):
            with open('metrics/states.csv', 'w') as f:
                f.write('index,' + ','.join(state_cols) + '\n')
        while not self.stop:
            try:
                metrics = self._conn.query(self._query_obj)
                if metrics:
                    for mtr in metrics:
                        mtr_name = mtr.metric_name
                        mtr_val = mtr.value
                        mtr_type = mtr.metric_labels['type']
                        if mtr_name in ['last_gc_info', 'memory_heap_usage', 'memory_non_heap_usage']:
                            mtr_name = '%s_%s' % (mtr_name, mtr.attributeKey)
                        if not mtr_val:
                            continue
                        # print(mtr_name, mtr.attributeKey, mtr_val)
                        if mtr_name not in observations:
                            if mtr_type == 'gauge':
                                observations.update({mtr_name: [mtr_val]})
                            elif mtr_type == 'counter':
                                observations.update({mtr_name: mtr_val})
                        else:
                            if mtr_type == 'gauge':
                                observations[mtr_name].append(mtr_val)
                            elif mtr_type == 'counter':
                                observations[mtr_name] = mtr_val
            except Exception as ex:
                failures += 1
                if failures >= 5:
                    self.stop = True
            time.sleep(INTERVAL)

        results = {'index': self.label}
        for col in state_cols:
            if col in observations:
                if type(observations[col]) is list:
                    results.update({col: np.mean(observations[col])})
                else:
                    results.update({col: observations[col]})
            else:
                results.update({col: None})
        with open('metrics/states.csv', 'a') as f:
            vals = [str(x) for x in results.values()]
            f.write(','.join(vals) + '\n')

    def stop_monitor(self):
        self.stop = True


class GroupManager:
    def __init__(self, gid, pub_node, sub_node, server_node, num_pubs=1, num_brokers=1, num_subs=1, sleep=0, tc=False):
        self.gid = gid
        self.k8s_api = K8sAPI.K8sCluster()
        self.logger = utils.get_logger('Group%d' % gid)

        # the number of entities in each group
        self.num_pubs = num_pubs
        self.num_brokers = num_brokers
        self.num_subs = num_subs
        self.sleep = sleep
        self.enable_tc = tc

        # entity knobs
        self.broker_knobs = {}
        self.pub_knobs = {}
        self.sub_knobs = {}
        self.topic_knobs = {}

        self.this_group = {
            'nodes': {
                'pub': {'host': pub_node, 'label': None},
                'server': {'host': server_node, 'label': None},
                'sub': {'host': sub_node, 'label': None}
            },
            'pods': {'pub': [], 'sub': [], 'broker': [], 'zookeeper': []},
            'services': {'kafka': [], 'zookeeper': []}
        }

    def config_cluster(self):
        label_val = 'G%d' % self.gid        
        self.k8s_api.label_node(node=self.this_group['nodes']['pub']['host'],
                                label={'dmsconfig_pub_%d' % self.gid: label_val})
        self.this_group['nodes']['pub']['label'] = label_val
        
        self.k8s_api.label_node(node=self.this_group['nodes']['sub']['host'],
                                label={'dmsconfig_sub_%d' % self.gid: label_val})
        self.this_group['nodes']['sub']['label'] = label_val

        self.k8s_api.label_node(node=self.this_group['nodes']['server']['host'],
                                label={'dmsconfig_server_%d' % self.gid: label_val})
        self.this_group['nodes']['server']['label'] = label_val

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
                                command=None,
                                node_label={'dmsconfig_server_%d' % self.gid : self.this_group['nodes']['server']['label']})
        self.this_group['pods']['zookeeper'].append(pod_name)

        for j in range(self.num_brokers):
            pod_name = 'g-%d-b-%d' % (self.gid, j)
            envs = [{'name': 'JMX_PORT', 'value': '9999'}]
            self.k8s_api.create_pod(pod_name, DMSCONFIG_KAFKA_IMAGE, BROKER_REQ_RES, None,
                                    node_label={'dmsconfig_server_%d' % self.gid : self.this_group['nodes']['server']['label']},
                                    envs=envs)
            self.this_group['pods']['broker'].append(pod_name)

    def delete_kafka(self):
        self.k8s_api.delete_pod(self.this_group['pods']['zookeeper'][0])
        for pod in self.this_group['pods']['broker']:
            self.k8s_api.delete_pod(pod)
        self.this_group['pods']['broker'] = []
        self.this_group['pods']['zookeeper'] = []

    def deploy_clients(self):
        for i in range(self.num_pubs):
            # create pub pods
            pod_name = 'g-%d-p-%d' % (self.gid, i)
            # pub pods mount volume: /home/ubuntu/DMSConfig/benchmark/data -> /app/data
            self.k8s_api.create_pod(pod_name, DMSCONFIG_CLIENT_IMAGE, PUB_REQ_RES, None,
                                    node_label={'dmsconfig_pub_%d' % self.gid : self.this_group['nodes']['pub']['label']},
                                    volume={'host_path': '%s/data' % os.getcwd(),
                                            'container_path': '/app/data'})
            self.this_group['pods']['pub'].append(pod_name)

        for i in range(self.num_subs):
            # create sub pods
            pod_name = 'g-%s-s-%d' % (self.gid, i)
            self.k8s_api.create_pod(pod_name, DMSCONFIG_CLIENT_IMAGE, SUB_REQ_RES, None,
                                    node_label={'dmsconfig_sub_%d' % self.gid : self.this_group['nodes']['sub']['label']})
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
                "log.cleaner.enable": 'true',
                "log.dirs": "/kafka/logs",
                "zookeeper.connection.timeout.ms": 60000
            })

            with open('runtime/%s-server.properties' % pod, 'w') as f:
                for key in self.broker_knobs:
                    f.write('%s = %s\n' % (key, self.broker_knobs[key]))

            subprocess.check_output(
                "kubectl cp runtime/%s-server.properties %s:/kafka/config/server.properties" % (pod, pod), shell=True)

    def start_zk(self):
        cmd = './bin/zookeeper-server-start.sh config/zookeeper.properties'
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd, detach=True)

    def start_kafka(self):
        for broker in self.this_group['pods']['broker']:
            cmd = './bin/kafka-server-start.sh config/server.properties'
            self.k8s_api.exec_pod(broker, cmd, detach=True)
        # wait until all brokers are avaliable in zookeeper
        start = time.time()
        no_broker = True
        while time.time() - start < 60:
            cmd = './bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids | tail -n 1'
            try:
                avail_brokers = self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd).replace('[', '').replace(']', '').split(',')
                avail_brokers = [int(x) for x in avail_brokers]
                if len(self.this_group['pods']['broker']) == len(avail_brokers):
                    no_broker = False
                    break
            except Exception as ex:
                pass
            time.sleep(1)
        if no_broker:
            self.logger.error('Failed to start broker')

    def create_topic(self, topic):
        cmd = ['./bin/kafka-topics.sh --zookeeper localhost:2181',
               '--create', '--topic', topic,
               '--replication-factor %d' % (len(self.this_group['pods']['broker'])),
               '--partitions %d' % self.num_subs]  # set the number of partition as consumer number
        for key in self.topic_knobs:
            cmd.append('--%s %s' % (key, str(self.topic_knobs[key])))
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], ' '.join(cmd))

    def start_exp(self, config_index, topic, execution_time):
        brokers_str = ','.join(['%s:9092' % svc for svc in self.this_group['services']['kafka']])

        for pod in self.this_group['pods']['sub']:
            cmd = ['python3', 'consumer.py',
                   '--topic', topic,
                   '--bootstrap_servers', brokers_str,
                   '--execution_time', str(execution_time)]
            for knob in self.sub_knobs:
                cmd.append('--%s %s' % (knob, str(self.sub_knobs[knob])))

            # self.logger.info(msg='Consumer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd, detach=True)

        for pod in self.this_group['pods']['pub']:
            cmd = ['python3', 'producer.py',
                   '--sleep', self.sleep,
                   '--topic', topic,
                   '--bootstrap_servers', brokers_str,
                   '--execution_time', str(execution_time)]
            for knob in self.pub_knobs:
                cmd.append('--%s %s' % (knob, str(self.pub_knobs[knob])))

            # self.logger.info(msg='Producer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd, detach=True)

        # tracing broker JMX metrics
        cmd = "kubectl get pod %s -o wide | awk '{ print $6 }' | tail -n 1" % self.this_group['pods']['broker'][0]
        jmx_uri = subprocess.check_output(cmd, shell=True).decode().strip()
        jmx_monitor = JMXMonitor('%s:9999' % jmx_uri, config_index)
        jmx_monitor.join(execution_time)
        jmx_monitor.stop_monitor()

        def wait_log_ready(pod, logs):
            start = time.time()
            while True:
                try:
                    for log in logs:
                        self.k8s_api.exec_pod(pod, 'ls %s' % log)
                    break
                except Exception:
                    if time.time() - start > 60:  # wait 10 seconds at most
                        raise
                    else:
                        time.sleep(1)
    
        def write_log(fname, data):
            data = [str(x) for x in data]
            data = ','.join(data)
            with open('metrics/%s' % fname, 'a+') as f:
                f.write('%d,%s\n' % (config_index, data))

        latency_samples = []
        throughput_samples = []
        e2e_latency_samples = []
        process_metrics = lambda data_str: [float(x) for x in data_str.split(',')]
        for sub in self.this_group['pods']['sub']:
            wait_log_ready(sub, ['latency.log', 'throughput.log', 'e2e_latency.log'])
            latency_samples.append(process_metrics(self.k8s_api.exec_pod(sub, 'cat latency.log').strip('\n')))
            throughput_samples.append(process_metrics(self.k8s_api.exec_pod(sub, 'cat throughput.log').strip('\n')))
            e2e_latency_samples.append(process_metrics(self.k8s_api.exec_pod(sub, 'cat e2e_latency.log').strip('\n')))

        if np.array(latency_samples).shape[1] == 6:
            mean_latency = np.mean(latency_samples, axis=0)
            mean_throughput = np.mean(throughput_samples, axis=0)
            mean_e2e_latency = np.mean(e2e_latency_samples, axis=0)

            write_log('consumer-latency.csv', mean_latency)
            write_log('consumer-throughput.csv', mean_throughput * len(self.this_group['pods']['pub']))
            write_log('consumer-e2e_latency.csv', mean_e2e_latency)

        latency_samples = []
        throughput_samples = []
        for pub in self.this_group['pods']['pub']:
            wait_log_ready(pub, ['latency.log', 'throughput.log'])
            latency_samples.append(process_metrics(self.k8s_api.exec_pod(pub, 'cat latency.log').strip('\n')))
            throughput_samples.append(process_metrics(self.k8s_api.exec_pod(pub, 'cat throughput.log').strip('\n')))
        
        if np.array(latency_samples).shape[1] == 6:
            mean_latency = np.mean(latency_samples, axis=0)
            mean_throughput = np.mean(throughput_samples, axis=0)

            write_log('producer-latency.csv', mean_latency)
            write_log('producer-throughput.csv', mean_throughput * len(self.this_group['pods']['sub']))


    def run(self, schedule, topic, execution_time):
        self.config_cluster()
        self.deploy_services()
        self.deploy_clients()
        self.k8s_api.wait_pods_ready(self.this_group['pods']['pub'] + self.this_group['pods']['sub'])
        if self.enable_tc:
            self.k8s_api.limit_bw(self.this_group['pods']['pub'], 100)
            self.k8s_api.limit_bw(self.this_group['pods']['sub'], 1000)
        columns = list(schedule.columns[2:])

        index = 0
        while index < schedule.shape[0]:
            exp = schedule.iloc[index]
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
                if self.enable_tc:
                    self.k8s_api.limit_bw(self.this_group['pods']['broker'] + self.this_group['pods']['zookeeper'], 1000)
                self.start_zk()
                self.config_kafka()
                self.start_kafka()
                self.create_topic(topic)
                self.start_exp(exp['index'], topic, execution_time)
            except Exception as ex:
                self.logger.error('Config %d failed due to: %s' % (exp['index'], str(ex)))
                for pod in self.this_group['pods']['pub']:
                    self.k8s_api.exec_pod(pod, 'pgrep python3 | xargs kill', detach=True)
                for pod in self.this_group['pods']['sub']:
                    self.k8s_api.exec_pod(pod, 'pgrep java | xargs kill', detach=True)

            self.delete_kafka()
            index += 1


def sample_configs(budget):
    """
    Generate samples using Latin Hypercube Sampling
    :param budget: the number of samples
    :return:
    """
    meta_info = pd.read_csv('meta/knob_meta.csv')
    knobs = pd.DataFrame(lhs(meta_info.shape[0], samples=budget))
    knobs = knobs.apply(
        lambda raw_knobs: (np.ceil(raw_knobs * meta_info['max'])).astype(int), axis=1)
    knobs.columns = meta_info['knob']
    knobs.reset_index(inplace=True)
    msg_cps = ['none', 'gzip', 'snappy', 'lz4']
    knobs['producer->compression_type'] = knobs['producer->compression_type'].apply(lambda x: msg_cps[x-1])
    knobs.to_csv(path_or_buf="schedule.csv", index=None)
    return knobs


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Benchmark")
    parser.add_argument("--new", action="store_true", default=False, help="build model with new test plan")
    parser.add_argument("--from_index", type=int, default=0, help="start running test from the given index")
    parser.add_argument('--to_index', type=int, default=-1, help='stop running test at the given index')
    parser.add_argument("--budget", type=int, default=1000, help='LHS budget')
    parser.add_argument('--num_pubs', type=int, default=3)
    parser.add_argument('--num_brokers', type=int, default=1)
    parser.add_argument('--num_subs', type=int, default=2)
    parser.add_argument('--topic', type=str, default='distracted_driver_detection')
    parser.add_argument('--execution_time', type=int, default=90)
    parser.add_argument('--sleep', type=float, default=0)
    parser.add_argument('--tc', action='store_true', help='enable linux TC', default=False)
    args = parser.parse_args()

    if args.new or not os.path.exists("schedule.csv"):
        exp_sch = sample_configs(args.budget)
    else:
        exp_sch = pd.read_csv('schedule.csv')
    exp_sch = exp_sch.iloc[args.from_index: args.to_index, :]

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
        gman = GroupManager(gid, worker_nodes[gid]['pub'], worker_nodes[gid]['sub'], worker_nodes[gid]['kafka'], args.num_pubs,
                            args.num_brokers, args.num_subs, args.sleep, args.tc)
        exp_thr = threading.Thread(target=gman.run, args=(subset, args.topic, args.execution_time,))
        exp_thr.start()