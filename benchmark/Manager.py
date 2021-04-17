import argparse
import os
import pandas as pd
import subprocess
import threading
import time
import json
import zmq

import K8sAPI
from pyDOE import lhs
import Monitor
import utils

DMSCONFIG_PRO_IMAGE = "zhuangweikang/nyctaxi_producer"
DMSCONFIG_CON_IMAGE = "zhuangweikang/nyctaxi_consumer"
DMSCONFIG_KAFKA_IMAGE = "zhuangweikang/dmsconfig-kafka:latest"
DMSCONFIG_ZOOKEEPER_IMAGE = "zhuangweikang/dmsconfig-zookeeper:latest"

BROKER_REQ_RES = {'cpu': '4', 'memory': '8Gi'}
CLIENT_REQ_RES = {'cpu': '2', 'memory': '2Gi'}
ZOOKEEPER_REQ_RES = {'cpu': '2', 'memory': '4Gi'}

NUM_PUB = 2
NUM_BROKER = 3
NUM_SUB = 1


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
            'nodes': {'client': client_node, 'server': server_node},
            'pods': {'pub': [], 'sub': [], 'broker': [], 'zookeeper': []},
            'services': {'kafka': [], 'zookeeper': []}
        }
        self.logger.info(msg='Client node: %s, Server node: %s, Num_pubs: %d, Num_brokers: %d, Num_subs: %d' %
                             (client_node, server_node, num_pubs, num_brokers, num_subs))

    def config_cluster(self):
        client_label = 'G%d_Client' % self.gid  # node for kafka client
        self.k8s_api.label_node(node=self.this_group['node']['client'], label_val=client_label)

        server_label = 'G%d_Server' % self.gid  # node for kafka server
        self.k8s_api.label_node(node=self.this_group['node']['server'], label_val=server_label)

        self.logger.info('Set client node label to: %s' % self.k8s_api.get_node_label(self.this_group['nodes']['client']))
        self.logger.info('Set server node label to: %s' % self.k8s_api.get_node_label(self.this_group['nodes']['server']))

    def deploy_kafka(self):
        # create zookeeper pod
        pod_name = 'g%d-zk' % self.gid
        cluster_ip = self.k8s_api.create_svc(svc_name='g%d-zk' % self.gid, svc_ports=[2181])
        self.k8s_api.create_pod(name=pod_name, image=DMSCONFIG_ZOOKEEPER_IMAGE, resource_limit=ZOOKEEPER_REQ_RES,
                                command=None, node_label=self.this_group['nodes']['server'])
        self.this_group['pods']['zookeeper'].append(pod_name)
        self.this_group['services']['zookeeper'].append(cluster_ip)

        # create a K8s Service for each Kafka broker
        for j in range(self.num_brokers):
            pod_name = 'g-%d-b-%d' % (self.gid, j)
            cluster_ip = self.k8s_api.create_svc(svc_name='g-%d-b-%d' % (self.gid, j), svc_ports=[9092])
            self.k8s_api.create_pod(pod_name, DMSCONFIG_KAFKA_IMAGE, BROKER_REQ_RES, None, node_label=self.this_group['server_node'])
            self.this_group['pods']['broker'].append(pod_name)
            self.this_group['services']['kafka'].append(cluster_ip)

        # create pub pods
        for j in range(self.num_pubs):
            pod_name = 'g-%d-p-%d' % (self.gid, j)
            self.k8s_api.create_pod(pod_name, DMSCONFIG_PRO_IMAGE, CLIENT_REQ_RES, None, node_label=self.this_group['nodes']['client'])
            self.this_group['pods']['pub'].append(pod_name)
            self.logger.info(msg='Deploy producer %s' % pod_name)

        # create sub pods
        for j in range(self.num_subs):
            pod_name = 'g-%s-s-%d' % (self.gid, j)
            self.k8s_api.create_pod(pod_name, DMSCONFIG_CON_IMAGE, CLIENT_REQ_RES, None, node_label=self.this_group['nodes']['client'])
            self.this_group['pods']['sub'].append(pod_name)
            self.logger.info(msg='Deploy consumer %s' % pod_name)

        all_pods = list(filter(lambda pname: 'g%d' % gid in pname, self.k8s_api.list_pods_name()))
        self.logger.info(msg='Create pods %s' % str(all_pods))

    def config_kafka(self):
        for j, pod in enumerate(self.this_group['pods']['broker']):
            # add static configs
            self.broker_knobs.update({
                "broker.id": j,
                "offsets.topic.replication.factor": len(self.this_group['pods']['broker']),
                "listeners": "PLAINTEXT://0.0.0.0:9092",
                "advertised.listeners": "PLAINTEXT://%s:9092" % self.this_group['services']['kafka'][j],
                "zookeeper.connect": "%s:2181" % self.this_group['services']['zookeeper'][0],
                "log.dirs": "/kafka/logs/kafka"
            })

            # populate Broker config file
            with open('runtime/%s-server.properties' % pod, 'w') as f:
                for key in self.broker_knobs:
                    f.write('%s = %s\n' % (key, self.broker_knobs[key]))

            # copy broker config file into pod
            subprocess.check_output("kubectl cp runtime/%s-server.properties %s:/kafka/config/server.properties" % (pod, pod), shell=True)
            self.logger.info('Broker configuration: %s' % str(self.broker_knobs))

    def create_topic(self, topic):
        for i, broker in enumerate(self.this_group['pods']['broker']):
            cmd = ['sh', 'bin/kafka-topics.sh',
                   '--bootstrap-server', '%s:9092' % self.this_group['services']['kafka'][i],
                   '--zookeeper', '%s:2181' % self.this_group['services']['zookeeper'][0],
                   '--create', '--topic', topic,
                   '--replication-factor', len(self.this_group['pods']['broker']) - 1]
            for key in self.topic_knobs:
                cmd.append('--%s' % key)
                cmd.append(self.topic_knobs[key])
            self.k8s_api.exec_pod(broker, cmd)

            cmd = ['sh', 'bin/kafka-topics.sh', '--list',
                   '--zookeeper', '%s:2181' % self.this_group['services']['zookeeper'][0]]
            topic = self.k8s_api.exec_pod(broker, cmd)
            self.logger.info(msg='Create topic %s in broker %s' % (topic, broker))

    def start_kafka(self):
        # start zookeeper
        cmd = ['sh', 'bin/zookeeper-server-start.sh', '-daemon', 'config/zookeeper.properties']
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd)
        time.sleep(5)
        cmd = ['sh', 'pgrep', 'zookeeper']
        zk_pid = self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd)
        self.logger.info(msg='Zookeeper process ID: %s' % zk_pid)

        for broker in self.this_group['pods']['broker']:
            cmd = ['JMX_PORT=9999', 'sh', 'bin/kafka-server-start.sh', '-daemon', 'config/server.properties']
            self.k8s_api.exec_pod(broker, cmd)
            cmd = ['sh', 'pgrep', 'kafka']
            kafka_pid = self.k8s_api.exec_pod(broker, cmd)
            self.logger.info(msg='Kafka process ID: %s' % kafka_pid)

    def start_exp(self, topic, execution_time):
        brokers_str = ','.join(['%s:9092' % svc for svc in self.this_group['services']['kafka']])
        for j, pod in enumerate(self.this_group['pods']['sub']):
            cmd = ['spark-submit',
                   '--packages', 'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7',
                   '--py-files', 'consumer.py',
                   '--bootstrap_servers', brokers_str,
                   '--topic', topic,
                   '--execution_time', execution_time]
            for knob in self.sub_knobs:
                cmd.append('--%s' % knob)
                cmd.append(self.sub_knobs[knob])

            self.logger.info(msg='Consumer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd)

        for j, pod in enumerate(self.this_group['pods']['pub']):
            cmd = ['python3', 'producer.py',
                   '--topic', topic,
                   '--bootstrap_servers', brokers_str]
            for knob in self.pub_knobs:
                cmd.append('--%s' % knob)
                cmd.append(self.pub_knobs[knob])

            self.logger.info(msg='Producer %s executes: %s' % (pod, ' '.join(cmd)))
            self.k8s_api.exec_pod(pod, cmd)

    def stop_exp(self):
        stop_zk = ['sh', 'bin/zookeeper-server-stop.sh']
        self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], stop_zk)
        cmd = ['sh', 'pgrep', 'zookeeper']
        zk_pid = self.k8s_api.exec_pod(self.this_group['pods']['zookeeper'][0], cmd)
        if len(zk_pid) == 0:
            self.logger.info(msg='Stop Zookeeper server %s' % zk_pid)
        else:
            self.logger.error(
                msg='Failed to stop zookeeper server %s(%s)' % (self.this_group['pods']['zookeeper'][0], zk_pid))

        for broker in self.this_group['pods']['broker']:
            stop_kafka = ['sh', 'bin/kafka-server-stop.sh']
            self.k8s_api.exec_pod(broker, stop_kafka)
            cmd = ['sh', 'pgrep', 'kafka']
            kafka_pid = self.k8s_api.exec_pod(broker, cmd)
            if len(zk_pid) == 0:
                self.logger.info(msg='Stop Kafka server %s(%s)' % (broker, kafka_pid))
            else:
                self.logger.error(
                    msg='Failed to stop zookeeper server %s(%s)' % (broker, kafka_pid))

    def run(self, schedule, topic, execution_time):
        # ZMQ is for communication with Monitor, which is running as process on the same machine with this Manager
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.connect('tcp://localhost:5555')

        self.config_cluster()
        self.deploy_kafka()
        all_pods = []
        for e in self.this_group['pods']:
            all_pods.extend(self.this_group['pods'][e])
        self.k8s_api.wait_pods_ready(all_pods)
        self.logger.info('All pods in this group are ready.')

        columns = schedule.columns
        for index, exp in schedule.iterrows():
            entity_knob = columns[index].split('->')
            if entity_knob[0] == 'pub':
                self.pub_knobs.update({columns[1]: exp[columns[index]]})
            elif entity_knob[0] == 'broker':
                self.broker_knobs.update({entity_knob[1]: exp[columns[index]]})
            elif entity_knob[0] == 'sub':
                self.sub_knobs.update({entity_knob[1]: exp[columns[index]]})
            elif entity_knob[0] == 'topic':
                self.topic_knobs.update({entity_knob[1]: exp[columns[index]]})

            self.logger.info(msg='-----------------------')
            self.logger.info(msg='Starting experiment: %d' % index)
            monitor_thr = threading.Thread(target=Monitor.run, daemon=True, args=(gid, index,))
            monitor_thr.start()
            self.logger.info('Starting monitor.')
            self.start_kafka()
            self.config_kafka()
            self.create_topic(topic)
            self.start_exp(topic, execution_time)
            time.sleep(execution_time)
            self.stop_exp()
            self.logger.info(msg='Done')
            # notify monitor to parse metrics from InfluxDB
            notify_monitor = {
                'pods': self.this_group['pods']['broker'],
                'duration': execution_time
            }
            notify_str = json.dumps(notify_monitor)
            socket.send_string(notify_str)
            self.logger.info('Notify monitor to start processing experiments results.')

    def clean(self):
        all_pods = self.k8s_api.list_pods_name()
        [self.k8s_api.delete_pod(pod) for pod in all_pods]
        all_svcs = self.k8s_api.list_services()
        [self.k8s_api.delete_svc(svc) for svc in all_svcs]


def sample_configs(budget):
    """
    Generate samples using Latin Hypercube Sampling
    :param budget: the number of samples
    :return:
    """
    meta_info = pd.read_csv('meta/knob_meta.csv')
    knobs = pd.DataFrame(lhs(meta_info.shape[0], samples=budget, criterion="maximin"))
    knobs = knobs.apply(
        lambda raw_knobs: (raw_knobs * (meta_info['max'] - meta_info['min']) * meta_info['unit']).round(2).astype(
            int), axis=1)
    knobs.columns = meta_info['knob']
    knobs.to_csv(path_or_buf="schedule.csv", index=True, index_label='sample')
    return knobs


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Benchmark")
    parser.add_argument('--groups', type=int, default=1, help='The number of groups')
    parser.add_argument("--new", action="store_true", default=False, help="build model with new test plan")
    parser.add_argument("--from", type=int, default=0, help="start running test from the given index")
    parser.add_argument('--to', type=int, default=-1, help='stop running test at the given index')
    parser.add_argument("--budget", type=int, default=1000, help='LHS budget')
    parser.add_argument('--num_pubs', type=int, default=1)
    parser.add_argument('--num_brokers', type=int, default=3)
    parser.add_argument('--num_subs', type=int, default=1)
    args = parser.parse_args()

    if args.new or not os.path.exists("schedule.csv"):
        exp_sch = sample_configs(args.budget)
    else:
        exp_sch = pd.read_csv('schedule.csv')
    exp_sch = exp_sch.iloc[args.from_index: args.to_index, :]

    columns = exp_sch.columns

    k = 0
    cluster = K8sAPI.K8sCluster()
    worker_nodes = cluster.list_pods_name()[1:]
    group_size = int(exp_sch.shape[0] / args.parallel)
    # distribute experiments to multiple groups
    for gid in range(args.groups):
        subset = exp_sch.iloc[range(gid * group_size, min((gid + 1) * group_size, exp_sch.shape[0]))].reset_index().T
        gman = GroupManager(gid, worker_nodes[k], worker_nodes[k+1], args.num_pubs, args.num_brokers, args.num_subs)
        exp_thr = threading.Thread(target=gman.run, args=(subset, args.topic, args.execution_time,))
        exp_thr.start()
        k += 2

