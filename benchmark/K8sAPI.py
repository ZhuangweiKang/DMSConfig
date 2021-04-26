import math
import subprocess
from kubernetes import client, config

PSBENCH_PORTS = [2181, 9092, 9999]


class K8sCluster:
    def __init__(self):
        config.load_kube_config()
        self.core_api = client.CoreV1Api

        self.master_node = {}
        self.worker_nodes = {}
        self.nodes_ip = {}

        for i, node in enumerate(self.core_api().list_node().items):
            if i == 0:
                self.master_node.update({node.metadata.name: node})
            else:
                self.worker_nodes.update({node.metadata.name: node})

        for node in self.worker_nodes:
            for addr in self.worker_nodes[node].status.addresses:
                if addr.type == "InternalIP":
                    self.nodes_ip.update({node: addr.address})
                    # self.label_node(node, addr.address)

        self.pods = {}
        self.services = {}

    def create_pod(self, name, image, resource_limit, command, node_label, volume=None, envs=None):
        volume_mounts = []
        volumes = []
        if volume:
            volume_mounts = [client.V1VolumeMount(name="%s-volume" % name, mount_path=volume['container_path'])]
            volumes = [client.V1Volume(name="%s-volume" % name, host_path=client.V1HostPathVolumeSource(path=volume['host_path']))]
        pod_envs = []
        if envs:
            for env in envs:
                pod_envs.append(client.V1EnvVar(name=env['name'], value=env['value']))
        pod = self.core_api().create_namespaced_pod(
            namespace="default",
            body=client.V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=client.V1ObjectMeta(
                    name=name,
                    namespace="default",
                    labels={"app": name}
                ),
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name=name,
                            tty=True,
                            image=image,
                            image_pull_policy="IfNotPresent",
                            security_context=client.V1SecurityContext(
                                capabilities=client.V1Capabilities(add=["NET_ADMIN"])),
                            resources=client.V1ResourceRequirements(
                                limits=resource_limit),
                            ports=[client.V1ContainerPort(container_port=prt) for prt in PSBENCH_PORTS],
                            volume_mounts=volume_mounts,
                            env=pod_envs,
                            command=command
                        )
                    ],
                    volumes=volumes,
                    restart_policy="Never",
                    node_selector=node_label
                )
            ))
        self.pods.update({name: pod})

    def exec_pod(self, pod, command, detach=False):
        if type(command) == list:
            command = ' '.join([str(x) for x in command])
        if detach:
            wrap_cmd = 'nohup kubectl exec %s -- %s > logs/%s.log 2>&1 &' % (pod, command, pod)
        else:
            wrap_cmd = 'kubectl exec %s -- %s' % (pod, command)
        rc = subprocess.check_output(wrap_cmd, stderr=subprocess.STDOUT, shell=True).decode()
        return rc

    def create_svc(self, svc_name, svc_ports):
        svc = self.core_api().create_namespaced_service(
            namespace="default",
            body=client.V1Service(
                api_version="v1",
                kind="Service",
                metadata=client.V1ObjectMeta(name=svc_name),
                spec=client.V1ServiceSpec(
                    selector={'app': svc_name},
                    ports=[client.V1ServicePort(name=str(prt), port=prt) for prt in svc_ports])
            ))
        self.services.update({svc_name: svc})
        return svc.spec.cluster_ip

    def get_free_rsrc(self):
        resources = {}
        all_nodes = self.core_api().list_node().items
        self.master_node = all_nodes[0]
        self.worker_nodes = all_nodes[1:]

        for node in self.worker_nodes:
            resources.update({node.metadata.name: {
                'cpu': int(node.status.allocatable['cpu']) - 1,
                'memory': math.floor(int(node.status.allocatable['memory'].replace('Ki', '')) / (1024 * 1024)) - 1}})
        return resources

    def list_nodes_name(self):
        nodes = []
        for node in self.worker_nodes:
            nodes.append(node.metadata.name)
        return nodes

    def list_pods_ip(self):
        pods_addr = {}
        for pod in self.pods:
            pods_addr.update({pod: self.pods[pod].status.pod_ip})
        return pods_addr

    def list_pods_name(self):
        pods_name = []
        for pod in self.pods:
            pods_name.append(self.pods[pod].metadata.name)
        return pods_name

    def list_container(self, find_pods):
        containers = {}
        for pod in find_pods:
            node_name = self.pods[pod].spec.node_name
            #print(self.pods[pod].status.container_statuses)
            con_ids = [con.container_id.split('docker://')[1] for con in self.pods[pod].status.container_statuses]
            if node_name not in containers:
                containers.update({node_name: {pod: con_ids}})
            else:
                containers[node_name].update({pod: con_ids})
        return containers

    def list_container_vifs(self, pods):
        containers = self.list_container(pods)
        veth_ifs = {}
        for node in containers:
            veth_ifs.update({node: []})
            # get all ifaces from node
            exec_command = "sudo ssh ubuntu@%s ip ad | grep @" % node
            tmp = subprocess.check_output(exec_command, shell=True).decode().strip().split('\n')
            tmp = [x.split('@')[0] for x in tmp]
            vifs = {}
            for item in tmp:
                ifpair = item.split(': ')
                vifs.update({ifpair[0]: ifpair[1]})
            for pod in containers[node]:
                # stream函数有bug，在scaled环境中会发生文件描述符溢出
                # exec_command = [
                #     '/bin/sh',
                #     '-c',
                #     'cat /sys/class/net/eth0/iflink | sed s/[^0-9]*//g']
                # ifindex = stream(self.core_api().connect_get_namespaced_pod_exec, name=pod, namespace='default',
                #                  command=exec_command,
                #                  stderr=True, stdin=False,
                #                  stdout=True, tty=False)
                exec_command = 'kubectl exec %s -- cat /sys/class/net/eth0/iflink | sed s/[^0-9]*//g' % pod
                ifindex = subprocess.check_output(exec_command, shell=True).decode().replace('\n', '')
                veth_ifs[node].append((pod, vifs[ifindex]))
        return veth_ifs

    def wait_pods_ready(self, pods):
        for pod in pods:
            while True:
                pod_obj = self.core_api().read_namespaced_pod_status(name=pod, namespace='default')
                try:
                    if pod_obj.status.container_statuses[0].state.running:
                        self.pods.update({pod: pod_obj})
                        break
                except:
                    pass

    def limit_bw(self, pods, bandwidth=1000, detach=False):
        veths = self.list_container_vifs(pods)
        for node in veths:
            for veth in veths[node]:
                # set download BW in node
                cmd = "sudo ssh ubuntu@%s sudo tc qdisc add dev %s root tbf rate %dmbit latency 10ms burst 1000kb mpu 64 mtu 1540" \
                      % (node, veth[1], bandwidth)
                subprocess.check_output(cmd, shell=True).decode()

                # set upload BW in container
                cmd = "tc qdisc add dev eth0 root tbf rate %dmbit latency 10ms burst 1000kb mpu 64 mtu 1540" % int(bandwidth)
                self.exec_pod(veth[0], cmd, detach)

    def label_node(self, node, label):
        self.core_api().patch_node(name=node, body={
            "metadata": {
                "labels": label
            }
        })

    def delete_pod(self, pod_name):
        try:
            self.core_api().delete_namespaced_pod(name=pod_name, namespace='default', grace_period_seconds=0)
            del self.pods[pod_name]
        except Exception as ex:
            print('Error when deleting pod: %s, info: %s' % (pod_name, ex))

    def delete_svc(self, svc_name):
        try:
            self.core_api().delete_namespaced_service(name=svc_name, namespace='default', grace_period_seconds=0)
            del self.services[svc_name]
        except Exception as ex:
            print('Error when deleting service: %s, info: %s' % (svc_name, ex))