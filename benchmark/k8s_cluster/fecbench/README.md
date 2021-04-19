# FECBench Framework
FECBench is an open source framework comprising a set of 106 applications covering a wide range of application classes to guide providers in building performance interference prediction models for their services without incurring undue costs and efforts. 

In DMSConfig, we use FECBench's resource monitoring functionalities to automate the process of tracing resource utilization rate of dockerized kafka server. The core of FECBench resource metrics monitor is [Collectd](https://collectd.org/). Specifically, we developed a Collectd python plugin to do so.

There are several key repositories when using FECBench bundle in DMSConfig:

- https://github.com/ZhuangweiKang/fecbench-ansible: Entrance of setting up FECBench cluster using Ansible.
- https://github.com/ZhuangweiKang/fecbench-server-docker: RabbitMQ server in FECBench 
- https://github.com/ZhuangweiKang/docker-collectd-plugin: Collectd docker plugin.
- https://github.com/doc-vu/docker-kafka-collectd-plugin: Collectd Kafka container plugin. (JMX metrics)
- https://github.com/ZhuangweiKang/pycollectdsubscriber: Subscriber for parsing resource metrics data received from RabbitMQ.  

## Steps of setting up FECBench in DMSConfig

### Step 1. Clone FECBench repository
```shell script
git clone https://github.com/ZhuangweiKang/fecbench-ansible
```

### Step 2. Install Ansible 2.8.5 & Update fecbench_inv
```shell script
pip3 install ansible==2.8.5
```

### Step 3. Install requirement
```shell script
./install_requirements.sh
```

### Step 4. Update client-deploy.yml indices_manager_ip --> IP address of the Master Node

### Step 5. Deploy FECBench using Ansible playbooks
```shell script
ansible-playbook playbooks/docker-server-deploy.yml -i fecbench_inv --limit server -vvv
ansible-playbook playbooks/client-deploy.yml -i fecbench_inv --limit client -vvv
```
