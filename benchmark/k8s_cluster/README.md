# DMSConfig Cluster Deployment

DMSConfig uses a Kubernetes cluster to manage experimental instances. 

### Steps of setting up DMSConfig cluster
```shell script
# 1. update dmsconfig_inv

# 2. install k8s on nodes
ansible-playbook -i dmsconfig_inv playbooks/install_k8s.yml

# 3.  clone DMSConfig repository
ansible-playbook -i dmsconfig_inv playbooks/get_repo.yml

# 4. init k8s master node
ansible-playbook -i dmsconfig_inv playbooks/init.yml

# 5. add worker nodes into the cluster
ansible-playbook -i dmsconfig_inv playbooks/join.yml

# 6. set up FECBench bundle, refer README file in  DMSConfig/benchmark/fecbench
```