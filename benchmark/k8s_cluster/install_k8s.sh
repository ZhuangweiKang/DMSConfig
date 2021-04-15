#!/bin/bash

# install pip3 and required libraries
sudo apt-get update && sudo apt-get install -y python3-pip
pip3 install kubernetes pandas 

# install docker
sudo apt-get install docker.io
sudo systemctl enable docker
sudo systemctl start docker

# install kubernetes
sudo apt-get install -y apt-transport-https curl
curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
cat <<EOF | sudo tee /etc/apt/sources.list.d/kubernetes.list
deb https://apt.kubernetes.io/ kubernetes-xenial main
EOF
sudo apt-get update
sudo apt-get install -y kubelet=1.17.6-00 kubeadm=1.17.6-00 kubectl=1.17.6-00
sudo apt-mark hold kubelet kubeadm kubectl
