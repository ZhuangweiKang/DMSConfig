#!/usr/bin/env bash
# init master
sudo kubeadm init --pod-network-cidr=10.244.0.0/16

# config master
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

# install CNI
kubectl apply -f flannel-vxlan.yaml
kubectl taint nodes $(hostname | awk '{print tolower($0)}') node-role.kubernetes.io/master-
kubectl get pod -n kube-system

# deploy cluster metrics monitor
kubectl create ns kubernetes-dashboard
kubectl create secret generic kubernetes-dashboard-certs --from-file=certs -n kubernetes-dashboard
kubectl apply -f deploy/
kubectl apply -f access/