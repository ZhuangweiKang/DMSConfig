#!/usr/bin/env bash
sudo kubeadm reset -f
sudo rm -r /etc/cni/net.d/*
sudo rm -r $HOME/.kube/*
sudo ip link delete cni0
sudo ip link delete flannel.1

