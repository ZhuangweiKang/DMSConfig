#!/usr/bin/env bash
workers=("isis31" "isis32" "isis33" "isis34" "isis35" "isis36" "isis37" "isis38" "isis27" "isis40")
for worker in "${workers[@]}"; do
    # shellcheck disable=SC2029
    ssh "$worker" "sudo $(sudo kubeadm token create --print-join-command)"
done