#!/bin/bash

METAS=../../benchmark/meta
MODELS=../../emulator/models
VAR=0.8
sudo python3 run.py --play --env_mode $MODELS --meta_info $METAS --lat_bound $VAR
