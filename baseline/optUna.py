import sys
import optuna
import pandas as pd
import numpy as np
from model import *

exp_id = None
lat_bound = None
config_meta = pd.read_csv('../benchmark/meta/config_meta.csv')

best_reward = -100
best_config = None

progress = []
last_thr = 0


def objective(trial):
    global last_thr, best_reward, best_config, progress
    rec_config = np.array([])
    for index, row in config_meta.iterrows():
        param = row['name'].split('->')[1]
        rec_config = np.append(rec_config, trial.suggest_float(param, 0, 1))
    reward, thr, lat = get_reward(exp_id, rec_config, last_thr, lat_bound)
    last_thr = thr
    # log step
    if reward > best_reward:
        best_reward = reward
        best_config = rec_config
    
    record = {
        'best_config': best_config, 
        'current_config': rec_config,
        'reward': reward,
        'throughput': thr,
        'latency': lat, 
        'best_reward': best_reward, 
        'latency_bound': lat_bound
    }
    progress.append(record)

    return -reward


def optimize():
    study = optuna.create_study()
    study.optimize(objective, n_trials=500, show_progress_bar=True)
    return study.best_params


if __name__ == "__main__":
    for alph in range(1, 2):
        exps = ['exp-%d' % i for i in range(9)]
        for exp in exps:
            progress = []
            best_reward = -100
            exp_id = exp
            def_config = (config_meta['default'] - config_meta['min']) / (config_meta['max'] - config_meta['min'])
            def_config = def_config.to_numpy()
            def_lat = get_lat(exp_id, def_config)
            lat_bound = def_lat / alph
            optimize()
            progress = pd.DataFrame(progress)
            progress.to_csv('results/optuna/progress_%s_%s.csv' % (exp_id, str(alph)), index=None)
            