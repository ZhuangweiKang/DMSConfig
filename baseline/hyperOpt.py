import pandas as pd
import numpy as np
from sklearn.externals import joblib
from hyperopt import fmin, tpe, hp, space_eval, rand, STATUS_OK, STATUS_FAIL, anneal
import sys
from model import *

exp_id = None
param_min = None
param_max = None
lat_bound = None
config_meta = pd.read_csv('../benchmark/meta/knob_meta.csv')

best_reward = -100
best_config = None

last_thr = 0
progress = []


def objective(rec_config):
    global last_thr, best_reward, best_config, progress
    rec_config = np.array(rec_config)
    # normalize args
    rec_config = np.array([(rec_config[i]-param_min[i]) / (param_max[i]-param_min[i]) for i in range(len(rec_config))])
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
    return {'loss': -reward, 'status': STATUS_OK}


def optimize():
    global param_min, param_max

    space = []
    for index, row in config_meta.iterrows():
        param = row['name'].split('->')[1]
        space.append(hp.randint(param, row['min'], row['max']))
    
    param_min = list(config_meta['min'])
    param_max = list(config_meta['max'])
    best = fmin(objective, space, algo=tpe.suggest, max_evals=500)
    space_eval(space, best)


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
            progress.to_csv('results/hyperopt/progress_%s_%s.csv' % (exp_id, str(alph)), index=None)