#!/usr/bin/python3
import sys
import numpy as np
import pandas as pd
import json
from ConfigSpace.hyperparameters import UniformFloatHyperparameter
from smac.configspace import ConfigurationSpace
from smac.facade.smac_hpo_facade import SMAC4HPO, SMAC4AC
from smac.scenario.scenario import Scenario
from model import *


exp_id = None
lat_bound = None
config_meta = pd.read_csv('../benchmark/meta/config_meta.csv')

best_reward = -100
best_config = None

progress = []
last_thr = 0


def objective(rec_config):
    global last_thr, best_reward, best_config, progress
    rec_config = np.array(rec_config.get_array()).reshape(1, -1)
    reward, thr, lat = get_reward(exp_id, rec_config, last_thr, lat_bound)
    last_thr = thr
    return -reward


def optimize():
    # Build Configuration Space which defines all parameters and their ranges
    cs = ConfigurationSpace()
    for index, row in config_meta.iterrows():
        param = row['name'].split('->')[1]
        cs.add_hyperparameter(UniformFloatHyperparameter(param, 0.0, 1.0))

    # Scenario object
    scenario = Scenario({"run_obj": "quality",  # we optimize quality (alternatively runtime)
                         "runcount-limit": 500,  # max. number of function evaluations;
                         "cs": cs,  # configuration space
                         "deterministic": "true"
                         })

    # Optimize, using a SMAC-object
    smac = SMAC4HPO(scenario=scenario,
                    rng=np.random.RandomState(42),
                    tae_runner=objective)
    incumbent = smac.optimize()
    inc_value = smac.get_tae_runner().run(incumbent, 1)[1]

    record = {
        'best_config': incumbent.get_dictionary(),
        'latency': get_lat(exp_id, incumbent.get_array()),
        'throughput': get_thr(exp_id, incumbent.get_array()),
        'best_reward': inc_value,
        'latency_bound': lat_bound
    }
    return record


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
            lat_const = alph
            best = optimize()
            with open('results/smac/best_%s_%s.json' % (exp_id, str(alph)), 'w') as f:
                json.dump(best, f)
