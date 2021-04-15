#!/usr/bin/python3
from sklearn.externals import joblib
import pandas as pd
import numpy as np


config_meta = pd.read_csv('../benchmark/meta/config_meta.csv')


def get_thr(exp_id, config):
    thr_model = joblib.load('../dms_simulator/%s/throughput.joblib' % (exp_id))
    pre_thr = thr_model.predict(X=config.reshape(1, -1))
    pre_thr = np.random.normal(pre_thr, np.abs(0.05 * pre_thr))
    return pre_thr[0]


def get_lat(exp_id, config):
    lat_model = joblib.load('../dms_simulator/%s/latency.joblib' % (exp_id))
    pre_lat = lat_model.predict(X=config.reshape(1, -1))
    return pre_lat[0]


def get_reward(exp_id, rec_config, last_thr, lat_bound):
    # predict default throughput and latency
    def_config = (config_meta['default'] - config_meta['min']) / (config_meta['max'] - config_meta['min'])
    def_config = def_config.to_numpy()
    def_thr = get_thr(exp_id, def_config)

    if lat_bound is None:
        lat_bound = get_lat(exp_id, def_config)

    # predict latency and throughput
    thr = get_thr(exp_id, rec_config)
    lat = get_lat(exp_id, rec_config)

    delta_thr_0 = thr - def_thr  # long-term improvement
    delta_thr_t = thr - last_thr
    lat_over = lat - lat_bound  # latency overhead

    if delta_thr_0 > 0:
        reward = ((1 + delta_thr_0) ** 2 - 1) * abs(1 + delta_thr_t)
    else:
        reward = -((1 - delta_thr_0) ** 2 - 1) * abs(1 - delta_thr_t)

    # add latency penalty
    if lat_over > 0:
        if reward > 0:
            reward *= -(1 + lat_over)
        else:
            reward *= (1 + lat_over)

    return reward, thr, lat