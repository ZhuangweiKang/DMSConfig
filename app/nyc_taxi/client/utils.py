import numpy as np


def process_metrics(data):
    if len(data) > 0:
        _min = np.min(data)
        _25th, _50th, _90th = np.quantile(data, [0.25, 0.5, 0.9])
        _mean = np.mean(data)
        _max = np.max(data)
        return [_min, _25th, _50th, _90th, _mean, _max]
    else:
        return []


def print_metrics(name, data):
    if len(data) > 0:
        _min = np.min(data)
        _25th, _50th, _90th = np.quantile(data, [0.25, 0.5, 0.9])
        _mean = np.mean(data)
        _max = np.max(data)
    print(','.join([str(x) for x in [_min, _25th, _50th, _90th, _mean, _max]]))