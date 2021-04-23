import pandas as pd
import numpy as np

warm_ratio = 5


def format_container_metrics(perf_data: pd.DataFrame, cores):
    perf_data = perf_data.replace(np.nan, 0)
    sel_index = list(perf_data.index)[int(perf_data.shape[0] / warm_ratio): -int(perf_data.shape[0] / warm_ratio)]
    perf_data = perf_data.iloc[sel_index]
    rm_index = perf_data[(perf_data['cpu.percent_value'] == 0) | (perf_data['memory.usage_total'] == 0)].index
    perf_data = perf_data.drop(rm_index)
    cpu_metrics = perf_data.filter(regex='cpu').columns
    perf_data[cpu_metrics] = perf_data[cpu_metrics] / (cores)
    memory_metrics = perf_data.filter(regex='memory').columns
    perf_data[memory_metrics] = perf_data[memory_metrics] / (1024 * 1024)
    perf_data = perf_data.mean().to_frame().T
    return perf_data


# TODO: code needs to be tested
def format_kafka_metrics(perf_data: pd.DataFrame):
    perf_data = perf_data.replace(np.nan, 0)
    sel_index = list(perf_data.index)[int(perf_data.shape[0] / warm_ratio): -int(perf_data.shape[0] / warm_ratio)]
    perf_data = perf_data.iloc[sel_index]
    rm_index = perf_data[(perf_data['server_broker_topics_AllTopicsBytesIn'] == 0) | (perf_data['server_broker_topics_AllTopicsBytesOut'] == 0)].index
    perf_data = perf_data.drop(rm_index)
    bytes_metrics = perf_data.filter(regex='Bytes|Memory|bytes|memory').columns
    perf_data[bytes_metrics] = perf_data[bytes_metrics] / (1024 * 1024)
    perf_data = perf_data.dropna()
    perf_data_min = perf_data.min().to_frame().T
    perf_data_max = perf_data.max().to_frame().T
    perf_data_median = perf_data.median().to_frame().T
    perf_data_mean = perf_data.mean().to_frame().T
    perf_data_std = perf_data.std().to_frame().T
    perf_data_p25 = perf_data.quantile(0.25).to_frame().T
    perf_data_p50 = perf_data.quantile(0.50).to_frame().T
    perf_data_p75 = perf_data.quantile(0.75).to_frame().T
    perf_data_p90 = perf_data.quantile(0.90).to_frame().T
    perf_data_p95 = perf_data.quantile(0.95).to_frame().T
    perf_data_p99 = perf_data.quantile(0.99).to_frame().T
    return perf_data_min, perf_data_max, perf_data_median, perf_data_mean, perf_data_std, perf_data_p25, perf_data_p50, perf_data_p75, perf_data_p90, perf_data_p95, perf_data_p99