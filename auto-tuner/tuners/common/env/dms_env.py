from joblib import load
import numpy as np
import gym
from gym import spaces
import pandas as pd
from tuners.common.env.utils import queue


class Simulator:
    def __init__(self, model_path, meta_path):
        self.predictors = {}
        kafka_state_meta = pd.read_csv('%s/kafka_state_meta.csv' % meta_path)
        # container_state_meta = pd.read_csv('%s/container_state_meta.csv' % meta_path)
        # states = pd.concat([kafka_state_meta, container_state_meta], axis=0)['name']
        states = kafka_state_meta['name'].to_list()
        
        states.remove('server_broker_topics_AllTopicsBytesOut')
        states.extend(['latency', 'throughput'])
        for m in states:
            self.predictors.update({m: load(model_path + '/%s.joblib' % m)})

    def predict(self, action, metrics):
        results = []
        for m in metrics:
            val = self.predictors[m].predict(action)
            results.append(val)
        results = np.abs(results)
        results = np.clip(results, a_min=0.0, a_max=1.0)
        results = [np.array(x) for x in results]
        return results


class DMSConfigEnv(gym.Env):
    def __init__(self, seed, model_path, meta_path, latency_bound):
        act_meta = pd.read_csv('%s/knob_meta.csv' % meta_path)
        kafka_state_meta = pd.read_csv('%s/kafka_state_meta.csv' % meta_path)
        # container_state_meta = pd.read_csv('%s/container_state_meta.csv' % meta_path)
        # state_meta = pd.concat([kafka_state_meta, container_state_meta], axis=0)
        state_meta = kafka_state_meta

        # since server_broker_topics_AllTopicsBytesOut is throughput, remove it from state_meta
        state_meta = state_meta[state_meta['name'] != 'server_broker_topics_AllTopicsBytesOut'].reset_index()
        del state_meta['index']
        self.act_names = act_meta['knob'].tolist()
        self.obs_metrics = state_meta['name'].tolist()
        self.rew_metrics = ['latency', 'throughput']

        np.random.seed(seed)

        self._cur_obs = None
        self._cur_step = 0

        self.action_space = spaces.Box(low=0, high=1, shape=(len(self.act_names),), dtype=np.float32)
        self.observation_space = spaces.Box(low=0, high=1, shape=(len(self.obs_metrics),), dtype=np.float32)
        self.simulator = Simulator(model_path, meta_path)

        self.default_act = np.array((act_meta['default'] - act_meta['min']) / (act_meta['max'] - act_meta['min'])).reshape(1, -1)
        
        self._def_lat, self._def_thr = self.simulator.predict(self.default_act, self.rew_metrics)
        
        self._best_reward = -100
        self._lat_bound = float(latency_bound) * self._def_lat
        self._latency = None
        self._throughput = None
        self._last_thr = 0
        self.tail_queue = queue(capacity=50)

    def get_reward(self, action):
        self._latency, self._throughput = self.simulator.predict(action, self.rew_metrics)
        delta_thr_0 = self._throughput - self._def_thr  # long-term improvement
        delta_thr_t = self._throughput - self._last_thr  # short-term improvement
        lat_over = self._latency - self._lat_bound  # latency overhead

        if delta_thr_0 > 0:
            reward = ((1+delta_thr_0)**2 - 1) * abs(1+delta_thr_t)
        else:
            reward = -((1-delta_thr_0)**2 - 1) * abs(1-delta_thr_t)

        # add latency penalty
        if lat_over > 0:
            if reward > 0:
                reward *= -(1 + lat_over)
            else:
                reward *= (1 + lat_over)

        self._last_thr = self._throughput

        if self.tail_queue.is_full():
            self.tail_queue.dequeue()
        self.tail_queue.enqueue(reward)
        return reward

    def get_reward2(self, action):
        self._latency, self._throughput = self.simulator.predict(action, self.rew_metrics)
        if self._last_thr != 0:
            delta_thr = (self._throughput-self._last_thr) / self._last_thr
            delta_lat = (self._lat_bound-self._latency) / self._lat_bound
            reward = delta_lat * delta_thr
            if delta_thr < 0 and delta_lat < 0:
                reward = -reward
        else:
            reward = 0
        self._last_thr = self._throughput
        if self.tail_queue.is_full():
            self.tail_queue.dequeue()
        self.tail_queue.enqueue(reward)
        
        return reward

    def step(self, action):
        self._cur_obs = self.simulator.predict(action, self.obs_metrics)
        reward = self.get_reward(action)
        self._cur_step += 1

        done = self.tail_queue.is_full() and (np.std(self.tail_queue.get_array()) <= 0.05)
        info = {
            'action': action, 
            'obs': self._cur_obs, 
            'reward': reward,
            'cur_step': self._cur_step, 
            'throughput': self._throughput,
            'latency': self._latency,
            'latency_bound': self._lat_bound
        }
        return self._cur_obs, reward, done, info

    def reset(self):
        # self._cur_obs = self.get_default_obs()
        self._cur_obs = np.random.rand(*self.observation_space.shape)
        self._cur_step = 0
        self._last_thr = 0
        self._throughput = None
        self._latency = None
        self.tail_queue.reset()
        return self._cur_obs

    def render(self, mode=None):
        raise NotImplementedError

    def get_latency_bound(self):
        return self._lat_bound
    
    def get_latency(self):
        return self._latency

    def get_throughput(self):
        return self._throughput

    def get_default_obs(self):
        return self.simulator.predict(self.default_act, self.obs_metrics)