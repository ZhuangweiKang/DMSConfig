import os
import time
from collections import deque
import pickle
import pandas as pd

from baselines.ddpg.ddpg_learner import DDPG
from baselines.ddpg.models import Actor, Critic
from ddpg.memory import Memory
from ddpg.prio_memory import PriMemory
import bench
from baselines.ddpg.noise import AdaptiveParamNoiseSpec, NormalActionNoise, OrnsteinUhlenbeckActionNoise
from baselines.common import set_global_seeds
import baselines.common.tf_util as U

import logger
import numpy as np

try:
    from mpi4py import MPI
except ImportError:
    MPI = None


def learn(network, env,
          seed=1000,
          total_timesteps=2e4,
          nb_epochs=None,  # with default settings, perform 1M steps total
          nb_epoch_cycles=1000,
          nb_rollout_steps=1,
          reward_scale=1.0,
          render=False,
          render_eval=False,
          noise_type='normal_0.1',
          normalize_returns=False,
          normalize_observations=False,
          critic_l2_reg=0.01,
          actor_lr=0.0001,
          critic_lr=0.0001,
          popart=False,
          gamma=0.9,
          clip_norm=None,
          nb_train_steps=2,  # per epoch cycle and MPI worker,
          nb_eval_steps=100,
          batch_size=16,  # per MPI worker
          tau=0.001,
          eval_env=None,
          param_noise_adaption_interval=50,
          pri_flag=True,
          **network_kwargs):

    set_global_seeds(seed)
    eval_env = env

    if total_timesteps is None:
        total_timesteps = 1e6 

    if MPI is not None:
        rank = MPI.COMM_WORLD.Get_rank()
    else:
        rank = 0

    nb_actions = env.action_space.shape[-1]
    nb_obs = env.observation_space.shape[-1]

    # assert (np.abs(env.action_space.low) == env.action_space.high).all()  # we assume symmetric actions.

    if pri_flag: # use priority memory
        memory = PriMemory
    else:
        memory = Memory(limit=int(5e5), action_shape=env.action_space.shape, observation_shape=env.observation_space.shape)
    critic = Critic(nb_actions, nb_obs, critic_l2_reg, network=network, **network_kwargs)
    actor = Actor(nb_actions, network=network, **network_kwargs)

    action_noise = None
    param_noise = None
    if noise_type is not None:
        for current_noise_type in noise_type.split(','):
            current_noise_type = current_noise_type.strip()
            if current_noise_type == 'none':
                pass
            elif 'adaptive-param' in current_noise_type:
                _, stddev = current_noise_type.split('_')
                param_noise = AdaptiveParamNoiseSpec(initial_stddev=float(stddev), desired_action_stddev=float(stddev))
            elif 'normal' in current_noise_type:
                _, stddev = current_noise_type.split('_')
                action_noise = NormalActionNoise(mu=np.zeros(nb_actions), sigma=float(stddev) * np.ones(nb_actions))
            elif 'ou' in current_noise_type:
                _, stddev = current_noise_type.split('_')
                action_noise = OrnsteinUhlenbeckActionNoise(mu=np.zeros(nb_actions), sigma=float(stddev) * np.ones(nb_actions))
            else:
                raise RuntimeError('unknown noise type "{}"'.format(current_noise_type))

    agent = DDPG(actor, critic, memory, env.observation_space.shape, env.action_space.shape,
        gamma=gamma, tau=tau, normalize_returns=normalize_returns, normalize_observations=normalize_observations,
        batch_size=batch_size, action_noise=action_noise, param_noise=param_noise, critic_l2_reg=critic_l2_reg,
        actor_lr=actor_lr, critic_lr=critic_lr, enable_popart=popart, clip_norm=clip_norm,
        reward_scale=reward_scale, pri_flag=pri_flag)
    logger.info('Using agent with the following configuration:')
    logger.info(str(agent.__dict__.items()))

    eval_episode_rewards_history = deque(maxlen=100)
    episode_rewards_history = deque(maxlen=100)
    sess = U.get_session()
    # Prepare everything.
    agent.initialize(sess)
    sess.graph.finalize()

    agent.reset()
    obs = env.reset()

    if eval_env is not None:
        eval_obs = eval_env.reset()
    nenvs = 1

    
    start_time = time.time()

    t = 0
    episodes = 0 
    episode_steps = []
    episode_actions = []
    episode_qs = []
    
    episode_adaptive_distances = []
    eval_qs = []

    while t <= total_timesteps:
        agent.reset()
        obs = env.reset()
        
        rollout_throughput = []
        rollout_rewards = []
        episode_actor_losses = []
        episode_critic_losses = []

        # reset episode
        episode_reward = 0.
        episode_step = 0

        for _ in range(nb_epoch_cycles * nb_rollout_steps):
            # Predict next action.
            action, q, _, _ = agent.step(obs, apply_noise=True, compute_Q=True)

            # Execute next action.
            new_obs, r, done, info = env.step(action)
            episode_reward += r
            episode_step += 1
            t += 1

            # print(info)

            # Book-keeping.
            rollout_rewards.append(episode_reward)
            episode_actions.append(action)
            episode_qs.append(q)
            rollout_throughput.append(info['throughput'])

            if pri_flag:
                agent.store_transition_pm(obs, action, r, new_obs, done)
            else:
                agent.store_transition(obs, action, r, new_obs, done)

            obs = new_obs

            # Train.
            if t > batch_size:
                for t_train in range(nb_train_steps):
                    # Adapt param noise, if necessary.
                    if not pri_flag and t_train % param_noise_adaption_interval == 0:
                        distance = agent.adapt_param_noise()
                        episode_adaptive_distances.append(distance)

                    cl, al = agent.train()
                    episode_critic_losses.append(cl)
                    episode_actor_losses.append(al)
                    agent.update_target_net()
            
            if done:
                break

        episodes += 1

        # Evaluate.
        if eval_env is not None:
            eval_rewards = []
            eval_obs = eval_env.get_default_obs()
            eval_best_thr = 0
            eval_best_act = None
            eval_best_act_lat = None
            for t_rollout in range(nb_eval_steps):
                eval_action, eval_q, _, _ = agent.step(eval_obs, apply_noise=False, compute_Q=True)
                eval_obs, eval_r, eval_done, eval_info = eval_env.step(eval_action)
                eval_qs.append(eval_q)
                eval
                eval_rewards.append(eval_r)
                if eval_info['throughput'] > eval_best_thr:
                    eval_best_thr = eval_info['throughput']
                    eval_best_act = eval_action
                    eval_best_act_lat = info['latency']

        if MPI is not None:
            mpi_size = MPI.COMM_WORLD.Get_size()
        else:
            mpi_size = 1

        # Log stats.
        # XXX shouldn't call np.mean on variable length lists
        duration = time.time() - start_time
        stats = agent.get_stats()
        print(stats)
        combined_stats = stats.copy()
        combined_stats['rollout/throughput'] = np.mean(rollout_throughput)
        combined_stats['rollout/return'] = np.mean(rollout_rewards)
        combined_stats['rollout/return_std'] = np.std(rollout_rewards)
        combined_stats['train/steps'] = episode_step
        combined_stats['train/actions_mean'] = np.mean(episode_actions, axis=1)
        combined_stats['train/actions_std'] = np.std(episode_actions, axis=1)
        combined_stats['train/Q_mean'] = np.mean(episode_qs)
        combined_stats['train/loss_actor'] = np.mean(episode_actor_losses)
        combined_stats['train/loss_critic'] = np.mean(episode_critic_losses)
        combined_stats['total/duration'] = duration
        combined_stats['total/steps_per_second'] = float(t) / float(duration)
        combined_stats['total/episodes'] = episodes
        
        # Evaluation statistics.
        if eval_env is not None:
            combined_stats['eval/return'] = np.mean(eval_rewards)
            combined_stats['eval/best_throughput'] = eval_best_thr
            combined_stats['eval/best_act_latency'] = eval_best_act_lat
            combined_stats['eval/Q'] = np.mean(eval_qs)
        def as_scalar(x):
            if isinstance(x, np.ndarray):
                assert x.size == 1
                return x[0]
            elif np.isscalar(x):
                return x
            else:
                raise ValueError('expected scalar, got %s' % x)

        combined_stats_sums = np.array([np.array(x).flatten()[0] for x in combined_stats.values()])
        if MPI is not None:
            combined_stats_sums = MPI.COMM_WORLD.allreduce(combined_stats_sums)

        combined_stats = {k : v / mpi_size for (k,v) in zip(combined_stats.keys(), combined_stats_sums)}

        # Total statistics.
        combined_stats['total/steps'] = t

        for key in sorted(combined_stats.keys()):
            logger.record_tabular(key, combined_stats[key])

        if rank == 0:
            logger.dump_tabular()
        logger.info('')
        logdir = logger.get_dir()
        if rank == 0 and logdir:
            if hasattr(env, 'get_state'):
                with open(os.path.join(logdir, 'env_state.pkl'), 'wb') as f:
                    pickle.dump(env.get_state(), f)
            if eval_env and hasattr(eval_env, 'get_state'):
                with open(os.path.join(logdir, 'eval_env_state.pkl'), 'wb') as f:
                    pickle.dump(eval_env.get_state(), f)

    return agent

