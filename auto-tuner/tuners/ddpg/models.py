import tensorflow as tf
import numpy as np
from tuners.common.models import get_network_builder
from tuners.a2c.utils import conv, fc, conv_to_fc, batch_to_seq, seq_to_batch


class Model(object):
    def __init__(self, name, network='mlp', **network_kwargs):
        self.name = name
        self.network_builder = get_network_builder(network)(**network_kwargs)

    @property
    def vars(self):
        return tf.get_collection(tf.GraphKeys.GLOBAL_VARIABLES, scope=self.name)

    @property
    def trainable_vars(self):
        return tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES, scope=self.name)

    @property
    def perturbable_vars(self):
        return [var for var in self.trainable_vars if 'LayerNorm' not in var.name]


class Actor(Model):
    def __init__(self, nb_actions, name='actor', network='mlp', **network_kwargs):
        super().__init__(name=name, network=network, **network_kwargs)
        self.nb_actions = nb_actions

    def __call__(self, obs, reuse=False):
        with tf.variable_scope(self.name, reuse=tf.AUTO_REUSE):
            x = tf.layers.flatten(obs)
            x = fc(x, 'mlp_fc{}'.format(0), nh=128, init_scale=np.sqrt(2))
            x = tf.nn.relu(x)
            # x = tf.layers.batch_normalization(x)

            x = fc(x, 'mlp_fc{}'.format(1), nh=128, init_scale=np.sqrt(2))
            x = tf.nn.relu(x)
            # x = tf.layers.dropout(x, rate=0.3)

            x = tf.layers.dense(x,
                                units=self.nb_actions,
                                activation=tf.nn.sigmoid,
                                kernel_initializer=tf.random_uniform_initializer(minval=-0.1, maxval=0.1),
                                name="a_output")
        return x


class Critic(Model):
    def __init__(self, nb_actions, nb_obs, l2_reg, name='critic', network='mlp', **network_kwargs):
        super().__init__(name=name, network=network, **network_kwargs)
        self.layer_norm = True
        self.nb_actions = nb_actions
        self.nb_obs = nb_obs
        self.l2_reg = l2_reg

    def __call__(self, obs, action, reuse=False):
        with tf.variable_scope(self.name, reuse=tf.AUTO_REUSE):
            hs = tf.layers.flatten(obs)
            hs = fc(hs, 'mlp_fc{}'.format(0), nh=64, init_scale=np.sqrt(2))
            # hs = tf.nn.relu(hs)

            ha = tf.layers.flatten(action)
            ha = fc(ha, 'mlp_fc{}'.format(1), nh=64, init_scale=np.sqrt(2))
            # ha = tf.nn.relu(ha)

            h = tf.concat([hs, ha], 1, name="h_concat")
            h = fc(h, 'mlp_fc{}'.format(2), nh=128, init_scale=np.sqrt(2))
            h = tf.nn.relu(h)
            # h = tf.layers.batch_normalization(h)

            q_output = tf.layers.dense(h,
                                       units=1,
                                       activation=None,
                                       kernel_initializer=tf.random_uniform_initializer(minval=-0.1, maxval=0.1),
                                       kernel_regularizer=tf.contrib.layers.l2_regularizer(self.l2_reg),
                                       name="output")
        return q_output

    @property
    def output_vars(self):
        output_vars = [var for var in self.trainable_vars if 'output' in var.name]
        return output_vars
