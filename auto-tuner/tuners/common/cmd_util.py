"""
Helpers for scripts like run_atari.py.
"""

try:
    from mpi4py import MPI
except ImportError:
    MPI = None


def arg_parser():
    """
    Create an empty argparse.ArgumentParser.
    """
    import argparse
    return argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)


def atari_arg_parser():
    """
    Create an argparse.ArgumentParser for run_atari.py.
    """
    print('Obsolete - use common_arg_parser instead')
    return common_arg_parser()


def mujoco_arg_parser():
    print('Obsolete - use common_arg_parser instead')
    return common_arg_parser()


def common_arg_parser():
    """
    Create an argparse.ArgumentParser for run_mujoco.py.
    """
    parser = arg_parser()
    parser.add_argument('--env', help='environment ID', type=str, default='dms')
    parser.add_argument('--env_type', help='type of environment, used when the environment type cannot be '
                                           'automatically determined', type=str, default='dms')
    parser.add_argument('--env_model', help='dms environment prediction model path', type=str)
    parser.add_argument('--meta_info', help='meta information path of DMS knobs, states, and rewards', type=str)
    parser.add_argument('--seed', help='RNG seed', type=int, default=1000)
    parser.add_argument('--alg', help='Algorithm', type=str, default='ddpg')
    parser.add_argument('--episodes', type=float, default=1000)
    parser.add_argument('--epoch_steps', type=float, default=100)
    parser.add_argument('--network', help='network type (mlp, cnn, lstm, cnn_lstm, conv_only)', default='mlp')
    parser.add_argument('--reward_scale', help='Reward scale factor. Default: 1.0', default=1.0, type=float)
    parser.add_argument('--save_path', help='Path to save trained model to', default='./model', type=str)
    parser.add_argument('--lat_bound', default=1.0, type=float, help='latency bound')
    return parser


def parse_unknown_args(args):
    """
    Parse arguments not consumed by arg parser into a dictionary
    """
    retval = {}
    preceded_by_key = False
    for arg in args:
        if arg.startswith('--'):
            if '=' in arg:
                key = arg.split('=')[0][2:]
                value = arg.split('=')[1]
                retval[key] = value
            else:
                key = arg[2:]
                preceded_by_key = True
        elif preceded_by_key:
            retval[key] = arg
            preceded_by_key = False

    return retval
