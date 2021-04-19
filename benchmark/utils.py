#!venv/bin/python3

import logging
import sys


def get_logger(name=None):
    if not name:
        name = 'benchmark'
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fhandler = logging.FileHandler('%s.log' % name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(fhandler)
    return logger
