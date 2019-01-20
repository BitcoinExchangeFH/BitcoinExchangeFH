# -*- coding: utf-8 -*-

"""Top-level package for Bitcoin exchange feedhandler."""

__author__ = """Gavin Chan"""
__email__ = 'gavincyi@gmail.com'

from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

# flake8: noqa
from .core import Configuration, Runner
